import mongoose from "mongoose";

import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { sendSms } from "../services/sms/termiiClient.js";
import { SmsTemplateModel } from "../models/SmsTemplate.js";
import { CommunicationLogModel } from "../models/CommunicationLog.js";

import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { ProfileModel } from "../models/Profile.js";
import { ContributionModel } from "../models/Contribution.js";
import { getContributionTypeMatch } from "../utils/contributionPolicy.js";

function clamp(n, min, max) {
  return Math.min(max, Math.max(min, n));
}

function splitCsv(value) {
  if (!value) return [];
  return String(value)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function uniqueStrings(values) {
  return [...new Set(values.map((v) => String(v).trim()).filter(Boolean))];
}

function startOfUtcDay(d) {
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), 0, 0, 0, 0));
}

function startOfUtcMonth(d) {
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1, 0, 0, 0, 0));
}

function dueDateUtc(year, month1to12, day = 25) {
  return new Date(Date.UTC(year, month1to12 - 1, day, 23, 59, 59, 999));
}

async function getManageableGroupIds(req) {
  if (!req.user) throw new AppError("Not authenticated", 401);
  if (!req.user.profileId) throw new AppError("User profile not found", 400);

  if (req.user.role === "admin") return null;

  if (req.user.role !== "groupCoordinator") {
    throw new AppError("Insufficient permissions", 403);
  }

  const coordinatorMemberships = await GroupMembershipModel.find(
    { userId: req.user.profileId, role: "coordinator", status: "active" },
    { groupId: 1 },
  ).lean();

  return coordinatorMemberships.map((m) => String(m.groupId));
}

export const listAdminSmsTemplates = catchAsync(async (req, res) => {
  const templates = await SmsTemplateModel.find({ isActive: true }).sort({ name: 1 }).lean();
  return sendSuccess(res, { statusCode: 200, results: templates.length, data: { templates } });
});

export const getAdminSmsStats = catchAsync(async (req, res) => {
  const now = new Date();
  const todayStart = startOfUtcDay(now);
  const monthStart = startOfUtcMonth(now);

  const [todayAgg, monthAgg] = await Promise.all([
    CommunicationLogModel.aggregate([
      { $match: { createdAt: { $gte: todayStart }, "channels.sms.requested": true } },
      {
        $group: {
          _id: null,
          attempted: { $sum: "$channels.sms.attempted" },
          sent: { $sum: "$channels.sms.sent" },
          failed: { $sum: "$channels.sms.failed" },
        },
      },
    ]),
    CommunicationLogModel.aggregate([
      { $match: { createdAt: { $gte: monthStart }, "channels.sms.requested": true } },
      {
        $group: {
          _id: null,
          attempted: { $sum: "$channels.sms.attempted" },
          sent: { $sum: "$channels.sms.sent" },
          failed: { $sum: "$channels.sms.failed" },
        },
      },
    ]),
  ]);

  const today = todayAgg?.[0] ?? { attempted: 0, sent: 0, failed: 0 };
  const month = monthAgg?.[0] ?? { attempted: 0, sent: 0, failed: 0 };
  const deliveryRate = month.attempted > 0 ? (month.sent / month.attempted) * 100 : 0;

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      today: { attempted: Number(today.attempted || 0), sent: Number(today.sent || 0), failed: Number(today.failed || 0) },
      month: { attempted: Number(month.attempted || 0), sent: Number(month.sent || 0), failed: Number(month.failed || 0) },
      deliveryRatePct: clamp(deliveryRate, 0, 100),
    },
  });
});

async function getRecipientPhonesForTarget(req, { target, groupNumbers, month, year }) {
  const manageableGroupIds = await getManageableGroupIds(req);

  if (target === "coordinators") {
    const filter = {};
    if (manageableGroupIds) filter._id = { $in: manageableGroupIds };
    if (Array.isArray(groupNumbers) && groupNumbers.length > 0) {
      filter.groupNumber = { $in: groupNumbers.map((n) => Number(n)).filter((n) => Number.isFinite(n)) };
    }

    const groups = await GroupModel.find(filter, { coordinatorPhone: 1 }).lean();
    return uniqueStrings(groups.flatMap((g) => splitCsv(g.coordinatorPhone)));
  }

  // For member-based targets we build a scoped membership set first.
  const membershipFilter = { status: "active" };
  if (manageableGroupIds) {
    const objectIds = manageableGroupIds
      .filter((id) => mongoose.Types.ObjectId.isValid(id))
      .map((id) => new mongoose.Types.ObjectId(id));
    membershipFilter.groupId = { $in: objectIds };
  }

  if (target === "selected") {
    const nums = (Array.isArray(groupNumbers) ? groupNumbers : [])
      .map((n) => Number(n))
      .filter((n) => Number.isFinite(n));
    if (nums.length === 0) return [];

    const groups = await GroupModel.find(
      { ...(manageableGroupIds ? { _id: { $in: manageableGroupIds } } : {}), groupNumber: { $in: nums } },
      { _id: 1 },
    ).lean();
    const ids = groups.map((g) => g._id);
    membershipFilter.groupId = { $in: ids };
  }

  const memberships = await GroupMembershipModel.find(membershipFilter, { userId: 1, groupId: 1, joinedAt: 1 }).lean();
  const userIds = uniqueStrings(memberships.map((m) => String(m.userId)));

  if (target === "all") {
    const profiles = await ProfileModel.find({ _id: { $in: userIds }, phone: { $ne: null } }, { phone: 1 }).lean();
    return uniqueStrings(profiles.flatMap((p) => splitCsv(p.phone)));
  }

  if (target === "defaulters") {
    const now = new Date();
    const y = Number.isFinite(year) ? Number(year) : now.getUTCFullYear();
    const m = Number.isFinite(month) ? Number(month) : now.getUTCMonth() + 1;

    const groupIds = uniqueStrings(memberships.map((mm) => String(mm.groupId)));
    const groupObjectIds = groupIds
      .filter((id) => mongoose.Types.ObjectId.isValid(id))
      .map((id) => new mongoose.Types.ObjectId(id));

    const [groups, profiles, contribDocs] = await Promise.all([
      GroupModel.find({ _id: { $in: groupObjectIds } }, { monthlyContribution: 1 }).lean(),
      ProfileModel.find({ _id: { $in: userIds }, phone: { $ne: null } }, { phone: 1 }).lean(),
      ContributionModel.find(
        {
          groupId: { $in: groupObjectIds },
          userId: { $in: userIds },
          year: y,
          month: m,
          contributionType: { $in: getContributionTypeMatch("revolving") || ["revolving"] },
        },
        { userId: 1, groupId: 1, status: 1, amount: 1, year: 1, month: 1 },
      ).lean(),
    ]);

    const groupById = new Map(groups.map((g) => [String(g._id), g]));
    const profileById = new Map(profiles.map((p) => [String(p._id), p]));

    const paidByKey = new Map();
    for (const c of contribDocs) {
      const k = `${String(c.userId)}|${String(c.groupId)}|${Number(c.year)}|${Number(c.month)}`;
      const isPaid = ["verified", "completed"].includes(String(c.status));
      if (!isPaid) continue;
      paidByKey.set(k, Number(paidByKey.get(k) || 0) + Number(c.amount || 0));
    }

    const due = dueDateUtc(y, m, 25);
    const isDefaulted = (userId, groupId) => {
      const expected = Number(groupById.get(String(groupId))?.monthlyContribution || 0);
      const key = `${String(userId)}|${String(groupId)}|${y}|${m}`;
      const paid = Number(paidByKey.get(key) || 0);
      if (paid > 0) return false;
      return new Date().getTime() > due.getTime() && expected > 0;
    };

    const phones = [];
    for (const mm of memberships) {
      const uid = String(mm.userId);
      const gid = String(mm.groupId);
      if (!isDefaulted(uid, gid)) continue;
      const p = profileById.get(uid);
      if (!p?.phone) continue;
      phones.push(...splitCsv(p.phone));
    }
    return uniqueStrings(phones);
  }

  return [];
}

export const sendAdminBulkSms = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  if (!process.env.TERMII_API_KEY || !process.env.TERMII_SENDER_ID) {
    return next(new AppError("SMS is not configured on the server", 400));
  }

  const message = typeof req.body?.message === "string" ? req.body.message : "";
  if (!message.trim()) return next(new AppError("Message is required", 400));

  const target = String(req.body?.target ?? "coordinators").trim().toLowerCase();
  const groupNumbers = Array.isArray(req.body?.groupNumbers) ? req.body.groupNumbers : [];
  const year = req.body?.year ? Number(req.body.year) : undefined;
  const month = req.body?.month ? Number(req.body.month) : undefined;

  const phones = await getRecipientPhonesForTarget(req, { target, groupNumbers, year, month });
  if (phones.length === 0) return next(new AppError("No SMS recipients found for the selected target", 404));

  const settles = await Promise.allSettled(phones.map((to) => sendSms({ to, message })));

  const result = {
    target,
    channels: {
      sms: { requested: true, attempted: phones.length, sent: 0, failed: 0, skipped: 0 },
    },
    failures: [],
  };

  for (let i = 0; i < settles.length; i++) {
    const s = settles[i];
    if (s.status === "fulfilled") {
      result.channels.sms.sent += 1;
    } else {
      result.channels.sms.failed += 1;
      result.failures.push({
        channel: "sms",
        to: phones[i],
        error: s.reason ? String(s.reason?.message ?? s.reason) : "Unknown error",
      });
    }
  }

  CommunicationLogModel.create({
    createdBy: req.user.profileId,
    creatorRole: req.user.role,
    kind: "sms",
    target,
    groupNumbers: (Array.isArray(groupNumbers) ? groupNumbers : []).map((n) => Number(n)).filter((n) => Number.isFinite(n)),
    title: "Bulk SMS",
    message,
    channels: {
      email: { requested: false, attempted: 0, sent: 0, failed: 0, skipped: 0 },
      sms: result.channels.sms,
      notification: { requested: false, attempted: 0, sent: 0, failed: 0, skipped: 0 },
    },
    failures: result.failures,
  }).catch(() => {});

  return sendSuccess(res, { statusCode: 201, message: "SMS dispatched", data: { dispatch: result } });
});
