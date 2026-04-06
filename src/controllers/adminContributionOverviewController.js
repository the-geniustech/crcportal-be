import mongoose from "mongoose";

import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { ContributionModel } from "../models/Contribution.js";
import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import {
  ContributionTypeCanonical,
  getContributionTypeMatch,
  normalizeContributionType,
} from "../utils/contributionPolicy.js";

function clamp(n, min, max) {
  return Math.min(max, Math.max(min, n));
}

function parseYear(req) {
  const now = new Date();
  const year = Number(req.query?.year ?? now.getUTCFullYear());
  if (!Number.isFinite(year) || year < 2000 || year > 2100) {
    return { error: "Invalid year" };
  }
  return { year };
}

function parseContributionType(req) {
  const raw = String(req.query?.contributionType ?? "revolving").trim();
  const canonical = normalizeContributionType(raw);
  if (!canonical) {
    return { error: "Invalid contributionType" };
  }
  return { type: canonical };
}

function parseMonth(req) {
  if (typeof req.query?.month === "undefined") return { month: null };
  const month = Number(req.query?.month);
  if (!Number.isFinite(month) || month < 1 || month > 12) {
    return { error: "Invalid month" };
  }
  return { month };
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

export const getAdminContributionTracking = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const yearParsed = parseYear(req);
  if (yearParsed.error) return next(new AppError(yearParsed.error, 400));
  const typeParsed = parseContributionType(req);
  if (typeParsed.error) return next(new AppError(typeParsed.error, 400));
  const monthParsed = parseMonth(req);
  if (monthParsed.error) return next(new AppError(monthParsed.error, 400));

  const { year } = yearParsed;
  const { type: contributionType } = typeParsed;
  const { month } = monthParsed;
  const typeMatch = getContributionTypeMatch(contributionType) || [contributionType];

  const manageableGroupIds = await getManageableGroupIds(req);
  const filter = {};
  if (manageableGroupIds) filter._id = { $in: manageableGroupIds };

  const groups = await GroupModel.find(filter).sort({ groupNumber: 1 }).lean();
  if (groups.length === 0) {
    return sendSuccess(res, {
      statusCode: 200,
      results: 0,
      data: { year, month, contributionType, groups: [] },
    });
  }

  const groupIds = groups.map((g) => String(g._id));
  const groupObjectIds = groupIds
    .filter((id) => mongoose.Types.ObjectId.isValid(id))
    .map((id) => new mongoose.Types.ObjectId(id));

  const match = {
    groupId: { $in: groupObjectIds },
    year,
    contributionType: { $in: typeMatch },
  };
  if (month) {
    match.month = { $lte: month };
  }

  const [activeCounts, contribAgg] = await Promise.all([
    GroupMembershipModel.aggregate([
      { $match: { groupId: { $in: groupObjectIds }, status: "active" } },
      { $group: { _id: "$groupId", count: { $sum: 1 } } },
    ]),
    ContributionModel.aggregate([
      { $match: match },
      {
        $group: {
          _id: { groupId: "$groupId", month: "$month" },
          paidAmount: {
            $sum: {
              $cond: [{ $in: ["$status", ["verified", "completed"]] }, "$amount", 0],
            },
          },
          hasVerified: {
            $max: { $cond: [{ $eq: ["$status", "verified"] }, 1, 0] },
          },
        },
      },
    ]),
  ]);

  const activeByGroupId = new Map(activeCounts.map((r) => [String(r._id), Number(r.count || 0)]));
  const monthlyByKey = new Map(
    contribAgg.map((r) => [
      `${String(r._id.groupId)}|${Number(r._id.month)}`,
      {
        paidAmount: Number(r.paidAmount || 0),
        hasVerified: Boolean(r.hasVerified),
      },
    ]),
  );

  const monthCount = month ? Math.max(1, Number(month)) : 12;
  const months = Array.from({ length: monthCount }, (_, idx) => idx + 1);

  const rows = groups.map((g) => {
    const gid = String(g._id);
    const activeMembers = activeByGroupId.get(gid) ?? Number(g.memberCount || 0);
    const expectedPerMonth = Number(g.monthlyContribution || 0) * Math.max(0, activeMembers);
    let totalPaid = 0;

    const monthStatuses = months.map((m) => {
      const key = `${gid}|${m}`;
      const entry = monthlyByKey.get(key);
      const paidAmount = entry?.paidAmount ?? 0;
      const hasVerified = entry?.hasVerified ?? false;
      totalPaid += paidAmount;

      let status = "pending";
      if (paidAmount >= expectedPerMonth && expectedPerMonth > 0) {
        status = hasVerified ? "verified" : "completed";
      } else if (paidAmount > 0) {
        status = "completed";
      }

      return {
        month: m,
        status,
        expectedAmount: expectedPerMonth,
        paidAmount,
        hasVerified,
      };
    });

    return {
      groupId: gid,
      groupNumber: g.groupNumber,
      groupName: g.groupName,
      isSpecial: Boolean(g.isSpecial),
      monthlyContribution: Number(g.monthlyContribution || 0),
      activeMembers,
      totalPaid,
      months: monthStatuses,
    };
  });

  return sendSuccess(res, {
    statusCode: 200,
    results: rows.length,
    data: { year, month, contributionType, groups: rows },
  });
});

export const getAdminSpecialContributionSummary = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const yearParsed = parseYear(req);
  if (yearParsed.error) return next(new AppError(yearParsed.error, 400));
  const { year } = yearParsed;

  const manageableGroupIds = await getManageableGroupIds(req);
  const groupScopeFilter = manageableGroupIds ? { _id: { $in: manageableGroupIds } } : {};

  const specialGroup =
    (await GroupModel.findOne({ ...groupScopeFilter, groupNumber: 0 }).lean()) ||
    (await GroupModel.findOne({ ...groupScopeFilter, isSpecial: true }).lean());

  const specialTypes = ["special", "endwell", "festive"];
  const specialTypeMatches = specialTypes.flatMap(
    (t) => getContributionTypeMatch(t) || [t],
  );

  const match = {
    year,
    contributionType: { $in: specialTypeMatches },
  };

  if (specialGroup) {
    match.groupId = specialGroup._id;
  } else if (manageableGroupIds) {
    const groupObjectIds = manageableGroupIds
      .filter((id) => mongoose.Types.ObjectId.isValid(id))
      .map((id) => new mongoose.Types.ObjectId(id));
    match.groupId = { $in: groupObjectIds };
  }

  const rows = await ContributionModel.aggregate([
    { $match: match },
    {
      $group: {
        _id: "$contributionType",
        paidAmount: {
          $sum: {
            $cond: [{ $in: ["$status", ["verified", "completed"]] }, "$amount", 0],
          },
        },
        contributors: { $addToSet: "$userId" },
      },
    },
  ]);

  const totalsByType = new Map();
  const contributorsByType = new Map();

  for (const row of rows) {
    const canonical = normalizeContributionType(row._id);
    if (!canonical) continue;
    const total = Number(row.paidAmount || 0);
    totalsByType.set(canonical, (totalsByType.get(canonical) || 0) + total);
    const set = contributorsByType.get(canonical) || new Set();
    if (Array.isArray(row.contributors)) {
      row.contributors.forEach((id) => set.add(String(id)));
    }
    contributorsByType.set(canonical, set);
  }

  const summary = specialTypes.map((type) => ({
    type,
    totalCollected: totalsByType.get(type) ?? 0,
    contributors: contributorsByType.get(type)?.size ?? 0,
  }));

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      year,
      group: specialGroup
        ? { id: String(specialGroup._id), groupNumber: specialGroup.groupNumber, groupName: specialGroup.groupName }
        : null,
      summary,
    },
  });
});
