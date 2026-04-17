import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import {
  hasNonZeroGroupMembership,
  isGeneralGroup,
} from "../utils/groupMembershipPolicy.js";

import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { ContributionModel } from "../models/Contribution.js";
import { ProfileModel } from "../models/Profile.js";
import { NotificationModel } from "../models/Notification.js";
import { RecurringPaymentModel } from "../models/RecurringPayment.js";
import { TransactionModel } from "../models/Transaction.js";
import mongoose from "mongoose";
import { assignGroupMemberSerial } from "../utils/groupMemberSerial.js";
import { hasUserRole } from "../utils/roles.js";
import {
  ContributionTypeCanonical,
  calculateContributionInterestForType,
  calculateContributionUnits,
  getContributionTypeMatch,
  isContributionAmountValid,
  normalizeContributionType,
  resolveExpectedContributionAmount,
} from "../utils/contributionPolicy.js";
import { sendEmail } from "../services/mail/resendClient.js";
import { sendSms } from "../services/sms/termiiClient.js";
import { emitToUser } from "../socket.js";

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

function pick(obj, allowedKeys) {
  const out = {};
  for (const key of allowedKeys) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) out[key] = obj[key];
  }
  return out;
}

function parseMonthYear(req) {
  const now = new Date();
  const year = Number(req.query?.year ?? now.getUTCFullYear());
  const month = Number(req.query?.month ?? now.getUTCMonth() + 1);

  if (!Number.isFinite(year) || year < 2000 || year > 2100) {
    return { error: "Invalid year" };
  }
  if (!Number.isFinite(month) || month < 1 || month > 12) {
    return { error: "Invalid month" };
  }

  return { year, month };
}

function parseMonthYearPayload(req) {
  const now = new Date();
  const year = Number(req.body?.year ?? now.getUTCFullYear());
  const month = Number(req.body?.month ?? now.getUTCMonth() + 1);

  if (!Number.isFinite(year) || year < 2000 || year > 2100) {
    return { error: "Invalid year" };
  }
  if (!Number.isFinite(month) || month < 1 || month > 12) {
    return { error: "Invalid month" };
  }

  return { year, month };
}

function dueDateUtc(year, month1to12, day = 4) {
  const nextMonthIndex = month1to12;
  return new Date(Date.UTC(year, nextMonthIndex, day, 23, 59, 59, 999));
}

function generateReference(prefix = "CRC") {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 11).toUpperCase()}`;
}

function addFrequency(date, frequency) {
  const d = new Date(date);
  if (frequency === "weekly") d.setDate(d.getDate() + 7);
  else if (frequency === "bi-weekly") d.setDate(d.getDate() + 14);
  else d.setMonth(d.getMonth() + 1);
  return d;
}

function parseDate(value, fallback = null) {
  if (!value) return fallback;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return fallback;
  return date;
}

function applyContributionMetrics(contribution, amount) {
  if (!contribution) return;
  const safeAmount = Number(amount ?? contribution.amount ?? 0);
  contribution.units = calculateContributionUnits(safeAmount);
  contribution.interestAmount = calculateContributionInterestForType(
    contribution.contributionType,
    safeAmount,
  );
}

async function updateRecurringPaymentStats({
  userId,
  paymentType,
  groupId,
  loanId,
  amount,
  count = 1,
  paidAt,
} = {}) {
  if (!userId || !paymentType) return;

  const query = { userId, paymentType, isActive: true };
  if (paymentType === "group_contribution") {
    if (!groupId) return;
    query.groupId = groupId;
  }
  if (paymentType === "loan_repayment") {
    if (!loanId) return;
    query.loanId = loanId;
  }

  const schedules = await RecurringPaymentModel.find(query).sort({
    nextPaymentDate: 1,
    createdAt: 1,
  });
  if (schedules.length === 0) return;

  let target = schedules[0];
  if (Number.isFinite(amount) && count === 1) {
    const match = schedules.find(
      (schedule) =>
        Math.round(Number(schedule.amount ?? 0) * 100) ===
        Math.round(Number(amount ?? 0) * 100),
    );
    if (match) target = match;
  }

  const paidAtDate = parseDate(paidAt, new Date());
  target.totalPaymentsMade = Number(target.totalPaymentsMade ?? 0) + count;
  target.totalAmountPaid =
    Number(target.totalAmountPaid ?? 0) + Number(amount ?? 0);
  target.lastPaymentDate = paidAtDate;
  target.lastPaymentStatus = "success";

  const baseDate =
    parseDate(target.nextPaymentDate, null) ||
    parseDate(target.startDate, null) ||
    paidAtDate;
  let nextDate = baseDate || paidAtDate;
  for (let i = 0; i < count; i += 1) {
    nextDate = addFrequency(nextDate, target.frequency);
  }
  while (nextDate <= paidAtDate) {
    nextDate = addFrequency(nextDate, target.frequency);
  }
  target.nextPaymentDate = nextDate;
  await target.save();
}

function endOfMonthUtc(year, month1to12) {
  return new Date(Date.UTC(year, month1to12, 0, 23, 59, 59, 999));
}

function formatMonthLabel(year, month) {
  return new Date(Date.UTC(year, month - 1, 1)).toLocaleDateString("en-US", {
    month: "long",
    year: "numeric",
  });
}

function buildContributionReminderMessage({ groupName, label, amount }) {
  const safeGroup = groupName || "your group";
  const safeLabel = label || "this month";
  const amountText = Number(amount || 0)
    ? ` Amount: NGN${Number(amount || 0).toLocaleString()}.`
    : "";
  return `Your contribution for ${safeGroup} (${safeLabel}) is due.${amountText} Payment window: 27th-4th.`;
}

const ManualContributionPaymentMethods = new Set([
  "bank_transfer",
  "cash",
  "card",
  "pos",
  "mobile_money",
  "cheque",
  "other",
]);

export const listAdminGroups = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const manageableGroupIds = await getManageableGroupIds(req);

  const filter = {};
  if (manageableGroupIds) {
    filter._id = { $in: manageableGroupIds };
  }

  if (typeof req.query?.status === "string" && req.query.status.trim()) {
    filter.status = String(req.query.status).trim();
  }

  const category =
    typeof req.query?.category === "string"
      ? String(req.query.category).trim()
      : "";
  if (category && category !== "All Categories") {
    filter.category = category;
  }

  const location =
    typeof req.query?.location === "string"
      ? String(req.query.location).trim()
      : "";
  if (location && location !== "All Locations") {
    filter.location = location;
  }

  const search =
    typeof req.query?.search === "string" ? req.query.search.trim() : "";
  if (search) {
    filter.$or = [
      { groupName: { $regex: search, $options: "i" } },
      { description: { $regex: search, $options: "i" } },
      { location: { $regex: search, $options: "i" } },
      { coordinatorName: { $regex: search, $options: "i" } },
    ];
  }

  const page = Math.max(1, parseInt(String(req.query?.page ?? "1"), 10) || 1);
  const limit = Math.min(
    200,
    Math.max(1, parseInt(String(req.query?.limit ?? "100"), 10) || 100),
  );
  const skip = (page - 1) * limit;

  const sortKey =
    typeof req.query?.sort === "string" ? req.query.sort.trim() : "";
  let sort = { groupNumber: 1 };
  if (sortKey === "newest") {
    sort = { createdAt: -1 };
  } else if (sortKey === "savings") {
    sort = { totalSavings: -1, groupNumber: 1 };
  } else if (sortKey === "contribution") {
    sort = { monthlyContribution: 1, groupNumber: 1 };
  } else if (sortKey === "popular") {
    sort = { memberCount: -1, groupNumber: 1 };
  }

  const [groups, total] = await Promise.all([
    GroupModel.find(filter).sort(sort).skip(skip).limit(limit).lean(),
    GroupModel.countDocuments(filter),
  ]);

  const includeMetrics =
    String(req.query?.includeMetrics ?? "true").toLowerCase() !== "false";
  const monthYear = parseMonthYear(req);
  if (includeMetrics && monthYear.error)
    return next(new AppError(monthYear.error, 400));

  if (!includeMetrics) {
    return sendSuccess(res, {
      statusCode: 200,
      results: groups.length,
      total,
      page,
      limit,
      data: { groups },
    });
  }

  const { year, month } = monthYear;
  const groupIds = groups.map((g) => String(g._id));
  const groupObjectIds = groupIds
    .filter((id) => mongoose.Types.ObjectId.isValid(id))
    .map((id) => new mongoose.Types.ObjectId(id));

  const [activeCounts, paidSums] = await Promise.all([
    GroupMembershipModel.aggregate([
      { $match: { groupId: { $in: groupObjectIds }, status: "active" } },
      { $group: { _id: "$groupId", count: { $sum: 1 } } },
    ]),
    ContributionModel.aggregate([
      {
        $match: {
          groupId: { $in: groupObjectIds },
          year,
          month,
          contributionType: {
            $in: getContributionTypeMatch("revolving") || ["revolving"],
          },
        },
      },
      {
        $group: {
          _id: "$groupId",
          paidAmount: {
            $sum: {
              $cond: [
                { $in: ["$status", ["verified", "completed"]] },
                "$amount",
                0,
              ],
            },
          },
        },
      },
    ]),
  ]);

  const activeByGroupId = new Map(
    activeCounts.map((r) => [String(r._id), Number(r.count || 0)]),
  );
  const paidByGroupId = new Map(
    paidSums.map((r) => [String(r._id), Number(r.paidAmount || 0)]),
  );

  const enriched = groups.map((g) => {
    const gid = String(g._id);
    const activeMembers =
      activeByGroupId.get(gid) ?? Number(g.memberCount || 0);
    const expected =
      Number(g.monthlyContribution || 0) *
      Math.max(0, Number(activeMembers || 0));
    const paid = paidByGroupId.get(gid) ?? 0;
    const rate = expected > 0 ? (paid / expected) * 100 : 0;

    return {
      ...g,
      activeMemberCount: activeMembers,
      contributionPeriod: { year, month },
      expectedContributions: expected,
      collectedContributions: paid,
      collectionRate: clamp(rate, 0, 100),
    };
  });

  const allGroups = await GroupModel.find(filter, {
    _id: 1,
    coordinatorId: 1,
    memberCount: 1,
  }).lean();
  const allGroupObjectIds = allGroups
    .map((g) => String(g._id))
    .filter((id) => mongoose.Types.ObjectId.isValid(id))
    .map((id) => new mongoose.Types.ObjectId(id));

  const totalMembers = allGroups.reduce(
    (sum, g) => sum + Number(g.memberCount || 0),
    0,
  );

  const [withCoordinators, totalCollectedAgg, categories, locations] =
    await Promise.all([
      GroupModel.countDocuments({ ...filter, coordinatorId: { $ne: null } }),
      ContributionModel.aggregate([
        {
          $match: {
            groupId: { $in: allGroupObjectIds },
            year,
            month,
            contributionType: {
              $in: getContributionTypeMatch("revolving") || ["revolving"],
            },
            status: { $in: ["verified", "completed"] },
          },
        },
        { $group: { _id: null, total: { $sum: "$amount" } } },
      ]),
      GroupModel.distinct("category", filter),
      GroupModel.distinct("location", filter),
    ]);

  const totalCollected = Number(totalCollectedAgg?.[0]?.total ?? 0);

  const ytdSums = await ContributionModel.aggregate([
    {
      $match: {
        groupId: { $in: allGroupObjectIds },
        year,
        month: { $gte: 1, $lte: month },
      },
    },
    {
      $group: {
        _id: "$contributionType",
        paidAmount: {
          $sum: {
            $cond: [
              { $in: ["$status", ["verified", "completed"]] },
              "$amount",
              0,
            ],
          },
        },
      },
    },
  ]);

  const contributionTypeTotalsYtd = ContributionTypeCanonical.reduce(
    (acc, key) => {
      acc[key] = 0;
      return acc;
    },
    {},
  );

  for (const row of ytdSums) {
    const canonical = normalizeContributionType(row._id);
    if (
      !canonical ||
      !Object.prototype.hasOwnProperty.call(
        contributionTypeTotalsYtd,
        canonical,
      )
    ) {
      continue;
    }
    contributionTypeTotalsYtd[canonical] += Number(row.paidAmount || 0);
  }

  return sendSuccess(res, {
    statusCode: 200,
    results: enriched.length,
    total,
    page,
    limit,
    data: {
      groups: enriched,
      summary: {
        totalGroups: total,
        totalMembers,
        withCoordinators,
        contributionPeriod: { year, month },
        totalCollected,
        categories: categories.filter(Boolean),
        locations: locations.filter(Boolean),
        contributionTypeTotalsYtd,
      },
    },
  });
});

async function getManageableGroupIds(req) {
  if (!req.user) throw new AppError("Not authenticated", 401);
  if (!req.user.profileId) throw new AppError("User profile not found", 400);

  if (hasUserRole(req.user, "admin")) return null;

  if (!hasUserRole(req.user, "groupCoordinator")) {
    throw new AppError("Insufficient permissions", 403);
  }

  const coordinatorMemberships = await GroupMembershipModel.find(
    { userId: req.user.profileId, role: "coordinator", status: "active" },
    { groupId: 1 },
  ).lean();

  return coordinatorMemberships.map((m) => String(m.groupId));
}

export const listMemberApprovals = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const status = String(req.query?.status ?? "pending")
    .trim()
    .toLowerCase();
  if (!["pending", "active", "rejected"].includes(status)) {
    return next(new AppError("Invalid status filter", 400));
  }

  const groupIdParam = req.query?.groupId ? String(req.query.groupId) : null;
  const manageableGroupIds = await getManageableGroupIds(req);

  const groupScope =
    manageableGroupIds === null ? null : new Set(manageableGroupIds);

  if (groupIdParam && groupScope && !groupScope.has(groupIdParam)) {
    return next(new AppError("You cannot manage this group", 403));
  }

  const filter = { status };
  if (groupIdParam) {
    filter.groupId = groupIdParam;
  } else if (groupScope) {
    filter.groupId = { $in: manageableGroupIds };
  }

  const page = Math.max(1, parseInt(String(req.query?.page ?? "1"), 10) || 1);
  const limit = Math.min(
    200,
    Math.max(1, parseInt(String(req.query?.limit ?? "50"), 10) || 50),
  );
  const skip = (page - 1) * limit;

  const memberships = await GroupMembershipModel.find(filter)
    .sort({ createdAt: -1 })
    .skip(skip)
    .limit(limit)
    .populate("userId")
    .populate("groupId");

  const applicants = memberships.map((m) => {
    const profile = m.userId && typeof m.userId === "object" ? m.userId : null;
    const group = m.groupId && typeof m.groupId === "object" ? m.groupId : null;

    return {
      id: String(m._id),
      membershipId: String(m._id),
      profileId: profile?._id ? String(profile._id) : null,
      groupId: group?._id ? String(group._id) : String(m.groupId),
      name: profile?.fullName ?? "Member",
      email: profile?.email ?? "",
      phone: profile?.phone ?? "",
      groupName: group?.groupName ?? "Group",
      applicationDate: (m.requestedAt || m.createdAt || new Date())
        .toISOString()
        .slice(0, 10),
      status:
        m.status === "active"
          ? "approved"
          : m.status === "rejected"
            ? "rejected"
            : "pending",
      notes: m.reviewNotes ?? null,
    };
  });

  return sendSuccess(res, {
    statusCode: 200,
    results: applicants.length,
    page,
    limit,
    data: { applicants },
  });
});

export const approveMemberApplication = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const { membershipId } = req.params;
  const notes = req.body?.notes ? String(req.body.notes).trim() : null;

  const membership = await GroupMembershipModel.findById(membershipId);
  if (!membership) return next(new AppError("Membership not found", 404));

  const manageableGroupIds = await getManageableGroupIds(req);
  if (
    manageableGroupIds &&
    !manageableGroupIds.includes(String(membership.groupId))
  ) {
    return next(new AppError("You cannot manage this group", 403));
  }

  const group = await GroupModel.findById(membership.groupId);
  if (!group) return next(new AppError("Group not found", 404));

  if (membership.status === "active") {
    return sendSuccess(res, { statusCode: 200, data: { membership } });
  }

  if (!isGeneralGroup(group)) {
    const conflict = await hasNonZeroGroupMembership(
      membership.userId,
      membership.groupId,
    );
    if (conflict) {
      return next(
        new AppError(
          "Member already belongs to another group. Group 0 is the only additional group allowed.",
          400,
        ),
      );
    }
  }

  if (group.memberCount >= group.maxMembers) {
    return next(new AppError("Group is full", 400));
  }

  membership.status = "active";
  membership.reviewedBy = req.user.profileId;
  membership.reviewedAt = new Date();
  membership.reviewNotes = notes;
  membership.requestedAt =
    membership.requestedAt || membership.createdAt || new Date();
  membership.joinedAt = membership.joinedAt || new Date();
  await membership.save({ validateBeforeSave: true });

  await GroupModel.findByIdAndUpdate(group._id, { $inc: { memberCount: 1 } });

  await assignGroupMemberSerial({ membership, group });

  return sendSuccess(res, {
    statusCode: 200,
    message: "Member approved",
    data: { membership },
  });
});

export const rejectMemberApplication = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const { membershipId } = req.params;
  const notes = req.body?.notes ? String(req.body.notes).trim() : null;

  const membership = await GroupMembershipModel.findById(membershipId);
  if (!membership) return next(new AppError("Membership not found", 404));

  const manageableGroupIds = await getManageableGroupIds(req);
  if (
    manageableGroupIds &&
    !manageableGroupIds.includes(String(membership.groupId))
  ) {
    return next(new AppError("You cannot manage this group", 403));
  }

  membership.status = "rejected";
  membership.reviewedBy = req.user.profileId;
  membership.reviewedAt = new Date();
  membership.reviewNotes = notes;
  membership.requestedAt =
    membership.requestedAt || membership.createdAt || new Date();
  await membership.save({ validateBeforeSave: true });

  return sendSuccess(res, {
    statusCode: 200,
    message: "Member rejected",
    data: { membership },
  });
});

export const listContributionTracker = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  if (!hasUserRole(req.user, "admin", "groupCoordinator")) {
    return next(
      new AppError(
        "Only admins and group coordinators can access contribution tracking",
        403,
      ),
    );
  }

  const { year, month, error } = parseMonthYear(req);
  if (error) return next(new AppError(error, 400));

  const manageableGroupIds = await getManageableGroupIds(req);
  const groupIdParam = req.query?.groupId ? String(req.query.groupId) : null;

  let groupIds = null;
  if (manageableGroupIds === null) {
    groupIds = groupIdParam ? [groupIdParam] : null;
  } else {
    if (groupIdParam && !manageableGroupIds.includes(groupIdParam)) {
      return next(new AppError("You cannot manage this group", 403));
    }
    groupIds = groupIdParam ? [groupIdParam] : manageableGroupIds;
  }

  const membershipFilter = { status: "active" };
  if (groupIds) membershipFilter.groupId = { $in: groupIds };

  const memberships = await GroupMembershipModel.find(membershipFilter, {
    userId: 1,
    groupId: 1,
    joinedAt: 1,
    memberSerial: 1,
    memberNumber: 1,
  }).lean();

  const memberProfiles = await ProfileModel.find(
    { _id: { $in: memberships.map((m) => m.userId) } },
    { fullName: 1, email: 1, phone: 1, contributionSettings: 1 },
  ).lean();
  const profileById = new Map(memberProfiles.map((p) => [String(p._id), p]));

  const groups = await GroupModel.find(
    { _id: { $in: [...new Set(memberships.map((m) => String(m.groupId)))] } },
    { groupName: 1, monthlyContribution: 1 },
  ).lean();
  const groupById = new Map(groups.map((g) => [String(g._id), g]));

  const scopeGroupIds = groupIds ?? groups.map((g) => String(g._id));

  const sixMonthsBack = 6;
  const historyMonths = [];
  for (let i = sixMonthsBack - 1; i >= 0; i -= 1) {
    const d = new Date(Date.UTC(year, month - 1 - i, 1));
    historyMonths.push({
      year: d.getUTCFullYear(),
      month: d.getUTCMonth() + 1,
    });
  }

  const typeMatch = getContributionTypeMatch("revolving") || ["revolving"];
  const contributionTypeFilter = {
    $or: [
      { contributionType: { $in: typeMatch } },
      { contributionType: { $exists: false } },
      { contributionType: null },
    ],
  };
  const historyFilters = historyMonths.map((hm) => ({
    year: hm.year,
    month: hm.month,
  }));
  const contributionFilters = [contributionTypeFilter];
  if (historyFilters.length > 0) {
    contributionFilters.push({ $or: historyFilters });
  }

  const contribDocs = await ContributionModel.find(
    {
      groupId: { $in: scopeGroupIds },
      ...(contributionFilters.length > 0 ? { $and: contributionFilters } : {}),
    },
    { userId: 1, groupId: 1, year: 1, month: 1, amount: 1, status: 1 },
  ).lean();

  const paidByKey = new Map();
  for (const c of contribDocs) {
    const k = `${String(c.userId)}|${String(c.groupId)}|${Number(c.year)}|${Number(c.month)}`;
    const isPaid = ["verified", "completed"].includes(String(c.status));
    if (!isPaid) continue;
    paidByKey.set(k, Number(paidByKey.get(k) || 0) + Number(c.amount || 0));
  }

  const due = dueDateUtc(year, month);
  const now = new Date();
  const records = memberships.map((m) => {
    const userId = String(m.userId);
    const groupId = String(m.groupId);
    const g = groupById.get(groupId);
    const p = profileById.get(userId);

    const expectedAmount = resolveExpectedContributionAmount({
      settings: p?.contributionSettings,
      year,
      groupMonthlyContribution: g?.monthlyContribution,
      type: "revolving",
    });
    const currentKey = `${userId}|${groupId}|${year}|${month}`;
    const paidAmount = Number(paidByKey.get(currentKey) || 0);

    const status =
      paidAmount >= expectedAmount && expectedAmount > 0
        ? "paid"
        : paidAmount > 0
          ? "partial"
          : now.getTime() > due.getTime()
            ? "defaulted"
            : "pending";

    let monthsDefaulted = 0;
    const joinedAt = m.joinedAt ? new Date(m.joinedAt) : null;
    for (const hm of historyMonths) {
      const monthDue = dueDateUtc(hm.year, hm.month);
      if (monthDue.getTime() > now.getTime()) continue;
      if (joinedAt && monthDue.getTime() < joinedAt.getTime()) continue;
      const k = `${userId}|${groupId}|${hm.year}|${hm.month}`;
      const paid = Number(paidByKey.get(k) || 0);
      if (paid <= 0) monthsDefaulted += 1;
    }

    return {
      id: `${userId}|${groupId}|${year}|${month}`,
      userId,
      groupId,
      memberName: p?.fullName ?? "Member",
      memberSerial: m.memberSerial ?? null,
      groupName: g?.groupName ?? "Group",
      expectedAmount,
      paidAmount,
      dueDate: due.toISOString().slice(0, 10),
      status,
      monthsDefaulted: clamp(monthsDefaulted, 0, 12),
    };
  });

  return sendSuccess(res, {
    statusCode: 200,
    results: records.length,
    data: { contributions: records },
  });
});

export const sendContributionReminders = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  if (!hasUserRole(req.user, "groupCoordinator")) {
    return next(
      new AppError("Only group coordinators can send reminders", 403),
    );
  }

  const { year, month, error } = parseMonthYearPayload(req);
  if (error) return next(new AppError(error, 400));

  const sendEmailFlag = Boolean(req.body?.sendEmail ?? false);
  const sendSmsFlag = Boolean(req.body?.sendSMS ?? false);
  const sendNotificationFlag = Boolean(req.body?.sendNotification ?? true);

  if (!sendEmailFlag && !sendSmsFlag && !sendNotificationFlag) {
    return next(new AppError("Select at least one delivery method", 400));
  }

  const rawRecipients = Array.isArray(req.body?.recipients)
    ? req.body.recipients
    : [];
  const recipients = rawRecipients
    .map((item) => ({
      userId: String(item?.userId || "").trim(),
      groupId: String(item?.groupId || "").trim(),
    }))
    .filter((item) => item.userId && item.groupId);

  if (recipients.length === 0) {
    return next(new AppError("Recipients are required", 400));
  }

  const manageableGroupIds = await getManageableGroupIds(req);
  const scopedRecipients =
    manageableGroupIds && manageableGroupIds.length > 0
      ? recipients.filter((r) => manageableGroupIds.includes(r.groupId))
      : recipients;

  if (scopedRecipients.length === 0) {
    return next(
      new AppError("No recipients are within your managed groups", 403),
    );
  }

  const userIds = uniqueStrings(scopedRecipients.map((r) => r.userId));
  const groupIds = uniqueStrings(scopedRecipients.map((r) => r.groupId));

  const memberships = await GroupMembershipModel.find(
    { userId: { $in: userIds }, groupId: { $in: groupIds }, status: "active" },
    { userId: 1, groupId: 1 },
  ).lean();

  const activeKeys = new Set(
    memberships.map((m) => `${String(m.userId)}|${String(m.groupId)}`),
  );
  const activeRecipients = scopedRecipients.filter((r) =>
    activeKeys.has(`${r.userId}|${r.groupId}`),
  );

  if (activeRecipients.length === 0) {
    return next(
      new AppError("No active members found for the selected reminders", 404),
    );
  }

  const [profiles, groups] = await Promise.all([
    ProfileModel.find(
      { _id: { $in: userIds } },
      { fullName: 1, email: 1, phone: 1, contributionSettings: 1 },
    ).lean(),
    GroupModel.find(
      { _id: { $in: groupIds } },
      { groupName: 1, monthlyContribution: 1 },
    ).lean(),
  ]);

  const profileById = new Map(profiles.map((p) => [String(p._id), p]));
  const groupById = new Map(groups.map((g) => [String(g._id), g]));

  const label = formatMonthLabel(year, month);
  const channels = {
    email: {
      requested: sendEmailFlag,
      attempted: 0,
      sent: 0,
      failed: 0,
      skipped: 0,
    },
    sms: {
      requested: sendSmsFlag,
      attempted: 0,
      sent: 0,
      failed: 0,
      skipped: 0,
    },
    notification: {
      requested: sendNotificationFlag,
      attempted: 0,
      sent: 0,
      failed: 0,
      skipped: 0,
    },
  };
  const failures = [];

  const notifications = [];
  const emailTargets = [];
  const smsTargets = [];

  for (const recipient of activeRecipients) {
    const profile = profileById.get(String(recipient.userId));
    const group = groupById.get(String(recipient.groupId));
    if (!group) continue;

    const expectedAmount = resolveExpectedContributionAmount({
      settings: profile?.contributionSettings,
      year,
      groupMonthlyContribution: group?.monthlyContribution,
      type: "revolving",
    });
    const message = buildContributionReminderMessage({
      groupName: group?.groupName || "your group",
      label,
      amount: expectedAmount,
    });
    const title = `${group?.groupName || "Group"} Contribution Reminder`;

    if (sendNotificationFlag) {
      notifications.push({
        userId: recipient.userId,
        title,
        message,
        type: "group_contribution_reminder",
        status: "unread",
        metadata: {
          groupId: recipient.groupId,
          year,
          month,
        },
      });
    }

    if (sendEmailFlag) {
      if (profile?.email) {
        emailTargets.push({
          to: profile.email,
          subject: title,
          message,
        });
      } else {
        channels.email.skipped += 1;
      }
    }

    if (sendSmsFlag) {
      const phones = profile?.phone ? splitCsv(profile.phone) : [];
      if (phones.length === 0) {
        channels.sms.skipped += 1;
      } else {
        phones.forEach((phone) => smsTargets.push({ to: phone, message }));
      }
    }
  }

  if (sendNotificationFlag) {
    channels.notification.attempted = notifications.length;
    if (notifications.length === 0) {
      channels.notification.skipped += 1;
    } else {
      try {
        const saved = await NotificationModel.insertMany(notifications, {
          ordered: false,
        });
        channels.notification.sent = saved.length;
        channels.notification.failed = notifications.length - saved.length;
        for (const notification of saved) {
          emitToUser(notification.userId, "notification:new", { notification });
        }
        if (channels.notification.failed > 0) {
          failures.push({
            channel: "notification",
            to: "some recipients",
            error: "Some notifications could not be created",
          });
        }
      } catch (err) {
        channels.notification.failed = notifications.length;
        failures.push({
          channel: "notification",
          to: "recipients",
          error: err
            ? String(err?.message ?? err)
            : "Notification creation failed",
        });
      }
    }
  }

  if (sendEmailFlag) {
    channels.email.attempted = emailTargets.length;
    const results = await Promise.allSettled(
      emailTargets.map((target) =>
        sendEmail({
          to: target.to,
          subject: target.subject,
          html: `<p>${target.message}</p>`,
          text: target.message,
        }),
      ),
    );
    results.forEach((result, idx) => {
      if (result.status === "fulfilled") {
        channels.email.sent += 1;
      } else {
        channels.email.failed += 1;
        failures.push({
          channel: "email",
          to: emailTargets[idx]?.to,
          error: result.reason
            ? String(result.reason?.message ?? result.reason)
            : "Email failed",
        });
      }
    });
  }

  if (sendSmsFlag) {
    channels.sms.attempted = smsTargets.length;
    const results = await Promise.allSettled(
      smsTargets.map((target) =>
        sendSms({
          to: target.to,
          message: target.message,
        }),
      ),
    );
    results.forEach((result, idx) => {
      if (result.status === "fulfilled") {
        channels.sms.sent += 1;
      } else {
        channels.sms.failed += 1;
        failures.push({
          channel: "sms",
          to: smsTargets[idx]?.to,
          error: result.reason
            ? String(result.reason?.message ?? result.reason)
            : "SMS failed",
        });
      }
    });
  }

  return sendSuccess(res, {
    statusCode: 200,
    message: "Reminders dispatched",
    data: {
      totalRecipients: activeRecipients.length,
      channels,
      failures,
    },
  });
});

export const markContributionPaid = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  if (!hasUserRole(req.user, "groupCoordinator")) {
    return next(
      new AppError(
        "Only group coordinators can mark contributions as paid",
        403,
      ),
    );
  }

  const userId = String(req.body?.userId || "").trim();
  const groupId = String(req.body?.groupId || "").trim();
  const month = Number(req.body?.month);
  const year = Number(req.body?.year);
  const amount = Number(req.body?.amount);
  const contributionTypeRaw = req.body?.contributionType;
  const paymentMethod = String(req.body?.paymentMethod || "bank_transfer")
    .trim()
    .toLowerCase();
  const manualPaymentReference = req.body?.paymentReference
    ? String(req.body.paymentReference).trim()
    : null;
  const description = req.body?.description
    ? String(req.body.description).trim()
    : "";

  if (!userId || !groupId)
    return next(new AppError("userId and groupId are required", 400));
  if (!Number.isFinite(month) || month < 1 || month > 12)
    return next(new AppError("Invalid month", 400));
  if (!Number.isFinite(year) || year < 2000 || year > 2100)
    return next(new AppError("Invalid year", 400));
  if (!Number.isFinite(amount) || amount <= 0)
    return next(new AppError("amount must be > 0", 400));
  if (!ManualContributionPaymentMethods.has(paymentMethod)) {
    return next(new AppError("Invalid paymentMethod", 400));
  }

  const normalizedTypeRaw = normalizeContributionType(contributionTypeRaw);
  if (contributionTypeRaw !== undefined && !normalizedTypeRaw) {
    return next(new AppError("Invalid contributionType", 400));
  }
  const normalizedType = normalizedTypeRaw || "revolving";

  const manageableGroupIds = await getManageableGroupIds(req);
  if (manageableGroupIds && !manageableGroupIds.includes(groupId)) {
    return next(new AppError("You cannot manage this group", 403));
  }

  const membership = await GroupMembershipModel.findOne({
    groupId,
    userId,
    status: "active",
  });
  if (!membership)
    return next(new AppError("Member is not active in this group", 400));

  const group = await GroupModel.findById(groupId, { groupName: 1 });
  if (!group) return next(new AppError("Group not found", 404));

  if (!isContributionAmountValid(normalizedType, amount)) {
    return next(
      new AppError("Amount does not meet contribution requirements", 400),
    );
  }

  const verifiedAt = new Date();
  const reference = generateReference("CRC-MAN");
  const transactionDescription =
    description ||
    `Manual ${normalizedType} contribution for ${formatMonthLabel(year, month)}`;

  const contribution = new ContributionModel({
    userId,
    groupId,
    month,
    year,
    contributionType: normalizedType,
    amount,
    status: "verified",
    paymentMethod,
    paymentReference: reference,
    verifiedBy: req.user.profileId,
    verifiedAt,
    notes: description || null,
  });
  applyContributionMetrics(contribution, amount);
  await contribution.save({ validateBeforeSave: true });

  const transaction = await TransactionModel.create({
    userId,
    reference,
    amount,
    type: "group_contribution",
    status: "success",
    description: transactionDescription,
    channel: paymentMethod,
    groupId,
    groupName: group.groupName || "Group",
    metadata: {
      paymentType: "group_contribution",
      contributionId: contribution._id,
      month,
      year,
      contributionType: normalizedType,
      manual: true,
      manualPaymentReference: manualPaymentReference || null,
      recordedBy: req.user.profileId,
      recordedAt: verifiedAt.toISOString(),
    },
    gateway: "manual",
  });

  await Promise.all([
    GroupModel.findByIdAndUpdate(groupId, { $inc: { totalSavings: amount } }),
    GroupMembershipModel.findOneAndUpdate(
      { groupId, userId },
      { $inc: { totalContributed: amount } },
    ),
    updateRecurringPaymentStats({
      userId,
      paymentType: "group_contribution",
      groupId,
      amount,
      count: 1,
      paidAt: verifiedAt,
    }),
  ]);

  return sendSuccess(res, {
    statusCode: 200,
    message: "Contribution marked as paid",
    data: { contribution, transaction },
  });
});
