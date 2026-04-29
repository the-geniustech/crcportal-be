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
import { generateAdminContributionTrackerWorkbookBuffer } from "../services/adminContributionTrackerWorkbook.js";
import {
  applyRecurringContributionSchedulePayment,
  attachRecurringContributionSchedule,
  findMatchingRecurringContributionSchedule,
  rebuildRecurringContributionSchedule,
} from "../utils/recurringContributionLink.js";
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

function parseContributionTypeQuery(req, fallback = "revolving") {
  const raw = String(req.query?.contributionType ?? fallback).trim();
  const contributionType = normalizeContributionType(raw);
  if (!contributionType) {
    return { error: "Invalid contributionType" };
  }
  return { contributionType };
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

const PaidContributionStatuses = new Set(["completed", "verified"]);
const MonthlyTrackedContributionTypes = new Set([
  "revolving",
  "endwell",
  "festive",
]);
const ContributionTrackerSortLabels = {
  member_name_asc: "Member name (A-Z)",
  member_name_desc: "Member name (Z-A)",
  group_name_asc: "Group name (A-Z)",
  group_name_desc: "Group name (Z-A)",
  expected_desc: "Expected amount (high-low)",
  expected_asc: "Expected amount (low-high)",
  paid_desc: "Paid amount (high-low)",
  paid_asc: "Paid amount (low-high)",
  status: "Status",
  defaulted_desc: "Months defaulted (high-low)",
};
const ContributionTypeLabels = {
  revolving: "Revolving Contribution",
  special: "Special Contribution",
  endwell: "Endwell Contribution",
  festive: "Festive Contribution",
};

function shouldCountTowardSavings(status) {
  return status === "completed" || status === "verified";
}

function mapContributionStatusToTxStatus(status) {
  if (status === "completed" || status === "verified") return "success";
  if (status === "pending") return "pending";
  if (status === "overdue") return "failed";
  return "pending";
}

function normalizeContributionTrackerSort(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (Object.prototype.hasOwnProperty.call(ContributionTrackerSortLabels, raw)) {
    return raw;
  }
  return "member_name_asc";
}

function getContributionTrackerSortLabel(value) {
  const normalized = normalizeContributionTrackerSort(value);
  return ContributionTrackerSortLabels[normalized];
}

function getContributionTrackerTypeLabel(value) {
  const canonical = normalizeContributionType(value) || "revolving";
  return ContributionTypeLabels[canonical] || "Contribution";
}

function getStatusSortRank(status) {
  switch (status) {
    case "paid":
      return 0;
    case "partial":
      return 1;
    case "pending":
      return 2;
    case "defaulted":
      return 3;
    default:
      return 9;
  }
}

function formatContributionTrackerPeriodLabel(year, month) {
  const date = new Date(Date.UTC(year, month - 1, 1));
  return date.toLocaleDateString("en-NG", {
    month: "long",
    year: "numeric",
  });
}

function formatContributionTrackerTimestamp(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat("en-NG", {
    day: "numeric",
    month: "short",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function csvEscape(value) {
  const raw = String(value ?? "");
  if (/[",\n]/.test(raw)) {
    return `"${raw.replace(/"/g, '""')}"`;
  }
  return raw;
}

function filterContributionTrackerRecords(records, { search, status } = {}) {
  const searchQuery = String(search || "")
    .trim()
    .toLowerCase();
  const normalizedStatus = String(status || "all")
    .trim()
    .toLowerCase();

  return records.filter((record) => {
    const matchesSearch =
      !searchQuery ||
      String(record.memberName || "")
        .toLowerCase()
        .includes(searchQuery) ||
      String(record.groupName || "")
        .toLowerCase()
        .includes(searchQuery) ||
      String(record.memberSerial || "")
        .toLowerCase()
        .includes(searchQuery);
    const matchesStatus =
      normalizedStatus === "all" || record.status === normalizedStatus;
    return matchesSearch && matchesStatus;
  });
}

function sortContributionTrackerRecords(records, sort) {
  const normalizedSort = normalizeContributionTrackerSort(sort);
  const list = [...records];
  list.sort((a, b) => {
    const memberA = String(a.memberName || "").localeCompare(
      String(b.memberName || ""),
      "en",
      { sensitivity: "base" },
    );
    const groupA = String(a.groupName || "").localeCompare(
      String(b.groupName || ""),
      "en",
      { sensitivity: "base" },
    );

    switch (normalizedSort) {
      case "member_name_desc":
        return -memberA || groupA;
      case "group_name_asc":
        return groupA || memberA;
      case "group_name_desc":
        return -groupA || memberA;
      case "expected_desc":
        return (
          Number(b.expectedAmount || 0) - Number(a.expectedAmount || 0) ||
          memberA
        );
      case "expected_asc":
        return (
          Number(a.expectedAmount || 0) - Number(b.expectedAmount || 0) ||
          memberA
        );
      case "paid_desc":
        return Number(b.paidAmount || 0) - Number(a.paidAmount || 0) || memberA;
      case "paid_asc":
        return Number(a.paidAmount || 0) - Number(b.paidAmount || 0) || memberA;
      case "status":
        return (
          getStatusSortRank(a.status) - getStatusSortRank(b.status) || memberA
        );
      case "defaulted_desc":
        return (
          Number(b.monthsDefaulted || 0) - Number(a.monthsDefaulted || 0) ||
          memberA
        );
      case "member_name_asc":
      default:
        return memberA || groupA;
    }
  });
  return list;
}

function summarizeContributionTrackerRecords(records) {
  const totalExpected = records.reduce(
    (sum, record) => sum + Number(record.expectedAmount || 0),
    0,
  );
  const totalPaid = records.reduce(
    (sum, record) => sum + Number(record.paidAmount || 0),
    0,
  );
  const defaulters = records.filter((record) => record.status === "defaulted").length;
  const collectionRate = totalExpected > 0 ? (totalPaid / totalExpected) * 100 : 0;
  return {
    totalExpected,
    totalPaid,
    defaulters,
    collectionRate,
  };
}

async function resolveContributionRecurringScheduleIds(
  contributions,
  session = null,
) {
  const recurringScheduleIds = new Set();

  for (const contribution of contributions) {
    if (contribution?.recurringPaymentId) {
      recurringScheduleIds.add(String(contribution.recurringPaymentId));
      continue;
    }

    const fallbackSchedule = await findMatchingRecurringContributionSchedule({
      userId: contribution?.userId,
      groupId: contribution?.groupId,
      contributionType: contribution?.contributionType,
      amount: Number(contribution?.amount || 0),
      session,
      isActive: undefined,
    });
    if (fallbackSchedule?._id) {
      recurringScheduleIds.add(String(fallbackSchedule._id));
    }
  }

  return recurringScheduleIds;
}

async function reconcileContributionTransactionsAfterDeletion({
  deletedContributionIds,
  session = null,
} = {}) {
  if (!Array.isArray(deletedContributionIds) || deletedContributionIds.length === 0) {
    return { deletedTransactions: 0, updatedTransactions: 0 };
  }

  const deletedIdSet = new Set(deletedContributionIds.map((id) => String(id)));
  const transactionQuery = TransactionModel.find({
    $or: [
      { "metadata.contributionId": { $in: deletedContributionIds } },
      { "metadata.bulkContributionIds": { $in: deletedContributionIds } },
    ],
  });
  if (session) transactionQuery.session(session);
  const transactions = await transactionQuery;

  let deletedTransactions = 0;
  let updatedTransactions = 0;

  for (const transaction of transactions) {
    const metadata = transaction.metadata || {};
    const bulkIds = Array.isArray(metadata.bulkContributionIds)
      ? metadata.bulkContributionIds.map((value) => String(value))
      : [];
    const singleId = metadata.contributionId
      ? String(metadata.contributionId)
      : null;

    if (bulkIds.length > 0) {
      const remainingBulkIds = bulkIds.filter((id) => !deletedIdSet.has(id));
      if (remainingBulkIds.length === 0) {
        await TransactionModel.deleteOne(
          { _id: transaction._id },
          session ? { session } : undefined,
        );
        deletedTransactions += 1;
        continue;
      }

      const remainingContributionQuery = ContributionModel.find(
        { _id: { $in: remainingBulkIds } },
        { amount: 1, groupId: 1 },
      );
      if (session) remainingContributionQuery.session(session);
      const remainingContributions = await remainingContributionQuery;
      const remainingIdSet = new Set(
        remainingContributions.map((contribution) => String(contribution._id)),
      );
      const remainingAmount = remainingContributions.reduce(
        (sum, contribution) => sum + Number(contribution.amount || 0),
        0,
      );

      const nextMetadata = {
        ...metadata,
        bulkContributionIds: remainingBulkIds.filter((id) => remainingIdSet.has(id)),
      };

      if (
        Array.isArray(metadata.bulkContributionLinks) &&
        metadata.bulkContributionLinks.length > 0
      ) {
        nextMetadata.bulkContributionLinks = metadata.bulkContributionLinks.filter(
          (item) =>
            item?.contributionId &&
            remainingIdSet.has(String(item.contributionId)),
        );
      }

      if (
        Array.isArray(metadata.bulkItems) &&
        metadata.bulkItems.length === bulkIds.length
      ) {
        nextMetadata.bulkItems = metadata.bulkItems.filter((_item, index) =>
          remainingIdSet.has(String(bulkIds[index])),
        );
      }

      transaction.amount = remainingAmount;
      transaction.metadata = nextMetadata;
      await transaction.save({ session: session || undefined, validateBeforeSave: true });
      updatedTransactions += 1;
      continue;
    }

    if (singleId && deletedIdSet.has(singleId)) {
      await TransactionModel.deleteOne(
        { _id: transaction._id },
        session ? { session } : undefined,
      );
      deletedTransactions += 1;
    }
  }

  return { deletedTransactions, updatedTransactions };
}

async function updateRecurringPaymentStats({
  userId,
  paymentType,
  groupId,
  loanId,
  contributionType,
  recurringPaymentId,
  amount,
  count = 1,
  paidAt,
} = {}) {
  if (!userId || !paymentType) return;

  if (paymentType === "group_contribution") {
    return applyRecurringContributionSchedulePayment({
      recurringPaymentId,
      userId,
      groupId,
      contributionType,
      amount,
      count,
      paidAt,
    });
  }

  const query = { userId, paymentType, isActive: true };
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

async function buildContributionTrackerRecords({
  req,
  year,
  month,
  contributionType,
  groupIdParam = null,
}) {
  const manageableGroupIds = await getManageableGroupIds(req);

  let groupIds = null;
  if (manageableGroupIds === null) {
    groupIds = groupIdParam ? [groupIdParam] : null;
  } else {
    if (groupIdParam && !manageableGroupIds.includes(groupIdParam)) {
      throw new AppError("You cannot manage this group", 403);
    }
    groupIds = groupIdParam ? [groupIdParam] : manageableGroupIds;
  }

  const membershipFilter = { status: "active" };
  if (groupIds) membershipFilter.groupId = { $in: groupIds };

  const memberships = await GroupMembershipModel.find(
    membershipFilter,
    {
      userId: 1,
      groupId: 1,
      joinedAt: 1,
      memberSerial: 1,
      memberNumber: 1,
    },
  ).lean();

  if (memberships.length === 0) {
    return {
      records: [],
      groupLabel: groupIdParam ? "Selected group" : "All groups",
    };
  }

  const memberProfiles = await ProfileModel.find(
    { _id: { $in: memberships.map((membership) => membership.userId) } },
    { fullName: 1, email: 1, phone: 1, contributionSettings: 1 },
  ).lean();
  const profileById = new Map(
    memberProfiles.map((profile) => [String(profile._id), profile]),
  );

  const groups = await GroupModel.find(
    {
      _id: {
        $in: [...new Set(memberships.map((membership) => String(membership.groupId)))],
      },
    },
    { groupName: 1, monthlyContribution: 1 },
  ).lean();
  const groupById = new Map(groups.map((group) => [String(group._id), group]));

  const scopeGroupIds = groupIds ?? groups.map((group) => String(group._id));
  const isMonthlyTrackedType = MonthlyTrackedContributionTypes.has(contributionType);

  const historyMonths = [];
  if (isMonthlyTrackedType) {
    const sixMonthsBack = 6;
    for (let i = sixMonthsBack - 1; i >= 0; i -= 1) {
      const date = new Date(Date.UTC(year, month - 1 - i, 1));
      historyMonths.push({
        year: date.getUTCFullYear(),
        month: date.getUTCMonth() + 1,
      });
    }
  } else {
    historyMonths.push({ year, month });
  }

  const typeMatch = getContributionTypeMatch(contributionType) || [contributionType];
  const contributionTypeFilter =
    contributionType === "revolving"
      ? {
          $or: [
            { contributionType: { $in: typeMatch } },
            { contributionType: { $exists: false } },
            { contributionType: null },
          ],
        }
      : { contributionType: { $in: typeMatch } };

  const contribDocs = await ContributionModel.find(
    {
      groupId: { $in: scopeGroupIds },
      ...(historyMonths.length > 0
        ? {
            $or: historyMonths.map((item) => ({
              year: item.year,
              month: item.month,
            })),
          }
        : {}),
      ...contributionTypeFilter,
    },
    {
      userId: 1,
      groupId: 1,
      year: 1,
      month: 1,
      amount: 1,
      status: 1,
    },
  ).lean();

  const paidByKey = new Map();
  for (const contribution of contribDocs) {
    const key = `${String(contribution.userId)}|${String(contribution.groupId)}|${Number(contribution.year)}|${Number(contribution.month)}`;
    if (!PaidContributionStatuses.has(String(contribution.status))) continue;
    paidByKey.set(
      key,
      Number(paidByKey.get(key) || 0) + Number(contribution.amount || 0),
    );
  }

  const due = dueDateUtc(year, month);
  const now = new Date();

  const records = memberships.map((membership) => {
    const userId = String(membership.userId);
    const groupId = String(membership.groupId);
    const group = groupById.get(groupId);
    const profile = profileById.get(userId);
    const expectedAmount = isMonthlyTrackedType
      ? resolveExpectedContributionAmount({
          settings: profile?.contributionSettings,
          year,
          groupMonthlyContribution: group?.monthlyContribution,
          type: contributionType,
        })
      : 0;
    const currentKey = `${userId}|${groupId}|${year}|${month}`;
    const paidAmount = Number(paidByKey.get(currentKey) || 0);

    let status = "pending";
    let monthsDefaulted = 0;

    if (!isMonthlyTrackedType) {
      status = paidAmount > 0 ? "paid" : "pending";
    } else {
      status =
        paidAmount >= expectedAmount && expectedAmount > 0
          ? "paid"
          : paidAmount > 0
            ? "partial"
            : now.getTime() > due.getTime()
              ? "defaulted"
              : "pending";

      const joinedAt = membership.joinedAt ? new Date(membership.joinedAt) : null;
      for (const item of historyMonths) {
        const monthDue = dueDateUtc(item.year, item.month);
        if (monthDue.getTime() > now.getTime()) continue;
        if (joinedAt && monthDue.getTime() < joinedAt.getTime()) continue;
        const historyKey = `${userId}|${groupId}|${item.year}|${item.month}`;
        const paid = Number(paidByKey.get(historyKey) || 0);
        if (paid <= 0) monthsDefaulted += 1;
      }
    }

    return {
      id: `${userId}|${groupId}|${year}|${month}`,
      userId,
      groupId,
      contributionType,
      memberName: profile?.fullName ?? "Member",
      memberSerial: membership.memberSerial ?? null,
      groupName: group?.groupName ?? "Group",
      expectedAmount,
      paidAmount,
      dueDate: isMonthlyTrackedType ? due.toISOString().slice(0, 10) : null,
      status,
      monthsDefaulted: clamp(monthsDefaulted, 0, 12),
    };
  });

  let groupLabel = "All groups";
  if (groupIds?.length === 1) {
    groupLabel = groupById.get(groupIds[0])?.groupName || "Selected group";
  }

  return {
    records,
    groupLabel,
  };
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
  const parsedType = parseContributionTypeQuery(req);
  if (parsedType.error) return next(new AppError(parsedType.error, 400));

  const groupIdParam = req.query?.groupId ? String(req.query.groupId) : null;
  const { records } = await buildContributionTrackerRecords({
    req,
    year,
    month,
    contributionType: parsedType.contributionType,
    groupIdParam,
  });
  const sort = normalizeContributionTrackerSort(req.query?.sort);
  const sortedRecords = sortContributionTrackerRecords(records, sort);

  return sendSuccess(res, {
    statusCode: 200,
    results: sortedRecords.length,
    data: {
      year,
      month,
      contributionType: parsedType.contributionType,
      contributions: sortedRecords,
    },
  });
});

export const exportContributionTracker = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  if (!hasUserRole(req.user, "admin", "groupCoordinator")) {
    return next(
      new AppError(
        "Only admins and group coordinators can export contribution tracking",
        403,
      ),
    );
  }

  const { year, month, error } = parseMonthYear(req);
  if (error) return next(new AppError(error, 400));

  const parsedType = parseContributionTypeQuery(req);
  if (parsedType.error) return next(new AppError(parsedType.error, 400));

  const format = String(req.query?.format || "csv")
    .trim()
    .toLowerCase();
  if (!["csv", "xlsx"].includes(format)) {
    return next(new AppError("Invalid export format", 400));
  }

  const groupIdParam = req.query?.groupId ? String(req.query.groupId) : null;
  const status = req.query?.status ? String(req.query.status) : "all";
  const search = req.query?.search ? String(req.query.search) : "";
  const sort = normalizeContributionTrackerSort(req.query?.sort);

  const { records, groupLabel } = await buildContributionTrackerRecords({
    req,
    year,
    month,
    contributionType: parsedType.contributionType,
    groupIdParam,
  });

  const filteredRecords = sortContributionTrackerRecords(
    filterContributionTrackerRecords(records, { search, status }),
    sort,
  );
  const summary = summarizeContributionTrackerRecords(filteredRecords);
  const contributionTypeLabel = getContributionTrackerTypeLabel(
    parsedType.contributionType,
  );
  const periodLabel = formatContributionTrackerPeriodLabel(year, month);
  const generatedAt = new Date();
  const statusLabel =
    String(status || "all")
      .trim()
      .toLowerCase() === "all"
      ? "All statuses"
      : String(status || "all");
  const searchLabel = String(search || "").trim() || "All records";
  const sortLabel = getContributionTrackerSortLabel(sort);
  const filenameBase = `admin-contribution-tracker-${String(
    parsedType.contributionType,
  )}-${year}-${String(month).padStart(2, "0")}`;

  if (format === "csv") {
    const rows = [
      ["Contribution Tracker Export"],
      ["Contribution Type", contributionTypeLabel],
      ["Period", periodLabel],
      ["Generated", formatContributionTrackerTimestamp(generatedAt)],
      ["Group Filter", groupLabel],
      ["Status Filter", statusLabel],
      ["Search", searchLabel],
      ["Sorted By", sortLabel],
      [],
      ["Summary"],
      ["Records", filteredRecords.length],
      ["Collection Rate", `${summary.collectionRate.toFixed(1)}%`],
      ["Total Expected", summary.totalExpected],
      ["Total Collected", summary.totalPaid],
      ["Defaulters", summary.defaulters],
      [],
      [
        "S/N",
        "Member Serial",
        "Member Name",
        "Group",
        "Expected",
        "Paid",
        "Due Date",
        "Status",
        "Months Defaulted",
      ],
      ...filteredRecords.map((record, index) => [
        index + 1,
        record.memberSerial ?? "-",
        record.memberName,
        record.groupName,
        Number(record.expectedAmount || 0),
        Number(record.paidAmount || 0),
        record.dueDate || "Anytime",
        record.status,
        Number(record.monthsDefaulted || 0),
      ]),
    ];

    const csv = `\uFEFF${rows
      .map((row) => row.map((value) => csvEscape(value)).join(","))
      .join("\n")}`;

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${filenameBase}.csv"`,
    );
    return res.status(200).send(csv);
  }

  const workbookBuffer = await generateAdminContributionTrackerWorkbookBuffer({
    contributionTypeLabel,
    periodLabel,
    generatedAt,
    groupLabel,
    statusLabel,
    searchLabel,
    sortLabel,
    records: filteredRecords,
    summary,
  });

  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  );
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="${filenameBase}.xlsx"`,
  );
  return res.status(200).send(workbookBuffer);
});

export const listContributionTrackerEntries = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  if (!hasUserRole(req.user, "admin", "groupCoordinator")) {
    return next(
      new AppError(
        "Only admins and group coordinators can view contribution entries",
        403,
      ),
    );
  }

  const { year, month, error } = parseMonthYear(req);
  if (error) return next(new AppError(error, 400));

  const parsedType = parseContributionTypeQuery(req);
  if (parsedType.error) return next(new AppError(parsedType.error, 400));

  const userId = String(req.query?.userId || "").trim();
  const groupId = String(req.query?.groupId || "").trim();
  if (!userId || !groupId) {
    return next(new AppError("userId and groupId are required", 400));
  }

  const manageableGroupIds = await getManageableGroupIds(req);
  if (manageableGroupIds && !manageableGroupIds.includes(groupId)) {
    return next(new AppError("You cannot manage this group", 403));
  }

  const typeMatch = getContributionTypeMatch(parsedType.contributionType) || [
    parsedType.contributionType,
  ];
  const contributionTypeFilter =
    parsedType.contributionType === "revolving"
      ? {
          $or: [
            { contributionType: { $in: typeMatch } },
            { contributionType: { $exists: false } },
            { contributionType: null },
          ],
        }
      : { contributionType: { $in: typeMatch } };

  const entries = await ContributionModel.find(
    {
      userId,
      groupId,
      year,
      month,
      ...contributionTypeFilter,
    },
    {
      amount: 1,
      month: 1,
      year: 1,
      status: 1,
      paymentMethod: 1,
      paymentReference: 1,
      notes: 1,
      units: 1,
      interestAmount: 1,
      contributionType: 1,
      recurringPaymentId: 1,
      createdAt: 1,
      updatedAt: 1,
      verifiedAt: 1,
    },
  )
    .sort({ createdAt: -1, _id: -1 })
    .lean();

  const contributionIds = entries.map((entry) => entry._id);
  const transactions = contributionIds.length
    ? await TransactionModel.find(
        {
          $or: [
            { "metadata.contributionId": { $in: contributionIds } },
            { "metadata.bulkContributionIds": { $in: contributionIds } },
          ],
        },
        {
          reference: 1,
          amount: 1,
          status: 1,
          channel: 1,
          description: 1,
          date: 1,
          metadata: 1,
        },
      ).lean()
    : [];

  const transactionByContributionId = new Map();
  transactions.forEach((transaction) => {
    const linkedContributionIds = [];
    if (transaction?.metadata?.contributionId) {
      linkedContributionIds.push(String(transaction.metadata.contributionId));
    }
    if (Array.isArray(transaction?.metadata?.bulkContributionIds)) {
      transaction.metadata.bulkContributionIds.forEach((value) => {
        if (!value) return;
        linkedContributionIds.push(String(value));
      });
    }
    linkedContributionIds.forEach((contributionId) => {
      if (!contributionId || transactionByContributionId.has(contributionId)) return;
      transactionByContributionId.set(contributionId, transaction);
    });
  });

  const normalizedEntries = entries.map((entry) => {
    const transaction = transactionByContributionId.get(String(entry._id)) || null;
    return {
      id: String(entry._id),
      amount: Number(entry.amount || 0),
      month: Number(entry.month || month),
      year: Number(entry.year || year),
      status: entry.status || "pending",
      paymentMethod: entry.paymentMethod || null,
      paymentReference: entry.paymentReference || null,
      notes: entry.notes || null,
      units: Number(entry.units || 0),
      interestAmount: Number(entry.interestAmount || 0),
      contributionType:
        normalizeContributionType(entry.contributionType) ||
        parsedType.contributionType,
      recurringPaymentId: entry.recurringPaymentId ? String(entry.recurringPaymentId) : null,
      createdAt: entry.createdAt || null,
      updatedAt: entry.updatedAt || null,
      verifiedAt: entry.verifiedAt || null,
      transaction: transaction
        ? {
            id: String(transaction._id),
            reference: transaction.reference || null,
            amount: Number(transaction.amount || 0),
            status: transaction.status || null,
            channel: transaction.channel || null,
            description: transaction.description || null,
            date: transaction.date || null,
          }
        : null,
    };
  });

  return sendSuccess(res, {
    statusCode: 200,
    results: normalizedEntries.length,
    data: {
      year,
      month,
      contributionType: parsedType.contributionType,
      entries: normalizedEntries,
    },
  });
});

export const updateTrackedContribution = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  if (!hasUserRole(req.user, "admin", "groupCoordinator")) {
    return next(
      new AppError(
        "Only admins and group coordinators can edit contributions",
        403,
      ),
    );
  }

  const { contributionId } = req.params;
  if (!mongoose.Types.ObjectId.isValid(contributionId)) {
    return next(new AppError("Invalid contributionId", 400));
  }

  const existing = await ContributionModel.findById(contributionId);
  if (!existing) return next(new AppError("Contribution not found", 404));

  const manageableGroupIds = await getManageableGroupIds(req);
  if (
    manageableGroupIds &&
    !manageableGroupIds.includes(String(existing.groupId))
  ) {
    return next(new AppError("You cannot manage this contribution", 403));
  }

  const updates = pick(req.body || {}, [
    "amount",
    "month",
    "year",
    "paymentMethod",
    "paymentReference",
    "notes",
  ]);
  if (Object.keys(updates).length === 0) {
    return next(new AppError("No editable contribution fields provided", 400));
  }

  if (Object.prototype.hasOwnProperty.call(updates, "month")) {
    const nextMonth = Number(updates.month);
    if (!Number.isFinite(nextMonth) || nextMonth < 1 || nextMonth > 12) {
      return next(new AppError("Invalid month", 400));
    }
    updates.month = nextMonth;
  }

  if (Object.prototype.hasOwnProperty.call(updates, "year")) {
    const nextYear = Number(updates.year);
    if (!Number.isFinite(nextYear) || nextYear < 2000 || nextYear > 2100) {
      return next(new AppError("Invalid year", 400));
    }
    updates.year = nextYear;
  }

  const normalizedType = normalizeContributionType(existing.contributionType) || "revolving";
  const nextAmount = Object.prototype.hasOwnProperty.call(updates, "amount")
    ? Number(updates.amount)
    : Number(existing.amount);
  if (!Number.isFinite(nextAmount) || nextAmount <= 0) {
    return next(new AppError("Updated amount must be greater than zero", 400));
  }
  if (!isContributionAmountValid(normalizedType, nextAmount)) {
    return next(
      new AppError(
        "Updated amount does not meet contribution requirements",
        400,
      ),
    );
  }

  let updatedContribution = null;
  let updatedTransaction = null;
  const executeUpdate = async (session = null) => {
    const contributionQuery = ContributionModel.findById(contributionId);
    if (session) contributionQuery.session(session);
    const contribution = await contributionQuery;
    if (!contribution) {
      throw new AppError("Contribution not found", 404);
    }

    const groupQuery = GroupModel.findById(contribution.groupId);
    if (session) groupQuery.session(session);
    const group = await groupQuery;
    if (!group) {
      throw new AppError("Group not found", 404);
    }

    const membershipQuery = GroupMembershipModel.findOne({
      userId: contribution.userId,
      groupId: contribution.groupId,
    });
    if (session) membershipQuery.session(session);
    const membership = await membershipQuery;
    if (!membership) {
      throw new AppError("Group membership not found", 404);
    }

    const previousAmount = Number(contribution.amount || 0);
    const delta = nextAmount - previousAmount;

    contribution.amount = nextAmount;
    if (Object.prototype.hasOwnProperty.call(updates, "month")) {
      contribution.month = updates.month;
    }
    if (Object.prototype.hasOwnProperty.call(updates, "year")) {
      contribution.year = updates.year;
    }
    if (Object.prototype.hasOwnProperty.call(updates, "paymentMethod")) {
      contribution.paymentMethod = updates.paymentMethod || null;
    }
    if (Object.prototype.hasOwnProperty.call(updates, "paymentReference")) {
      contribution.paymentReference = updates.paymentReference || null;
    }
    if (Object.prototype.hasOwnProperty.call(updates, "notes")) {
      contribution.notes = updates.notes || null;
    }
    if (!contribution.recurringPaymentId) {
      await attachRecurringContributionSchedule({
        contribution,
        userId: contribution.userId,
        groupId: contribution.groupId,
        contributionType: normalizedType,
        amount: nextAmount,
        session,
      });
    }
    applyContributionMetrics(contribution, nextAmount);
    await contribution.save({ session: session || undefined, validateBeforeSave: true });

    if (shouldCountTowardSavings(contribution.status) && delta !== 0) {
      await Promise.all([
        GroupModel.updateOne(
          { _id: group._id },
          { $inc: { totalSavings: delta } },
          session ? { session } : undefined,
        ),
        GroupMembershipModel.updateOne(
          { _id: membership._id },
          { $inc: { totalContributed: delta } },
          session ? { session } : undefined,
        ),
      ]);
    }

    const transactionQuery = TransactionModel.findOne({
      "metadata.contributionId": contribution._id,
    });
    if (session) transactionQuery.session(session);
    const transaction = await transactionQuery;

    const transactionDescription =
      contribution.notes ||
      transaction?.description ||
      `Group contribution - ${group.groupName}`;

    if (transaction) {
      transaction.amount = nextAmount;
      transaction.status = mapContributionStatusToTxStatus(contribution.status);
      transaction.channel = contribution.paymentMethod || null;
      transaction.description = transactionDescription;
      transaction.groupId = contribution.groupId;
      transaction.groupName = group.groupName;
      transaction.metadata = {
        ...(transaction.metadata || {}),
        contributionId: contribution._id,
        month: contribution.month,
        year: contribution.year,
        contributionType: normalizedType,
        recurringPaymentId: contribution.recurringPaymentId || null,
        manualPaymentReference: contribution.paymentReference || null,
        editedBy: req.user.profileId,
        editedAt: new Date(),
      };
      await transaction.save({ session: session || undefined, validateBeforeSave: true });
      updatedTransaction = transaction;
    } else {
      const createdTransaction = await TransactionModel.create(
        [
          {
            userId: contribution.userId,
            reference: generateReference("CRC-EDIT"),
            amount: nextAmount,
            type: "group_contribution",
            status: mapContributionStatusToTxStatus(contribution.status),
            description: transactionDescription,
            channel: contribution.paymentMethod || null,
            groupId: contribution.groupId,
            groupName: group.groupName,
            metadata: {
              paymentType: "group_contribution",
              contributionId: contribution._id,
              month: contribution.month,
              year: contribution.year,
              contributionType: normalizedType,
              recurringPaymentId: contribution.recurringPaymentId || null,
              manualPaymentReference: contribution.paymentReference || null,
              editedBy: req.user.profileId,
              editedAt: new Date(),
              createdByReconciliation: true,
            },
            gateway: "manual",
          },
        ],
        session ? { session } : undefined,
      );
      updatedTransaction = createdTransaction[0] || null;
    }

    updatedContribution = contribution;

    if (contribution.recurringPaymentId) {
      await rebuildRecurringContributionSchedule({
        recurringPaymentId: contribution.recurringPaymentId,
        session,
      });
    }
  };

  const session = await mongoose.startSession();
  try {
    await session.withTransaction(async () => {
      await executeUpdate(session);
    });
  } catch (error) {
    const message = String(error?.message || "");
    if (
      message.includes("Transaction numbers are only allowed") ||
      message.includes("Transaction support is not available")
    ) {
      await executeUpdate();
    } else {
      throw error;
    }
  } finally {
    await session.endSession();
  }

  return sendSuccess(res, {
    statusCode: 200,
    message: "Contribution updated successfully",
    data: {
      contribution: updatedContribution,
      transaction: updatedTransaction,
    },
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
  const recurringSchedule = await attachRecurringContributionSchedule({
    contribution,
    userId,
    groupId,
    contributionType: normalizedType,
    amount,
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
      recurringPaymentId: recurringSchedule?._id || null,
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
      contributionType: normalizedType,
      recurringPaymentId: contribution.recurringPaymentId || null,
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

export const markContributionUnpaid = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  if (!hasUserRole(req.user, "groupCoordinator")) {
    return next(
      new AppError(
        "Only group coordinators can mark contributions as unpaid",
        403,
      ),
    );
  }

  const userId = String(req.body?.userId || "").trim();
  const groupId = String(req.body?.groupId || "").trim();
  const month = Number(req.body?.month);
  const year = Number(req.body?.year);
  const contributionTypeRaw = req.body?.contributionType;

  if (!userId || !groupId) {
    return next(new AppError("userId and groupId are required", 400));
  }
  if (!Number.isFinite(month) || month < 1 || month > 12) {
    return next(new AppError("Invalid month", 400));
  }
  if (!Number.isFinite(year) || year < 2000 || year > 2100) {
    return next(new AppError("Invalid year", 400));
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

  const typeMatch = getContributionTypeMatch(normalizedType) || [normalizedType];
  const contributionTypeFilter =
    normalizedType === "revolving"
      ? {
          $or: [
            { contributionType: { $in: typeMatch } },
            { contributionType: { $exists: false } },
            { contributionType: null },
          ],
        }
      : { contributionType: { $in: typeMatch } };

  const executeDelete = async (session = null) => {
    const contributionQuery = ContributionModel.find({
      userId,
      groupId,
      month,
      year,
      ...contributionTypeFilter,
    });
    if (session) contributionQuery.session(session);
    const contributions = await contributionQuery;

    if (contributions.length === 0) {
      throw new AppError("No contribution entries found for this period", 404);
    }

    const countedAmount = contributions
      .filter((contribution) => shouldCountTowardSavings(contribution.status))
      .reduce((sum, contribution) => sum + Number(contribution.amount || 0), 0);

    const deletedContributionIds = contributions.map((contribution) => contribution._id);
    const recurringScheduleIds = await resolveContributionRecurringScheduleIds(
      contributions,
      session,
    );

    await ContributionModel.deleteMany(
      { _id: { $in: deletedContributionIds } },
      session ? { session } : undefined,
    );

    if (countedAmount > 0) {
      await Promise.all([
        GroupModel.updateOne(
          { _id: groupId },
          { $inc: { totalSavings: -countedAmount } },
          session ? { session } : undefined,
        ),
        GroupMembershipModel.updateOne(
          { groupId, userId },
          { $inc: { totalContributed: -countedAmount } },
          session ? { session } : undefined,
        ),
      ]);
    }

    const transactionStats = await reconcileContributionTransactionsAfterDeletion({
      deletedContributionIds,
      session,
    });

    for (const recurringPaymentId of recurringScheduleIds) {
      await rebuildRecurringContributionSchedule({
        recurringPaymentId,
        session,
      });
    }

    return {
      deletedContributions: contributions.length,
      deletedAmount: contributions.reduce(
        (sum, contribution) => sum + Number(contribution.amount || 0),
        0,
      ),
      countedAmount,
      deletedTransactions: transactionStats.deletedTransactions,
      updatedTransactions: transactionStats.updatedTransactions,
      recurringSchedulesRebuilt: recurringScheduleIds.size,
    };
  };

  let result = null;
  const session = await mongoose.startSession();
  try {
    await session.withTransaction(async () => {
      result = await executeDelete(session);
    });
  } catch (error) {
    const message = String(error?.message || "");
    if (
      message.includes("Transaction numbers are only allowed") ||
      message.includes("Transaction support is not available")
    ) {
      result = await executeDelete();
    } else {
      throw error;
    }
  } finally {
    await session.endSession();
  }

  return sendSuccess(res, {
    statusCode: 200,
    message: "Contribution period marked as unpaid",
    data: result,
  });
});
