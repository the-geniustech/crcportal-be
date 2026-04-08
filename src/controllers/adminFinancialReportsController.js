import mongoose from "mongoose";

import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { ContributionModel } from "../models/Contribution.js";
import { ProfileModel } from "../models/Profile.js";
import { TransactionModel } from "../models/Transaction.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import {
  getContributionTypeMatch,
  resolveExpectedContributionAmount,
} from "../utils/contributionPolicy.js";
import { hasUserRole } from "../utils/roles.js";
import {
  computeAggregateInterestSchedule,
  getMonthlyInterestRates,
  resolveMonthsToCompute,
} from "../utils/contributionInterest.js";

function clamp(n, min, max) {
  return Math.min(max, Math.max(min, n));
}

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

function monthStartUtc(year, month1to12) {
  return new Date(Date.UTC(year, month1to12 - 1, 1, 0, 0, 0, 0));
}

function monthEndUtc(year, month1to12) {
  return new Date(Date.UTC(year, month1to12, 0, 23, 59, 59, 999));
}

function shiftMonthUtc({ year, month }, deltaMonths) {
  const d = new Date(Date.UTC(year, month - 1 + deltaMonths, 1, 0, 0, 0, 0));
  return { year: d.getUTCFullYear(), month: d.getUTCMonth() + 1 };
}

function parseEndMonthYear(req) {
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

function parsePeriodMonths(req) {
  const raw = String(req.query?.period ?? "6months").trim().toLowerCase();
  if (raw === "3months") return 3;
  if (raw === "6months") return 6;
  if (raw === "12months") return 12;
  return 6;
}

function monthShort(month1to12) {
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return months[month1to12 - 1] ?? String(month1to12);
}

function pctChange(cur, prev) {
  const c = Number(cur || 0);
  const p = Number(prev || 0);
  if (!Number.isFinite(c) || !Number.isFinite(p)) return 0;
  if (p === 0) return c === 0 ? 0 : 100;
  return ((c - p) / p) * 100;
}

const PaidContributionStatuses = ["completed", "verified"];

function contributionDateExpr() {
  return { $ifNull: ["$verifiedAt", "$createdAt"] };
}

function buildPeriodMonthFilters(months) {
  if (!Array.isArray(months) || months.length === 0) return [];
  return months.map((m) => ({
    year: Number(m.year),
    month: Number(m.month),
  }));
}

async function getContributionTotalsByMonth({ year, groupObjectIds }) {
  const match = {
    year,
    status: { $in: PaidContributionStatuses },
    groupId: { $ne: null },
    ...(groupObjectIds ? { groupId: { $in: groupObjectIds } } : {}),
  };

  const rows = await ContributionModel.aggregate([
    { $match: match },
    { $group: { _id: "$month", total: { $sum: "$amount" } } },
  ]);

  const totals = Array(12).fill(0);
  rows.forEach((row) => {
    const month = Number(row._id);
    if (!Number.isFinite(month) || month < 1 || month > 12) return;
    totals[month - 1] = Number(row.total || 0);
  });

  return totals;
}

export const getAdminFinancialReports = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const now = new Date();
  const end = parseEndMonthYear(req);
  if (end.error) return next(new AppError(end.error, 400));

  const periodMonths = parsePeriodMonths(req);

  const endMonth = { year: end.year, month: end.month };
  const startMonth = shiftMonthUtc(endMonth, -(periodMonths - 1));

  const months = Array.from({ length: periodMonths }, (_v, i) =>
    shiftMonthUtc(startMonth, i),
  );
  const periodMonthFilters = buildPeriodMonthFilters(months);
  const periodMonthMatch =
    periodMonthFilters.length > 0 ? { $or: periodMonthFilters } : {};
  const ytdMonthMatch = { year: end.year, month: { $gte: 1, $lte: end.month } };
  const ytdMonthsCount = Math.max(0, Math.min(12, end.month));

  const rangeStart = monthStartUtc(startMonth.year, startMonth.month);
  const rangeEnd = monthEndUtc(endMonth.year, endMonth.month);

  const prevEndMonth = shiftMonthUtc(startMonth, -1);
  const prevStartMonth = shiftMonthUtc(startMonth, -periodMonths);

  const prevStart = monthStartUtc(prevStartMonth.year, prevStartMonth.month);
  const prevEnd = monthEndUtc(prevEndMonth.year, prevEndMonth.month);

  const manageableGroupIds = await getManageableGroupIds(req);
  const groupScopeIds = manageableGroupIds ?? [];
  const groupObjectIds =
    manageableGroupIds === null
      ? null
      : groupScopeIds
          .filter((id) => mongoose.Types.ObjectId.isValid(id))
          .map((id) => new mongoose.Types.ObjectId(id));

  const contributionRangeMatch = {
    status: { $in: PaidContributionStatuses },
    groupId: { $ne: null },
    ...(groupObjectIds ? { groupId: { $in: groupObjectIds } } : {}),
  };

  const repaymentRangeMatch = {
    status: "success",
    type: "loan_repayment",
    groupId: { $ne: null },
    date: { $gte: rangeStart, $lte: rangeEnd },
    ...(groupObjectIds ? { groupId: { $in: groupObjectIds } } : {}),
  };

  const loanRangeMatch = {
    groupId: { $ne: null },
    disbursedAt: { $gte: rangeStart, $lte: rangeEnd },
    status: { $in: ["disbursed", "completed", "defaulted"] },
    ...(groupObjectIds ? { groupId: { $in: groupObjectIds } } : {}),
  };

  const [contributionMonthly, repaymentMonthly, loanMonthly] = await Promise.all([
    ContributionModel.aggregate([
      { $match: contributionRangeMatch },
      { $addFields: { effectiveDate: contributionDateExpr() } },
      { $match: { effectiveDate: { $gte: rangeStart, $lte: rangeEnd } } },
      {
        $group: {
          _id: { y: { $year: "$effectiveDate" }, m: { $month: "$effectiveDate" } },
          contributions: { $sum: "$amount" },
        },
      },
    ]),
    TransactionModel.aggregate([
      { $match: repaymentRangeMatch },
      {
        $group: {
          _id: { y: { $year: "$date" }, m: { $month: "$date" } },
          repayments: { $sum: "$amount" },
        },
      },
    ]),
    LoanApplicationModel.aggregate([
      { $match: loanRangeMatch },
      {
        $project: {
          disbursedAt: 1,
          amount: { $ifNull: ["$approvedAmount", "$loanAmount"] },
        },
      },
      {
        $group: {
          _id: { y: { $year: "$disbursedAt" }, m: { $month: "$disbursedAt" } },
          loans: { $sum: "$amount" },
        },
      },
    ]),
  ]);

  const contributionByYm = new Map(
    contributionMonthly.map((r) => [`${r._id.y}-${r._id.m}`, r]),
  );
  const repaymentByYm = new Map(
    repaymentMonthly.map((r) => [`${r._id.y}-${r._id.m}`, r]),
  );
  const loanByYm = new Map(
    loanMonthly.map((r) => [`${r._id.y}-${r._id.m}`, r]),
  );

  const yearsInPeriod = Array.from(new Set(months.map((m) => m.year)));
  const interestByYm = new Map();
  for (const yr of yearsInPeriod) {
    const monthlyTotals = await getContributionTotalsByMonth({
      year: yr,
      groupObjectIds,
    });
    const monthlyRates = await getMonthlyInterestRates(yr);
    const monthsToCompute = resolveMonthsToCompute({ year: yr, now });
    const { schedule } = computeAggregateInterestSchedule({
      monthlyContributions: monthlyTotals,
      monthlyRates,
      monthsToCompute,
    });
    schedule.forEach((row) => {
      interestByYm.set(`${yr}-${row.month}`, Number(row.interest || 0));
    });
  }

  const monthlyData = months.map((m) => {
    const k = `${m.year}-${m.month}`;
    const contrib = contributionByYm.get(k);
    const repayment = repaymentByYm.get(k);
    const loans = loanByYm.get(k);
    const interest = interestByYm.get(k);

    return {
      month: monthShort(m.month),
      contributions: Number(contrib?.contributions || 0),
      loans: Number(loans?.loans || 0),
      repayments: Number(repayment?.repayments || 0),
      interest: Number(interest || 0),
    };
  });

  const scopeGroupFilter = groupObjectIds ? { _id: { $in: groupObjectIds } } : {};
  const groups = await GroupModel.find(scopeGroupFilter, {
    groupName: 1,
    monthlyContribution: 1,
  })
    .sort({ groupNumber: 1 })
    .lean();

  const groupIds = groups.map((g) => String(g._id));
  const groupIdsObj = groupIds
    .filter((id) => mongoose.Types.ObjectId.isValid(id))
    .map((id) => new mongoose.Types.ObjectId(id));

  if (groups.length === 0) {
    return sendSuccess(res, {
      statusCode: 200,
      data: {
        monthlyData,
        groupPerformance: [],
        summary: {
          contributionsChangePct: 0,
          loansChangePct: 0,
          repaymentRatePct: 0,
          interestRatePct: 0,
        },
        period: { months: periodMonths, end: endMonth },
      },
    });
  }

  const regularTypeMatch = getContributionTypeMatch("revolving") || ["revolving"];

  const activeMemberships = await GroupMembershipModel.find(
    { groupId: { $in: groupIdsObj }, status: "active" },
    { groupId: 1, userId: 1 },
  ).lean();

  const profileIds = Array.from(
    new Set(
      activeMemberships
        .map((m) => String(m.userId))
        .filter((id) => mongoose.Types.ObjectId.isValid(id)),
    ),
  ).map((id) => new mongoose.Types.ObjectId(id));

  const [profiles, contribByGroupAgg, activeLoansAgg, collectedRegularAgg] =
    await Promise.all([
      profileIds.length
        ? ProfileModel.find(
            { _id: { $in: profileIds } },
            { contributionSettings: 1 },
          ).lean()
        : Promise.resolve([]),
      ContributionModel.aggregate([
        {
          $match: {
            status: { $in: PaidContributionStatuses },
            groupId: { $in: groupIdsObj },
            ...ytdMonthMatch,
          },
        },
        { $group: { _id: "$groupId", total: { $sum: "$amount" } } },
      ]),
      LoanApplicationModel.aggregate([
        {
          $match: {
            groupId: { $in: groupIdsObj },
            status: { $in: ["disbursed", "defaulted"] },
            remainingBalance: { $gt: 0 },
          },
        },
        { $group: { _id: "$groupId", count: { $sum: 1 } } },
      ]),
      ContributionModel.aggregate([
        {
          $match: {
            status: { $in: PaidContributionStatuses },
            groupId: { $in: groupIdsObj },
            contributionType: { $in: regularTypeMatch },
            ...ytdMonthMatch,
          },
        },
        { $group: { _id: "$groupId", total: { $sum: "$amount" } } },
      ]),
    ]);

  const settingsByProfileId = new Map(
    profiles.map((p) => [String(p._id), p?.contributionSettings ?? null]),
  );

  const groupById = new Map(groups.map((g) => [String(g._id), g]));
  const memberCountByGroup = new Map();
  const expectedMonthlyByGroup = new Map();

  for (const membership of activeMemberships) {
    const gid = String(membership.groupId);
    const uid = String(membership.userId);
    memberCountByGroup.set(gid, (memberCountByGroup.get(gid) ?? 0) + 1);

    const group = groupById.get(gid);
    if (!group) continue;
    const settings = settingsByProfileId.get(uid) ?? null;

    const expectedMonthly = resolveExpectedContributionAmount({
      settings,
      year: end.year,
      groupMonthlyContribution: group.monthlyContribution,
      type: "revolving",
    });
    expectedMonthlyByGroup.set(
      gid,
      (expectedMonthlyByGroup.get(gid) ?? 0) + Number(expectedMonthly || 0),
    );
  }

  const expectedTotalByGroup = new Map();
  for (const group of groups) {
    const gid = String(group._id);
    const expectedMonthly = expectedMonthlyByGroup.get(gid) ?? 0;
    const expectedTotal = expectedMonthly * ytdMonthsCount;
    expectedTotalByGroup.set(gid, expectedTotal);
  }

  const contribByGroup = new Map(
    contribByGroupAgg.map((r) => [String(r._id), Number(r.total || 0)]),
  );
  const activeLoansByGroup = new Map(
    activeLoansAgg.map((r) => [String(r._id), Number(r.count || 0)]),
  );
  const collectedRegularByGroup = new Map(
    collectedRegularAgg.map((r) => [String(r._id), Number(r.total || 0)]),
  );

  const groupPerformance = groups.map((g) => {
    const gid = String(g._id);
    const memberCount = memberCountByGroup.get(gid) ?? 0;
    const expectedTotal = expectedTotalByGroup.get(gid) ?? 0;
    const collectedTotal = collectedRegularByGroup.get(gid) ?? 0;
    const rate = expectedTotal > 0 ? (collectedTotal / expectedTotal) * 100 : 0;
    const gap = expectedTotal - collectedTotal;

    return {
      groupName: g.groupName ?? "Group",
      totalContributions: contribByGroup.get(gid) ?? 0,
      activeLoans: activeLoansByGroup.get(gid) ?? 0,
      collectionRate: Math.round(clamp(rate, 0, 100)),
      memberCount,
      expectedTotal,
      collectedTotal,
      collectionGap: gap > 0 ? gap : 0,
    };
  });

  const totals = monthlyData.reduce(
    (acc, m) => {
      acc.contributions += Number(m.contributions || 0);
      acc.loans += Number(m.loans || 0);
      acc.repayments += Number(m.repayments || 0);
      acc.interest += Number(m.interest || 0);
      return acc;
    },
    { contributions: 0, loans: 0, repayments: 0, interest: 0 },
  );

  const contributionPrevMatch = {
    status: { $in: PaidContributionStatuses },
    groupId: { $ne: null },
    ...(groupObjectIds ? { groupId: { $in: groupObjectIds } } : {}),
  };
  const loanPrevMatch = {
    ...loanRangeMatch,
    disbursedAt: { $gte: prevStart, $lte: prevEnd },
  };

  const [prevContrib, prevLoans] = await Promise.all([
    ContributionModel.aggregate([
      { $match: contributionPrevMatch },
      { $addFields: { effectiveDate: contributionDateExpr() } },
      { $match: { effectiveDate: { $gte: prevStart, $lte: prevEnd } } },
      { $group: { _id: null, total: { $sum: "$amount" } } },
    ]),
    LoanApplicationModel.aggregate([
      { $match: loanPrevMatch },
      {
        $project: {
          amount: { $ifNull: ["$approvedAmount", "$loanAmount"] },
        },
      },
      { $group: { _id: null, total: { $sum: "$amount" } } },
    ]),
  ]);

  const prevContribTotal = Number(prevContrib?.[0]?.total || 0);
  const prevLoanTotal = Number(prevLoans?.[0]?.total || 0);

  const repaymentRatePct = totals.loans > 0 ? (totals.repayments / totals.loans) * 100 : 0;
  const interestRatePct = totals.contributions > 0 ? (totals.interest / totals.contributions) * 100 : 0;

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      monthlyData,
      groupPerformance,
      summary: {
        contributionsChangePct: pctChange(totals.contributions, prevContribTotal),
        loansChangePct: pctChange(totals.loans, prevLoanTotal),
        repaymentRatePct: clamp(repaymentRatePct, 0, 999),
        interestRatePct: clamp(interestRatePct, 0, 999),
      },
      period: { months: periodMonths, end: endMonth },
    },
  });
});
