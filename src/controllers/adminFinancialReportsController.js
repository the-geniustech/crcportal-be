import mongoose from "mongoose";

import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { TransactionModel } from "../models/Transaction.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";

function clamp(n, min, max) {
  return Math.min(max, Math.max(min, n));
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

export const getAdminFinancialReports = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const end = parseEndMonthYear(req);
  if (end.error) return next(new AppError(end.error, 400));

  const periodMonths = parsePeriodMonths(req);

  const endMonth = { year: end.year, month: end.month };
  const startMonth = shiftMonthUtc(endMonth, -(periodMonths - 1));

  const months = Array.from({ length: periodMonths }, (_v, i) =>
    shiftMonthUtc(startMonth, i),
  );

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

  const txBaseMatch = {
    status: "success",
    groupId: { $ne: null },
    type: { $in: ["group_contribution", "loan_repayment", "interest"] },
  };

  const txRangeMatch = {
    ...txBaseMatch,
    date: { $gte: rangeStart, $lte: rangeEnd },
    ...(groupObjectIds ? { groupId: { $in: groupObjectIds } } : {}),
  };

  const loanRangeMatch = {
    groupId: { $ne: null },
    disbursedAt: { $gte: rangeStart, $lte: rangeEnd },
    status: { $in: ["disbursed", "completed", "defaulted"] },
    ...(groupObjectIds ? { groupId: { $in: groupObjectIds } } : {}),
  };

  const [txMonthly, loanMonthly] = await Promise.all([
    TransactionModel.aggregate([
      { $match: txRangeMatch },
      {
        $group: {
          _id: { y: { $year: "$date" }, m: { $month: "$date" } },
          contributions: {
            $sum: {
              $cond: [{ $eq: ["$type", "group_contribution"] }, "$amount", 0],
            },
          },
          repayments: {
            $sum: {
              $cond: [{ $eq: ["$type", "loan_repayment"] }, "$amount", 0],
            },
          },
          interest: {
            $sum: {
              $cond: [{ $eq: ["$type", "interest"] }, "$amount", 0],
            },
          },
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

  const txByYm = new Map(
    txMonthly.map((r) => [`${r._id.y}-${r._id.m}`, r]),
  );
  const loanByYm = new Map(
    loanMonthly.map((r) => [`${r._id.y}-${r._id.m}`, r]),
  );

  const monthlyData = months.map((m) => {
    const k = `${m.year}-${m.month}`;
    const tx = txByYm.get(k);
    const loans = loanByYm.get(k);

    return {
      month: monthShort(m.month),
      contributions: Number(tx?.contributions || 0),
      loans: Number(loans?.loans || 0),
      repayments: Number(tx?.repayments || 0),
      interest: Number(tx?.interest || 0),
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

  const endMonthStart = monthStartUtc(endMonth.year, endMonth.month);
  const endMonthEnd = monthEndUtc(endMonth.year, endMonth.month);

  const [activeMembersAgg, contribByGroupAgg, activeLoansAgg, collectedRegularAgg] = await Promise.all([
    GroupMembershipModel.aggregate([
      { $match: { groupId: { $in: groupIdsObj }, status: "active" } },
      { $group: { _id: "$groupId", count: { $sum: 1 } } },
    ]),
    TransactionModel.aggregate([
      {
        $match: {
          status: "success",
          type: "group_contribution",
          groupId: { $in: groupIdsObj },
          date: { $gte: rangeStart, $lte: rangeEnd },
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
    TransactionModel.aggregate([
      {
        $match: {
          status: "success",
          type: "group_contribution",
          groupId: { $in: groupIdsObj },
          date: { $gte: endMonthStart, $lte: endMonthEnd },
          "metadata.contributionType": "regular",
        },
      },
      { $group: { _id: "$groupId", total: { $sum: "$amount" } } },
    ]),
  ]);

  const activeMembersByGroup = new Map(
    activeMembersAgg.map((r) => [String(r._id), Number(r.count || 0)]),
  );
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
    const memberCount = activeMembersByGroup.get(gid) ?? 0;
    const expected = Number(g.monthlyContribution || 0) * Math.max(0, memberCount);
    const collectedRegular = collectedRegularByGroup.get(gid) ?? 0;
    const rate = expected > 0 ? (collectedRegular / expected) * 100 : 0;

    return {
      groupName: g.groupName ?? "Group",
      totalContributions: contribByGroup.get(gid) ?? 0,
      activeLoans: activeLoansByGroup.get(gid) ?? 0,
      collectionRate: Math.round(clamp(rate, 0, 100)),
      memberCount,
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

  const txPrevMatch = {
    ...txBaseMatch,
    date: { $gte: prevStart, $lte: prevEnd },
    ...(groupObjectIds ? { groupId: { $in: groupObjectIds } } : {}),
  };
  const loanPrevMatch = {
    ...loanRangeMatch,
    disbursedAt: { $gte: prevStart, $lte: prevEnd },
  };

  const [prevContrib, prevLoans] = await Promise.all([
    TransactionModel.aggregate([
      { $match: { ...txPrevMatch, type: "group_contribution" } },
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
  const interestRatePct = totals.repayments > 0 ? (totals.interest / totals.repayments) * 100 : 0;

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

