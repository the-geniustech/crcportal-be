import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import { ContributionModel } from "../models/Contribution.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import { LoanRepaymentScheduleItemModel } from "../models/LoanRepaymentScheduleItem.js";

const PaidContributionStatuses = ["completed", "verified"];

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
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

function monthShort(month1to12) {
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return months[month1to12 - 1] ?? String(month1to12);
}

function contributionDateExpr() {
  return { $ifNull: ["$verifiedAt", "$createdAt"] };
}

export const getDashboardSummary = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const profileId = req.user.profileId;

  const contributionAgg = await ContributionModel.aggregate([
    {
      $match: {
        userId: profileId,
        status: { $in: ["completed", "verified"] },
      },
    },
    { $group: { _id: null, total: { $sum: "$amount" } } },
  ]);

  const totalContributions = Number(contributionAgg?.[0]?.total ?? 0);

  const activeLoans = await LoanApplicationModel.find({
    userId: profileId,
    status: { $in: ["disbursed", "defaulted"] },
  })
    .select("_id remainingBalance loanCode groupName")
    .lean();

  const activeLoanOutstanding = activeLoans.reduce(
    (sum, loan) => sum + Number(loan.remainingBalance ?? 0),
    0,
  );

  const activeLoanIds = activeLoans.map((loan) => loan._id);
  let nextPayment = null;

  if (activeLoanIds.length > 0) {
    const scheduleItem = await LoanRepaymentScheduleItemModel.findOne({
      loanApplicationId: { $in: activeLoanIds },
      status: { $in: ["pending", "upcoming", "overdue"] },
    })
      .sort({ dueDate: 1 })
      .lean();

    if (scheduleItem) {
      const loanMeta = activeLoans.find(
        (loan) => String(loan._id) === String(scheduleItem.loanApplicationId),
      );
      nextPayment = {
        loanId: scheduleItem.loanApplicationId,
        loanCode: loanMeta?.loanCode ?? null,
        groupName: loanMeta?.groupName ?? null,
        dueDate: scheduleItem.dueDate,
        amount: scheduleItem.totalAmount,
        status: scheduleItem.status,
      };
    }
  }

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      totalContributions,
      activeLoanOutstanding,
      nextPayment,
    },
  });
});

export const getContributionTrend = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const rawMonths = Number(req.query?.months ?? 6);
  const monthsCount = clamp(
    Number.isFinite(rawMonths) ? rawMonths : 6,
    3,
    24,
  );

  const now = new Date();
  const endMonth = { year: now.getUTCFullYear(), month: now.getUTCMonth() + 1 };
  const startMonth = shiftMonthUtc(endMonth, -(monthsCount - 1));

  const months = Array.from({ length: monthsCount }, (_v, i) =>
    shiftMonthUtc(startMonth, i),
  );

  const rangeStart = monthStartUtc(startMonth.year, startMonth.month);
  const rangeEnd = monthEndUtc(endMonth.year, endMonth.month);

  const trendAgg = await ContributionModel.aggregate([
    {
      $match: {
        userId: req.user.profileId,
        status: { $in: PaidContributionStatuses },
      },
    },
    { $addFields: { effectiveDate: contributionDateExpr() } },
    { $match: { effectiveDate: { $gte: rangeStart, $lte: rangeEnd } } },
    {
      $group: {
        _id: { y: { $year: "$effectiveDate" }, m: { $month: "$effectiveDate" } },
        total: { $sum: "$amount" },
      },
    },
  ]);

  const byYm = new Map(
    trendAgg.map((row) => [`${row._id.y}-${row._id.m}`, row]),
  );

  const trend = months.map((m) => {
    const key = `${m.year}-${m.month}`;
    const row = byYm.get(key);
    return {
      year: m.year,
      month: m.month,
      label: monthShort(m.month),
      amount: Number(row?.total ?? 0),
    };
  });

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      trend,
      period: { months: monthsCount, end: endMonth },
    },
  });
});
