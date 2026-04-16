import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import { RecurringPaymentModel } from "../models/RecurringPayment.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import { LoanRepaymentScheduleItemModel } from "../models/LoanRepaymentScheduleItem.js";
import { getLoanScheduleOutstandingAmount } from "../services/loanRepaymentService.js";

const DAY_MS = 24 * 60 * 60 * 1000;

function clampNumber(value, { min, max, fallback }) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, num));
}

function computeDaysUntil(dueDate, now) {
  const diffMs = dueDate.getTime() - now.getTime();
  const raw = diffMs / DAY_MS;
  if (raw >= 0) return Math.ceil(raw);
  return Math.floor(raw);
}

function buildTitle(type, { loanName, groupName }) {
  if (type === "loan_repayment") {
    return loanName ? `${loanName} Payment Due` : "Loan Repayment Due";
  }
  if (type === "group_contribution") {
    return groupName ? `${groupName} Contribution Due` : "Group Contribution Due";
  }
  return "Payment Due";
}

function nextMonthlyDue(startDate, now) {
  const due = new Date(startDate);
  if (Number.isNaN(due.getTime())) return null;
  while (due.getTime() < now.getTime()) {
    due.setMonth(due.getMonth() + 1);
  }
  return due;
}

function buildReminder({ id, type, amount, dueDate, groupId, loanId, groupName, loanName }) {
  const now = new Date();
  const daysUntilDue = computeDaysUntil(dueDate, now);
  const isOverdue = dueDate.getTime() < now.getTime();
  return {
    id: String(id),
    type,
    title: buildTitle(type, { loanName, groupName }),
    amount: Number(amount || 0),
    dueDate: dueDate.toISOString(),
    groupId: groupId ? String(groupId) : null,
    groupName: groupName || null,
    loanId: loanId ? String(loanId) : null,
    loanName: loanName || null,
    isOverdue,
    daysUntilDue,
  };
}

export const listMyPaymentReminders = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const windowDays = clampNumber(req.query?.windowDays, {
    min: 7,
    max: 365,
    fallback: 30,
  });

  const now = new Date();
  const windowEnd = new Date(now.getTime() + windowDays * DAY_MS);

  const recurringPayments = await RecurringPaymentModel.find({
    userId: req.user.profileId,
    isActive: true,
  }).lean();

  const reminders = [];
  const recurringLoanIds = new Set();

  for (const payment of recurringPayments) {
    if (!payment.paymentType || payment.paymentType === "deposit") continue;

    const dueDate = payment.nextPaymentDate || payment.startDate;
    if (!dueDate) continue;

    const dueTime = new Date(dueDate);
    if (Number.isNaN(dueTime.getTime())) continue;

    if (dueTime > windowEnd && dueTime >= now) continue;

    if (payment.paymentType === "loan_repayment" && payment.loanId) {
      recurringLoanIds.add(String(payment.loanId));
    }

    reminders.push(
      buildReminder({
        id: payment._id,
        type: payment.paymentType,
        amount: payment.amount,
        dueDate: dueTime,
        groupId: payment.groupId,
        loanId: payment.loanId,
        groupName: payment.groupName,
        loanName: payment.loanName,
      }),
    );
  }

  const loans = await LoanApplicationModel.find({
    userId: req.user.profileId,
    status: { $in: ["disbursed", "defaulted"] },
    remainingBalance: { $gt: 0 },
    repaymentStartDate: { $ne: null },
  }).lean();

  const nextScheduleItems =
    loans.length > 0
      ? await LoanRepaymentScheduleItemModel.find({
          loanApplicationId: { $in: loans.map((loan) => loan._id) },
          status: { $in: ["pending", "upcoming", "overdue"] },
        })
          .sort({ dueDate: 1, installmentNumber: 1 })
          .lean()
      : [];
  const nextScheduleByLoanId = new Map();
  for (const item of nextScheduleItems) {
    const loanId = String(item.loanApplicationId);
    if (!nextScheduleByLoanId.has(loanId)) {
      nextScheduleByLoanId.set(loanId, item);
    }
  }

  for (const loan of loans) {
    if (recurringLoanIds.has(String(loan._id))) continue;
    const nextSchedule = nextScheduleByLoanId.get(String(loan._id));
    const dueDate = nextSchedule?.dueDate
      ? new Date(nextSchedule.dueDate)
      : nextMonthlyDue(loan.repaymentStartDate, now);
    if (!dueDate || Number.isNaN(dueDate.getTime())) continue;
    if (dueDate > windowEnd) continue;

    reminders.push(
      buildReminder({
        id: loan._id,
        type: "loan_repayment",
        amount: nextSchedule
          ? getLoanScheduleOutstandingAmount(nextSchedule)
          : Number(loan.monthlyPayment || 0),
        dueDate,
        loanId: loan._id,
        groupName: loan.groupName,
        loanName: loan.loanCode || loan.loanPurpose,
      }),
    );
  }

  reminders.sort((a, b) => {
    if (a.isOverdue && !b.isOverdue) return -1;
    if (!a.isOverdue && b.isOverdue) return 1;
    return a.daysUntilDue - b.daysUntilDue;
  });

  const summary = reminders.reduce(
    (acc, reminder) => {
      acc.totalDue += reminder.amount;
      if (reminder.isOverdue) acc.overdueCount += 1;
      if (!reminder.isOverdue && reminder.daysUntilDue <= 7) acc.upcomingCount += 1;
      return acc;
    },
    { overdueCount: 0, upcomingCount: 0, totalDue: 0 },
  );

  return sendSuccess(res, {
    statusCode: 200,
    results: reminders.length,
    data: { reminders, summary },
  });
});
