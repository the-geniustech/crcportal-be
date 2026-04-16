import AppError from "../utils/AppError.js";

import { LoanRepaymentScheduleItemModel } from "../models/LoanRepaymentScheduleItem.js";
import { TransactionModel } from "../models/Transaction.js";
import { RecurringPaymentModel } from "../models/RecurringPayment.js";

function roundCurrency(value) {
  return Math.round((Number(value || 0) + Number.EPSILON) * 100) / 100;
}

function parseDate(value, fallback = null) {
  if (!value) return fallback;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return fallback;
  return date;
}

function addFrequency(date, frequency) {
  const next = new Date(date);
  if (frequency === "weekly") next.setDate(next.getDate() + 7);
  else if (frequency === "bi-weekly") next.setDate(next.getDate() + 14);
  else next.setMonth(next.getMonth() + 1);
  return next;
}

export function getLoanScheduleOutstandingAmount(item) {
  const total = roundCurrency(item?.totalAmount ?? 0);
  const paid = roundCurrency(item?.paidAmount ?? 0);
  return Math.max(0, roundCurrency(total - paid));
}

function resolveOpenScheduleStatus(item, now, hasEarlierOutstanding) {
  const dueAmount = getLoanScheduleOutstandingAmount(item);
  if (dueAmount <= 0) return "paid";

  const dueDate = parseDate(item?.dueDate, null);
  if (dueDate && dueDate.getTime() < now.getTime()) {
    return "overdue";
  }

  return hasEarlierOutstanding ? "upcoming" : "pending";
}

async function syncLoanRecurringPaymentStats({
  userId,
  loanId,
  amount,
  paidAt,
  settledInstallmentCount,
} = {}) {
  if (!userId || !loanId) return;

  const target = await RecurringPaymentModel.findOne({
    userId,
    paymentType: "loan_repayment",
    loanId,
    isActive: true,
  }).sort({ nextPaymentDate: 1, createdAt: 1 });

  if (!target) return;

  const paidAtDate = parseDate(paidAt, new Date()) || new Date();
  target.totalPaymentsMade = Number(target.totalPaymentsMade ?? 0) + 1;
  target.totalAmountPaid =
    roundCurrency(target.totalAmountPaid ?? 0) + roundCurrency(amount ?? 0);
  target.lastPaymentDate = paidAtDate;
  target.lastPaymentStatus = "success";

  const completedCount = Math.max(0, Number(settledInstallmentCount || 0));
  if (completedCount > 0) {
    let nextDate =
      parseDate(target.nextPaymentDate, null) ||
      parseDate(target.startDate, null) ||
      paidAtDate;

    for (let i = 0; i < completedCount; i += 1) {
      nextDate = addFrequency(nextDate, target.frequency);
    }

    while (nextDate <= paidAtDate) {
      nextDate = addFrequency(nextDate, target.frequency);
    }

    target.nextPaymentDate = nextDate;
  }

  await target.save();
}

export async function buildLoanNextPaymentMap(applications) {
  if (!Array.isArray(applications) || applications.length === 0) {
    return new Map();
  }

  const scheduleItems = await LoanRepaymentScheduleItemModel.find({
    loanApplicationId: { $in: applications.map((app) => app._id) },
    status: { $in: ["pending", "upcoming", "overdue"] },
  })
    .sort({ dueDate: 1, installmentNumber: 1 })
    .lean();

  const map = new Map();
  const now = new Date();

  for (const item of scheduleItems) {
    const loanId = String(item.loanApplicationId);
    if (map.has(loanId)) continue;

    const amountDue = getLoanScheduleOutstandingAmount(item);
    if (amountDue <= 0) continue;

    const dueDate = parseDate(item.dueDate, null);
    map.set(loanId, {
      ...item,
      amountDue,
      status:
        dueDate && dueDate.getTime() < now.getTime() ? "overdue" : "pending",
    });
  }

  return map;
}

export async function applyLoanRepayment({
  application,
  amount,
  reference,
  channel = null,
  description = null,
  gateway = "manual",
  metadata = {},
  paidAt = new Date(),
  transaction = null,
} = {}) {
  if (!application?._id) throw new AppError("Loan application not found", 404);

  if (!["disbursed", "defaulted"].includes(String(application.status))) {
    throw new AppError("Loan is not active", 400);
  }

  const repaymentAmount = roundCurrency(amount);
  if (!Number.isFinite(repaymentAmount) || repaymentAmount <= 0) {
    throw new AppError("amount must be greater than 0", 400);
  }

  const currentRemaining = roundCurrency(application.remainingBalance ?? 0);
  if (repaymentAmount > currentRemaining) {
    throw new AppError("Repayment amount cannot exceed the remaining balance", 400);
  }

  const scheduleItems = await LoanRepaymentScheduleItemModel.find({
    loanApplicationId: application._id,
    status: { $in: ["pending", "upcoming", "overdue"] },
  }).sort({ installmentNumber: 1 });

  if (scheduleItems.length === 0) {
    throw new AppError("No pending repayments found", 400);
  }

  const scheduleOutstanding = roundCurrency(
    scheduleItems.reduce(
      (sum, item) => sum + getLoanScheduleOutstandingAmount(item),
      0,
    ),
  );

  if (repaymentAmount > scheduleOutstanding) {
    throw new AppError("Repayment amount exceeds outstanding schedule balance", 400);
  }

  const paidAtDate = parseDate(paidAt, new Date()) || new Date();
  const tx =
    transaction ||
    (await TransactionModel.create({
      userId: application.userId,
      reference,
      amount: repaymentAmount,
      type: "loan_repayment",
      status: "success",
      description:
        String(description || "").trim() ||
        `Loan repayment for ${application.loanCode || "loan"}`,
      channel,
      groupId: application.groupId || null,
      groupName: application.groupName || null,
      loanId: application._id,
      loanName: application.loanCode || null,
      metadata: metadata || null,
      gateway,
    }));

  let remainingToApply = repaymentAmount;
  let settledInstallmentCount = 0;
  const allocations = [];

  for (const item of scheduleItems) {
    const dueAmount = getLoanScheduleOutstandingAmount(item);
    if (dueAmount <= 0 || remainingToApply <= 0) continue;

    const appliedAmount = roundCurrency(Math.min(dueAmount, remainingToApply));
    const nextPaidAmount = roundCurrency(
      Number(item.paidAmount ?? 0) + appliedAmount,
    );

    item.paidAmount = nextPaidAmount;
    remainingToApply = roundCurrency(remainingToApply - appliedAmount);

    allocations.push({
      scheduleItemId: item._id,
      installmentNumber: item.installmentNumber,
      dueDate: item.dueDate,
      appliedAmount,
      remainingInstallmentBalance: roundCurrency(
        Number(item.totalAmount ?? 0) - nextPaidAmount,
      ),
    });

    if (getLoanScheduleOutstandingAmount(item) <= 0) {
      item.paidAmount = roundCurrency(item.totalAmount ?? item.paidAmount ?? 0);
      item.status = "paid";
      item.paidAt = paidAtDate;
      item.transactionId = tx._id;
      item.reference = tx.reference;
      settledInstallmentCount += 1;
    } else {
      item.paidAt = null;
      item.transactionId = null;
      item.reference = null;
    }
  }

  if (remainingToApply > 0) {
    throw new AppError("Unable to fully apply repayment amount", 400);
  }

  let hasEarlierOutstanding = false;
  for (const item of scheduleItems) {
    const dueAmount = getLoanScheduleOutstandingAmount(item);
    if (dueAmount <= 0) {
      item.status = "paid";
      item.paidAmount = roundCurrency(item.totalAmount ?? item.paidAmount ?? 0);
      continue;
    }

    item.status = resolveOpenScheduleStatus(item, paidAtDate, hasEarlierOutstanding);
    hasEarlierOutstanding = true;
  }

  await Promise.all(scheduleItems.map((item) => item.save()));

  const updatedRemaining = Math.max(
    0,
    roundCurrency(scheduleOutstanding - repaymentAmount),
  );
  application.remainingBalance = updatedRemaining;
  if (updatedRemaining === 0) {
    application.status = "completed";
  }
  await application.save();

  tx.status = "success";
  tx.gateway = gateway || tx.gateway || "manual";
  tx.channel = channel || tx.channel || null;
  tx.groupId = application.groupId || tx.groupId || null;
  tx.groupName = application.groupName || tx.groupName || null;
  tx.loanId = application._id;
  tx.loanName = application.loanCode || tx.loanName || null;
  tx.metadata = {
    ...(tx.metadata || {}),
    ...(metadata || {}),
    allocations: allocations.map((allocation) => ({
      ...allocation,
      scheduleItemId: String(allocation.scheduleItemId),
    })),
    settledInstallmentCount,
    remainingBalanceAfterPayment: updatedRemaining,
    paidAt: paidAtDate.toISOString(),
  };
  await tx.save();

  await syncLoanRecurringPaymentStats({
    userId: application.userId,
    loanId: application._id,
    amount: repaymentAmount,
    paidAt: paidAtDate,
    settledInstallmentCount,
  });

  const nextPaymentItem = scheduleItems.find(
    (item) => getLoanScheduleOutstandingAmount(item) > 0,
  );

  return {
    application,
    transaction: tx,
    allocations,
    settledInstallmentCount,
    nextPayment: nextPaymentItem
      ? {
          scheduleItemId: nextPaymentItem._id,
          installmentNumber: nextPaymentItem.installmentNumber,
          dueDate: nextPaymentItem.dueDate,
          status: nextPaymentItem.status,
          amountDue: getLoanScheduleOutstandingAmount(nextPaymentItem),
        }
      : null,
    scheduleItems,
  };
}
