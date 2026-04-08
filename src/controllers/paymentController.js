import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { ContributionModel } from "../models/Contribution.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { GroupModel } from "../models/Group.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import { LoanRepaymentScheduleItemModel } from "../models/LoanRepaymentScheduleItem.js";
import { TransactionModel } from "../models/Transaction.js";
import { RecurringPaymentModel } from "../models/RecurringPayment.js";

import {
  initializeTransaction as paystackInitializeTransaction,
  verifyTransaction as paystackVerifyTransaction,
  isValidWebhookSignature,
} from "../services/paystack.js";
import {
  calculateContributionUnits,
  getContributionTypeConfig,
  isContributionAmountValid,
  normalizeContributionType,
} from "../utils/contributionPolicy.js";

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

function buildPaystackMeta(payload = {}) {
  return {
    id: payload?.id,
    status: payload?.status,
    gateway_response: payload?.gateway_response,
    paid_at: payload?.paid_at,
    channel: payload?.channel,
    currency: payload?.currency,
  };
}

function parseDate(value, fallback = null) {
  if (!value) return fallback;
  const date = new Date(value);
  // eslint-disable-next-line no-restricted-globals
  if (isNaN(date.getTime())) return fallback;
  return date;
}

function applyContributionMetrics(contribution, amount) {
  if (!contribution) return;
  const safeAmount = Number(amount ?? contribution.amount ?? 0);
  contribution.units = calculateContributionUnits(safeAmount);
  contribution.interestAmount = 0;
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

function normalizeBulkItems(rawItems) {
  if (!Array.isArray(rawItems) || rawItems.length === 0) return [];
  return rawItems.map((item) => {
    const type = String(item?.type || item?.paymentType || "").trim();
    const amount = Number(item?.amount);
    const groupId = item?.groupId ? String(item.groupId) : null;
    const loanApplicationId = item?.loanApplicationId
      ? String(item.loanApplicationId)
      : item?.loanId
        ? String(item.loanId)
        : null;
    const dueDate = item?.dueDate ? String(item.dueDate) : null;
    const rawMonth = item?.month;
    const rawYear = item?.year;
    const month =
      rawMonth === null || typeof rawMonth === "undefined" || rawMonth === ""
        ? NaN
        : Number(rawMonth);
    const year =
      rawYear === null || typeof rawYear === "undefined" || rawYear === ""
        ? NaN
        : Number(rawYear);
    const description = item?.description ? String(item.description) : null;
    const contributionType = item?.contributionType
      ? String(item.contributionType)
      : null;
    return {
      type,
      amount,
      groupId,
      loanApplicationId,
      dueDate,
      month: Number.isFinite(month) ? month : null,
      year: Number.isFinite(year) ? year : null,
      description,
      contributionType,
    };
  });
}

function ensureSameType(items) {
  const type = items[0]?.type;
  if (!type) return null;
  const mismatch = items.some((item) => item.type !== type);
  if (mismatch) return null;
  return type;
}

async function ensureGroupLeader(groupId) {
  if (!groupId) return null;
  const group = await GroupModel.findById(groupId).lean();
  if (!group) return null;
  if (group.coordinatorId) return group.coordinatorId;
  const leader = await GroupMembershipModel.findOne(
    { groupId, role: "coordinator", status: "active" },
    { _id: 1 },
  ).lean();
  return leader?._id || null;
}

async function finalizeGroupContribution({ transaction, paystackData }) {
  const bulkIds = transaction?.metadata?.bulkContributionIds;
  if (Array.isArray(bulkIds) && bulkIds.length > 0) {
    const contributions = await ContributionModel.find({
      _id: { $in: bulkIds },
    });
    if (contributions.length === 0) return;

    const paidAt = parseDate(paystackData?.paid_at, new Date());
    const contributionsToUpdate = contributions.filter(
      (contribution) =>
        contribution.status !== "completed" &&
        contribution.status !== "verified",
    );

    const updates = contributionsToUpdate
      .map((contribution) => {
        contribution.status = "verified";
        contribution.verifiedAt = paidAt;
        contribution.paymentReference = transaction.reference;
        contribution.paymentMethod = "paystack";
        contribution.notes =
          contribution.notes || transaction.description || null;
        applyContributionMetrics(contribution, contribution.amount);
        return contribution.save({ validateBeforeSave: true });
      })
      .filter(Boolean);

    const groupTotals = new Map();
    contributionsToUpdate.forEach((contribution) => {
      const amount = Number(contribution.amount ?? 0);
      const groupId = String(contribution.groupId);
      groupTotals.set(groupId, (groupTotals.get(groupId) || 0) + amount);
    });

    const groupUpdates = Array.from(groupTotals.entries()).flatMap(
      ([groupId, amount]) => [
        GroupModel.findByIdAndUpdate(groupId, {
          $inc: { totalSavings: amount },
        }),
        GroupMembershipModel.findOneAndUpdate(
          { groupId, userId: transaction.userId },
          { $inc: { totalContributed: amount } },
        ),
      ],
    );

    await Promise.all([
      ...updates,
      ...groupUpdates,
      ...contributionsToUpdate.map((contribution) =>
        updateRecurringPaymentStats({
          userId: transaction.userId,
          paymentType: "group_contribution",
          groupId: contribution.groupId,
          amount: Number(contribution.amount ?? 0),
          count: 1,
          paidAt,
        }),
      ),
      TransactionModel.updateOne(
        { _id: transaction._id },
        {
          $set: {
            gateway: "paystack",
            channel: paystackData?.channel || transaction.channel || null,
            status: "success",
            metadata: {
              ...(transaction.metadata || {}),
              paystack: buildPaystackMeta(paystackData),
            },
          },
        },
      ),
    ]);
    return;
  }

  const contributionId = transaction?.metadata?.contributionId;
  if (!contributionId) return;

  const contribution = await ContributionModel.findById(contributionId);
  if (!contribution) return;

  if (
    contribution.status === "completed" ||
    contribution.status === "verified"
  ) {
    return;
  }

  const paidAt = parseDate(paystackData?.paid_at, new Date());
  contribution.status = "verified";
  contribution.verifiedAt = paidAt;
  contribution.paymentReference = transaction.reference;
  contribution.paymentMethod = "paystack";
  contribution.notes = contribution.notes || transaction.description || null;
  applyContributionMetrics(contribution, contribution.amount);
  await contribution.save({ validateBeforeSave: true });

  const amount = Number(transaction.amount ?? 0);
  await updateRecurringPaymentStats({
    userId: transaction.userId,
    paymentType: "group_contribution",
    groupId: contribution.groupId,
    amount,
    count: 1,
    paidAt,
  });
  await Promise.all([
    GroupModel.findByIdAndUpdate(contribution.groupId, {
      $inc: { totalSavings: amount },
    }),
    GroupMembershipModel.findOneAndUpdate(
      { groupId: contribution.groupId, userId: contribution.userId },
      { $inc: { totalContributed: amount } },
    ),
    TransactionModel.updateOne(
      { _id: transaction._id },
      {
        $set: {
          groupId: contribution.groupId,
          gateway: "paystack",
          channel: paystackData?.channel || transaction.channel || null,
          status: "success",
          metadata: {
            ...(transaction.metadata || {}),
            paystack: buildPaystackMeta(paystackData),
          },
        },
      },
    ),
  ]);
}

async function finalizeLoanRepayment({ transaction, paystackData }) {
  const bulkLoanItems = transaction?.metadata?.bulkLoanScheduleItemIds;
  if (Array.isArray(bulkLoanItems) && bulkLoanItems.length > 0) {
    const scheduleItems = await LoanRepaymentScheduleItemModel.find({
      _id: { $in: bulkLoanItems },
    }).sort({ installmentNumber: 1 });

    if (scheduleItems.length === 0) {
      throw new AppError("No repayment schedule items found", 400);
    }

    const loanApplicationId = scheduleItems[0].loanApplicationId;
    const application = await LoanApplicationModel.findById(loanApplicationId);
    if (!application) throw new AppError("Loan application not found", 404);

    if (!["disbursed", "defaulted"].includes(application.status)) {
      throw new AppError("Loan is not active", 400);
    }

    const total = scheduleItems.reduce(
      (sum, item) => sum + Number(item.totalAmount ?? 0),
      0,
    );
    const txAmount = Number(transaction.amount ?? 0);
    if (Math.round(total * 100) !== Math.round(txAmount * 100)) {
      throw new AppError("Bulk repayment amount mismatch", 400);
    }

    const paidAt = new Date(paystackData?.paid_at || Date.now());
    await Promise.all(
      scheduleItems.map((item) => {
        item.status = "paid";
        item.paidAt = paidAt;
        item.paidAmount = Number(item.totalAmount ?? 0);
        item.transactionId = transaction._id;
        item.reference = transaction.reference;
        return item.save();
      }),
    );

    await updateRecurringPaymentStats({
      userId: transaction.userId,
      paymentType: "loan_repayment",
      loanId: application._id,
      amount: total,
      count: scheduleItems.length,
      paidAt,
    });

    const remaining = Math.max(
      0,
      Number(application.remainingBalance || 0) - total,
    );
    application.remainingBalance = remaining;

    const hasMore = await LoanRepaymentScheduleItemModel.exists({
      loanApplicationId: application._id,
      status: { $in: ["pending", "upcoming", "overdue"] },
      _id: { $nin: scheduleItems.map((i) => i._id) },
    });

    if (!hasMore && remaining === 0) {
      application.status = "completed";
    } else {
      const lastPaid = scheduleItems[scheduleItems.length - 1];
      await LoanRepaymentScheduleItemModel.updateOne(
        {
          loanApplicationId: application._id,
          status: "upcoming",
          installmentNumber: { $gt: lastPaid.installmentNumber },
        },
        { $set: { status: "pending" } },
      );
    }

    await application.save();

    await TransactionModel.updateOne(
      { _id: transaction._id },
      {
        $set: {
          status: "success",
          gateway: "paystack",
          channel: paystackData?.channel || transaction.channel || null,
          loanId: application._id,
          loanName: application.loanCode || transaction.loanName || null,
          metadata: {
            ...(transaction.metadata || {}),
            paystack: buildPaystackMeta(paystackData),
          },
        },
      },
    );
    return;
  }

  const loanApplicationId =
    transaction?.loanId || transaction?.metadata?.loanApplicationId || null;
  if (!loanApplicationId) return;

  const application = await LoanApplicationModel.findById(loanApplicationId);
  if (!application) throw new AppError("Loan application not found", 404);

  if (!["disbursed", "defaulted"].includes(application.status)) {
    throw new AppError("Loan is not active", 400);
  }

  const nextItem = await LoanRepaymentScheduleItemModel.findOne({
    loanApplicationId: application._id,
    status: { $in: ["pending", "upcoming", "overdue"] },
  }).sort({ installmentNumber: 1 });

  if (!nextItem) throw new AppError("No pending repayments found", 400);

  const txAmount = Number(transaction.amount ?? 0);
  if (txAmount < Number(nextItem.totalAmount)) {
    throw new AppError("Repayment amount must cover the next installment", 400);
  }

  // Attach tx to schedule item and update application balance.
  nextItem.status = "paid";
  nextItem.paidAt = new Date(paystackData?.paid_at || Date.now());
  nextItem.paidAmount = Number(nextItem.totalAmount);
  nextItem.transactionId = transaction._id;
  nextItem.reference = transaction.reference;
  await nextItem.save();

  await updateRecurringPaymentStats({
    userId: transaction.userId,
    paymentType: "loan_repayment",
    loanId: application._id,
    amount: Number(nextItem.totalAmount),
    count: 1,
    paidAt: nextItem.paidAt,
  });

  const remaining = Math.max(
    0,
    Number(application.remainingBalance || 0) - Number(nextItem.totalAmount),
  );
  application.remainingBalance = remaining;

  const hasMore = await LoanRepaymentScheduleItemModel.exists({
    loanApplicationId: application._id,
    status: { $in: ["pending", "upcoming", "overdue"] },
    _id: { $ne: nextItem._id },
  });

  if (!hasMore && remaining === 0) {
    application.status = "completed";
  } else {
    await LoanRepaymentScheduleItemModel.updateOne(
      {
        loanApplicationId: application._id,
        status: "upcoming",
        installmentNumber: nextItem.installmentNumber + 1,
      },
      { $set: { status: "pending" } },
    );
  }

  await application.save();

  await TransactionModel.updateOne(
    { _id: transaction._id },
    {
      $set: {
        status: "success",
        gateway: "paystack",
        channel: paystackData?.channel || transaction.channel || null,
        loanId: application._id,
        loanName: application.loanCode || transaction.loanName || null,
        metadata: {
          ...(transaction.metadata || {}),
          installmentNumber: nextItem.installmentNumber,
          paystack: buildPaystackMeta(paystackData),
        },
      },
    },
  );
}

async function finalizeDeposit({ transaction, paystackData }) {
  await updateRecurringPaymentStats({
    userId: transaction.userId,
    paymentType: "deposit",
    amount: Number(transaction.amount ?? 0),
    count: 1,
    paidAt: paystackData?.paid_at || new Date(),
  });
  await TransactionModel.updateOne(
    { _id: transaction._id },
    {
      $set: {
        status: "success",
        gateway: "paystack",
        channel: paystackData?.channel || transaction.channel || null,
        metadata: {
          ...(transaction.metadata || {}),
          paystack: buildPaystackMeta(paystackData),
        },
      },
    },
  );
}

async function finalizeTransactionByReference(reference, { userId } = {}) {
  const filter = { reference };
  if (userId) filter.userId = userId;

  const tx = await TransactionModel.findOne(filter);
  if (!tx) throw new AppError("Transaction not found", 404);

  if (tx.status === "success") {
    return tx;
  }

  // paystackVerifyTransaction returns { status: true, message: string, data: {...} }
  const paystackRes = await paystackVerifyTransaction(reference);
  const payload = paystackRes?.data;

  if (!payload) throw new AppError("Invalid Paystack response", 502);

  if (payload.status !== "success") {
    tx.status = payload.status === "failed" ? "failed" : "pending";
    tx.gateway = "paystack";
    tx.channel = payload.channel || tx.channel || null;
    tx.metadata = { ...(tx.metadata || {}), paystack: payload };
    await tx.save();
    return tx;
  }

  // Amount returned by Paystack is in kobo.
  const paystackAmount = Number(payload.amount ?? 0) / 100;
  if (
    Math.round(paystackAmount * 100) !==
    Math.round(Number(tx.amount ?? 0) * 100)
  ) {
    throw new AppError("Payment amount mismatch", 400);
  }

  if (tx.type === "deposit") {
    await finalizeDeposit({ transaction: tx, paystackData: payload });
  } else if (tx.type === "group_contribution") {
    await finalizeGroupContribution({ transaction: tx, paystackData: payload });
  } else if (tx.type === "loan_repayment") {
    await finalizeLoanRepayment({ transaction: tx, paystackData: payload });
  }

  const updated = await TransactionModel.findById(tx._id);
  return updated || tx;
}

export const initializePaystackPayment = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const {
    amount,
    email,
    paymentType,
    groupId,
    loanApplicationId,
    contributionType,
    month,
    year,
    description,
    callbackUrl,
  } = req.body || {};

  const parsedAmount = Number(amount);
  if (!parsedAmount || parsedAmount <= 0) {
    return next(new AppError("amount is required", 400));
  }
  if (!email) return next(new AppError("email is required", 400));
  if (!paymentType) return next(new AppError("paymentType is required", 400));

  if (paymentType === "deposit") {
    return next(
      new AppError("Savings deposits are temporarily suspended", 400),
    );
  }

  const allowed = ["loan_repayment", "group_contribution"];
  if (!allowed.includes(paymentType)) {
    return next(
      new AppError(`Invalid paymentType. Allowed: ${allowed.join(", ")}`, 400),
    );
  }

  const reference = generateReference("CRC");
  const userId = req.user.profileId;

  const metadata = {
    paymentType,
    groupId: groupId || null,
    loanApplicationId: loanApplicationId || null,
    description: description || null,
  };

  // Pre-create internal records to ensure webhook/verify is idempotent.
  let contributionId = null;
  let groupName = null;
  let loanName = null;

  if (paymentType === "group_contribution") {
    if (!groupId) return next(new AppError("groupId is required", 400));

    const membership = await GroupMembershipModel.findOne({
      groupId,
      userId,
      status: "active",
    }).populate("groupId");
    if (!membership)
      return next(new AppError("User is not an active group member", 400));
    groupName = membership.groupId?.groupName || null;

    const leader = await ensureGroupLeader(groupId);
    if (!leader) {
      return next(
        new AppError("This group does not have an assigned group leader", 400),
      );
    }

    const now = new Date();
    const parsedMonth =
      month === null || typeof month === "undefined" || month === ""
        ? NaN
        : Number(month);
    const parsedYear =
      year === null || typeof year === "undefined" || year === ""
        ? NaN
        : Number(year);
    const contributionMonth = Number.isFinite(parsedMonth)
      ? parsedMonth
      : now.getMonth() + 1;
    const contributionYear = Number.isFinite(parsedYear)
      ? parsedYear
      : now.getFullYear();

    if (contributionMonth < 1 || contributionMonth > 12) {
      return next(new AppError("Invalid contribution month", 400));
    }
    if (contributionYear < 2000 || contributionYear > 2100) {
      return next(new AppError("Invalid contribution year", 400));
    }

    const canonicalType =
      normalizeContributionType(contributionType) || "revolving";

    if (!isContributionAmountValid(canonicalType, parsedAmount)) {
      const cfg = getContributionTypeConfig(canonicalType);
      const minLabel = cfg?.minAmount
        ? `NGN ${Number(cfg.minAmount).toLocaleString()}`
        : "the minimum amount";
      const unitStep = cfg?.stepAmount || cfg?.unitAmount;
      const unitLabel = unitStep
        ? ` in multiples of NGN ${Number(unitStep).toLocaleString()}`
        : "";
      return next(
        new AppError(
          `Amount must be at least ${minLabel}${unitLabel} for ${cfg?.label || "this contribution type"}`,
          400,
        ),
      );
    }

    const contribution = await ContributionModel.create({
      groupId,
      userId,
      month: contributionMonth,
      year: contributionYear,
      amount: parsedAmount,
      contributionType: canonicalType,
      status: "pending",
      paymentReference: reference,
      paymentMethod: "paystack",
      notes: description || null,
    });
    contributionId = contribution._id;

    metadata.contributionId = contribution._id;
    metadata.month = contributionMonth;
    metadata.year = contributionYear;
    metadata.contributionType = canonicalType;
    metadata.groupName = groupName;
  }

  if (paymentType === "loan_repayment") {
    if (!loanApplicationId)
      return next(new AppError("loanApplicationId is required", 400));
    const loan = await LoanApplicationModel.findById(loanApplicationId);
    if (!loan) return next(new AppError("Loan application not found", 404));

    const nextItem = await LoanRepaymentScheduleItemModel.findOne({
      loanApplicationId: loan._id,
      status: { $in: ["pending", "upcoming", "overdue"] },
    }).sort({ installmentNumber: 1 });

    if (!nextItem)
      return next(new AppError("No pending repayments found", 400));

    // For now, enforce exact installment amount to avoid reconciliation issues.
    if (Number(parsedAmount) !== Number(nextItem.totalAmount)) {
      return next(
        new AppError(
          `Amount must equal the next installment (₦${Number(nextItem.totalAmount).toLocaleString()})`,
          400,
        ),
      );
    }

    loanName = loan.loanCode || null;
  }

  await TransactionModel.create({
    userId,
    reference,
    amount: parsedAmount,
    type: paymentType,
    status: "pending",
    description: String(description || "").trim() || `${paymentType} payment`,
    channel: null,
    groupId: groupId || null,
    groupName,
    loanId: loanApplicationId || null,
    loanName,
    metadata,
    gateway: "paystack",
  });

  let initRes;
  try {
    initRes = await paystackInitializeTransaction({
      email,
      amount: Math.round(parsedAmount * 100),
      reference,
      callback_url:
        callbackUrl || process.env.PAYSTACK_CALLBACK_URL || undefined,
      metadata,
    });
  } catch (err) {
    await Promise.all([
      TransactionModel.deleteOne({ reference, userId }),
      contributionId
        ? ContributionModel.deleteOne({ _id: contributionId, userId })
        : Promise.resolve(),
    ]);
    throw err;
  }

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      reference,
      authorizationUrl: initRes?.data?.authorization_url,
      accessCode: initRes?.data?.access_code,
      contributionId,
    },
  });
});

export const initializePaystackBulkPayment = catchAsync(
  async (req, res, next) => {
    if (!req.user) return next(new AppError("Not authenticated", 401));
    if (!req.user.profileId)
      return next(new AppError("User profile not found", 400));

    const email = req.body?.email;
    const callbackUrl = req.body?.callbackUrl;
    const description = req.body?.description
      ? String(req.body.description)
      : null;

    const items = normalizeBulkItems(req.body?.items);
    if (items.length === 0) {
      return next(new AppError("Bulk items are required", 400));
    }
    if (items.length > 25) {
      return next(new AppError("Too many bulk items. Maximum is 25.", 400));
    }

    const paymentType = ensureSameType(items);
    if (!paymentType) {
      return next(
        new AppError("Bulk items must share the same payment type", 400),
      );
    }

    const allowed = ["loan_repayment", "group_contribution"];
    if (!allowed.includes(paymentType)) {
      return next(
        new AppError(
          `Invalid paymentType for bulk. Allowed: ${allowed.join(", ")}`,
          400,
        ),
      );
    }

    if (!email) return next(new AppError("email is required", 400));

    const invalidItem = items.find(
      (item) => !Number.isFinite(item.amount) || item.amount <= 0,
    );
    if (invalidItem) {
      return next(
        new AppError("Each bulk item must include a valid amount", 400),
      );
    }

    const userId = req.user.profileId;
    const reference = generateReference("CRC-BULK");

    let contributionIds = [];
    let bulkLoanScheduleItemIds = [];
    let groupName = null;
    let groupId = null;
    let loanName = null;
    let loanId = null;
    let totalAmount = 0;

    if (paymentType === "group_contribution") {
      const groupNames = new Map();
      const typeSet = new Set();

      for (const item of items) {
        if (!item.groupId) {
          return next(
            new AppError(
              "groupId is required for bulk group contributions",
              400,
            ),
          );
        }
        const dueDate = parseDate(item.dueDate, null);
        const parsedItemMonth =
          item.month === null || typeof item.month === "undefined" || item.month === ""
            ? NaN
            : Number(item.month);
        const parsedItemYear =
          item.year === null || typeof item.year === "undefined" || item.year === ""
            ? NaN
            : Number(item.year);

        const month = Number.isFinite(parsedItemMonth)
          ? parsedItemMonth
          : dueDate
            ? dueDate.getMonth() + 1
            : null;
        const year = Number.isFinite(parsedItemYear)
          ? parsedItemYear
          : dueDate
            ? dueDate.getFullYear()
            : null;

        if (!month || month < 1 || month > 12) {
          return next(new AppError("Invalid month for bulk item", 400));
        }
        if (!year || year < 2000 || year > 2100) {
          return next(new AppError("Invalid year for bulk item", 400));
        }

        const membership = await GroupMembershipModel.findOne({
          groupId: item.groupId,
          userId,
          status: "active",
        }).populate("groupId");
        if (!membership) {
          return next(new AppError("User is not an active group member", 400));
        }

        const leader = await ensureGroupLeader(item.groupId);
        if (!leader) {
          return next(
            new AppError(
              "This group does not have an assigned group leader",
              400,
            ),
          );
        }

        const canonicalType =
          normalizeContributionType(item.contributionType) || "revolving";
        typeSet.add(canonicalType);
        if (typeSet.size > 1) {
          return next(
            new AppError(
              "Bulk contributions must use a single contribution type",
              400,
            ),
          );
        }

        if (!isContributionAmountValid(canonicalType, item.amount)) {
          const cfg = getContributionTypeConfig(canonicalType);
          const minLabel = cfg?.minAmount
            ? `NGN ${Number(cfg.minAmount).toLocaleString()}`
            : "the minimum amount";
          const unitStep = cfg?.stepAmount || cfg?.unitAmount;
          const unitLabel = unitStep
            ? ` in multiples of NGN ${Number(unitStep).toLocaleString()}`
            : "";
          return next(
            new AppError(
              `Amount must be at least ${minLabel}${unitLabel} for ${cfg?.label || "this contribution type"}`,
              400,
            ),
          );
        }

        const contribution = await ContributionModel.create({
          groupId: item.groupId,
          userId,
          month,
          year,
          amount: Number(item.amount || 0),
          contributionType: canonicalType,
          status: "pending",
          paymentReference: reference,
          paymentMethod: "paystack",
          notes: description || item.description || null,
        });
        contributionIds.push(contribution._id);
        groupNames.set(
          String(item.groupId),
          membership.groupId?.groupName || null,
        );
        totalAmount += Number(item.amount || 0);
      }

      const uniqueGroupIds = Array.from(groupNames.keys());
      if (uniqueGroupIds.length === 1) {
        groupId = uniqueGroupIds[0];
        groupName = groupNames.get(uniqueGroupIds[0]) || null;
      }
    }

    if (paymentType === "loan_repayment") {
      loanId = items[0]?.loanApplicationId;
      if (!loanId) {
        return next(
          new AppError(
            "loanApplicationId is required for bulk loan repayment",
            400,
          ),
        );
      }
      const mismatch = items.some((item) => item.loanApplicationId !== loanId);
      if (mismatch) {
        return next(
          new AppError("Bulk loan repayments must target a single loan", 400),
        );
      }

      const application = await LoanApplicationModel.findById(loanId);
      if (!application)
        return next(new AppError("Loan application not found", 404));
      if (!["disbursed", "defaulted"].includes(application.status)) {
        return next(new AppError("Loan is not active", 400));
      }

      const scheduleItems = await LoanRepaymentScheduleItemModel.find({
        loanApplicationId: loanId,
        status: { $in: ["pending", "upcoming", "overdue"] },
      })
        .sort({ installmentNumber: 1 })
        .limit(items.length);

      if (scheduleItems.length < items.length) {
        return next(
          new AppError(
            "Not enough pending installments for bulk repayment",
            400,
          ),
        );
      }

      const expectedTotal = scheduleItems.reduce(
        (sum, item) => sum + Number(item.totalAmount || 0),
        0,
      );
      const providedTotal = items.reduce(
        (sum, item) => sum + Number(item.amount || 0),
        0,
      );

      if (Math.round(expectedTotal * 100) !== Math.round(providedTotal * 100)) {
        return next(
          new AppError(
            "Bulk repayment amount does not match scheduled installments",
            400,
          ),
        );
      }

      bulkLoanScheduleItemIds = scheduleItems.map((item) => item._id);
      totalAmount = expectedTotal;
      loanName = application.loanCode || null;
    }

    if (!totalAmount || totalAmount <= 0) {
      return next(new AppError("Bulk amount must be greater than zero", 400));
    }

    const metadata = {
      paymentType,
      bulk: true,
      bulkItems: items,
      bulkContributionIds: contributionIds,
      bulkLoanScheduleItemIds,
    };

    await TransactionModel.create({
      userId,
      reference,
      amount: totalAmount,
      type: paymentType,
      status: "pending",
      description:
        description || `Bulk ${paymentType.replace("_", " ")} payment`,
      channel: null,
      groupId: groupId || null,
      groupName,
      loanId: loanId || null,
      loanName,
      metadata,
      gateway: "paystack",
    });

    let initRes;
    try {
      initRes = await paystackInitializeTransaction({
        email,
        amount: Math.round(totalAmount * 100),
        reference,
        callback_url:
          callbackUrl || process.env.PAYSTACK_CALLBACK_URL || undefined,
        metadata,
      });
    } catch (err) {
      await Promise.all([
        TransactionModel.deleteOne({ reference, userId }),
        contributionIds.length > 0
          ? ContributionModel.deleteMany({
              _id: { $in: contributionIds },
              userId,
            })
          : Promise.resolve(),
      ]);
      throw err;
    }

    return sendSuccess(res, {
      statusCode: 200,
      data: {
        reference,
        authorizationUrl: initRes?.data?.authorization_url,
        accessCode: initRes?.data?.access_code,
      },
    });
  },
);

export const verifyPaystackPayment = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));
  const reference = String(
    req.body?.reference || req.query?.reference || "",
  ).trim();
  if (!reference) return next(new AppError("reference is required", 400));

  const tx = await finalizeTransactionByReference(reference, {
    userId: req.user.profileId,
  });

  return sendSuccess(res, { statusCode: 200, data: { transaction: tx } });
});

export const paystackWebhook = catchAsync(async (req, res, next) => {
  const signature = req.get("x-paystack-signature");
  const rawBody = req.rawBody;

  if (!isValidWebhookSignature(rawBody, signature)) {
    return next(new AppError("Invalid webhook signature", 400));
  }

  const event = req.body;
  const eventType = String(event?.event || "");
  const data = event?.data || null;

  if (!data?.reference) {
    return sendSuccess(res, { statusCode: 200, data: { ok: true } });
  }

  // Only finalize on successful charge events; other events are ignored for now.
  if (eventType === "charge.success") {
    try {
      await finalizeTransactionByReference(String(data.reference));
    } catch {
      // Webhooks must respond 200 quickly; treat finalize as best-effort and idempotent.
    }
  }

  return sendSuccess(res, { statusCode: 200, data: { ok: true } });
});
