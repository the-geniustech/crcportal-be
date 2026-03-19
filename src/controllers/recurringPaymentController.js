import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import {
  RecurringPaymentModel,
  RecurringPaymentTypes,
  RecurringFrequencies,
} from "../models/RecurringPayment.js";
import { GroupModel } from "../models/Group.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";

function pick(obj, allowedKeys) {
  const out = {};
  for (const key of allowedKeys) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) out[key] = obj[key];
  }
  return out;
}

function addFrequency(date, frequency) {
  const d = new Date(date);
  if (frequency === "weekly") d.setDate(d.getDate() + 7);
  else if (frequency === "bi-weekly") d.setDate(d.getDate() + 14);
  else d.setMonth(d.getMonth() + 1);
  return d;
}

function computeNextPaymentDate(startDate, frequency) {
  const now = new Date();
  let nextDate = new Date(startDate);
  // eslint-disable-next-line no-restricted-globals
  if (isNaN(nextDate.getTime())) return now;
  while (nextDate <= now) {
    nextDate = addFrequency(nextDate, frequency);
  }
  return nextDate;
}

export const listMyRecurringPayments = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const payments = await RecurringPaymentModel.find({ userId: req.user.profileId }).sort({ createdAt: -1 });
  return sendSuccess(res, { statusCode: 200, results: payments.length, data: { payments } });
});

export const createRecurringPayment = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const payload = pick(req.body || {}, [
    "paymentType",
    "amount",
    "frequency",
    "startDate",
    "endDate",
    "groupId",
    "loanId",
    "description",
    "isActive",
  ]);

  const paymentType = String(payload.paymentType || "").trim();
  if (!RecurringPaymentTypes.includes(paymentType)) {
    return next(new AppError(`Invalid paymentType`, 400));
  }

  const frequency = String(payload.frequency || "").trim();
  if (!RecurringFrequencies.includes(frequency)) {
    return next(new AppError(`Invalid frequency`, 400));
  }

  const amount = Number(payload.amount);
  if (!amount || amount <= 0) return next(new AppError("amount is required", 400));

  const startDate = payload.startDate ? new Date(payload.startDate) : new Date();
  // eslint-disable-next-line no-restricted-globals
  if (isNaN(startDate.getTime())) return next(new AppError("Invalid startDate", 400));

  const nextPaymentDate = computeNextPaymentDate(startDate, frequency);

  let groupId = payload.groupId || null;
  let loanId = payload.loanId || null;
  let groupName = null;
  let loanName = null;

  if (paymentType === "group_contribution") {
    if (!groupId) return next(new AppError("groupId is required", 400));
    const group = await GroupModel.findById(groupId);
    if (!group) return next(new AppError("Group not found", 404));
    groupName = group.groupName;
    loanId = null;
  }

  if (paymentType === "loan_repayment") {
    if (!loanId) return next(new AppError("loanId is required", 400));
    const loan = await LoanApplicationModel.findById(loanId);
    if (!loan) return next(new AppError("Loan not found", 404));
    loanName = loan.loanCode || loan.loanPurpose;
    groupId = null;
  }

  if (paymentType === "deposit") {
    groupId = null;
    loanId = null;
  }

  const payment = await RecurringPaymentModel.create({
    userId: req.user.profileId,
    paymentType,
    amount,
    frequency,
    startDate,
    nextPaymentDate,
    endDate: payload.endDate ? new Date(payload.endDate) : null,
    groupId,
    groupName,
    loanId,
    loanName,
    description: payload.description ? String(payload.description).trim() : null,
    isActive: typeof payload.isActive === "boolean" ? payload.isActive : true,
  });

  return sendSuccess(res, { statusCode: 201, data: { payment } });
});

export const updateRecurringPayment = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const id = req.params.id;
  const allowed = [
    "paymentType",
    "amount",
    "frequency",
    "startDate",
    "nextPaymentDate",
    "endDate",
    "groupId",
    "loanId",
    "description",
    "isActive",
  ];
  const updates = pick(req.body || {}, allowed);

  const payment = await RecurringPaymentModel.findOne({ _id: id, userId: req.user.profileId });
  if (!payment) return next(new AppError("Recurring payment not found", 404));

  if (typeof updates.paymentType !== "undefined") {
    const paymentType = String(updates.paymentType || "").trim();
    if (!RecurringPaymentTypes.includes(paymentType)) {
      return next(new AppError("Invalid paymentType", 400));
    }
    payment.paymentType = paymentType;
  }

  if (typeof updates.frequency !== "undefined") {
    const frequency = String(updates.frequency || "").trim();
    if (!RecurringFrequencies.includes(frequency)) {
      return next(new AppError("Invalid frequency", 400));
    }
    payment.frequency = frequency;
  }

  if (typeof updates.amount !== "undefined") {
    const amount = Number(updates.amount);
    if (!amount || amount <= 0) return next(new AppError("Invalid amount", 400));
    payment.amount = amount;
  }

  if (typeof updates.startDate !== "undefined") {
    const startDate = new Date(updates.startDate);
    // eslint-disable-next-line no-restricted-globals
    if (isNaN(startDate.getTime())) return next(new AppError("Invalid startDate", 400));
    payment.startDate = startDate;
  }

  if (Object.prototype.hasOwnProperty.call(updates, "endDate")) {
    if (updates.endDate) {
      const endDate = new Date(updates.endDate);
      // eslint-disable-next-line no-restricted-globals
      if (isNaN(endDate.getTime())) return next(new AppError("Invalid endDate", 400));
      payment.endDate = endDate;
    } else {
      payment.endDate = null;
    }
  }

  if (Object.prototype.hasOwnProperty.call(updates, "description")) {
    payment.description = updates.description ? String(updates.description).trim() : null;
  }

  if (typeof updates.isActive !== "undefined") {
    payment.isActive = Boolean(updates.isActive);
  }

  if (Object.prototype.hasOwnProperty.call(updates, "nextPaymentDate")) {
    if (updates.nextPaymentDate) {
      const nextPaymentDate = new Date(updates.nextPaymentDate);
      // eslint-disable-next-line no-restricted-globals
      if (isNaN(nextPaymentDate.getTime())) {
        return next(new AppError("Invalid nextPaymentDate", 400));
      }
      payment.nextPaymentDate = nextPaymentDate;
    } else if (payment.startDate && payment.frequency) {
      payment.nextPaymentDate = computeNextPaymentDate(payment.startDate, payment.frequency);
    }
  }

  const nextPaymentType = payment.paymentType;
  const nextGroupId = Object.prototype.hasOwnProperty.call(updates, "groupId")
    ? updates.groupId
    : payment.groupId;
  const nextLoanId = Object.prototype.hasOwnProperty.call(updates, "loanId")
    ? updates.loanId
    : payment.loanId;

  if (nextPaymentType === "group_contribution") {
    if (!nextGroupId) return next(new AppError("groupId is required", 400));
    const group = await GroupModel.findById(nextGroupId);
    if (!group) return next(new AppError("Group not found", 404));
    payment.groupId = nextGroupId;
    payment.groupName = group.groupName;
    payment.loanId = null;
    payment.loanName = null;
  }

  if (nextPaymentType === "loan_repayment") {
    if (!nextLoanId) return next(new AppError("loanId is required", 400));
    const loan = await LoanApplicationModel.findById(nextLoanId);
    if (!loan) return next(new AppError("Loan not found", 404));
    payment.loanId = nextLoanId;
    payment.loanName = loan.loanCode || loan.loanPurpose;
    payment.groupId = null;
    payment.groupName = null;
  }

  if (nextPaymentType === "deposit") {
    payment.groupId = null;
    payment.groupName = null;
    payment.loanId = null;
    payment.loanName = null;
  }

  if (
    !Object.prototype.hasOwnProperty.call(updates, "nextPaymentDate") &&
    (Object.prototype.hasOwnProperty.call(updates, "startDate") ||
      Object.prototype.hasOwnProperty.call(updates, "frequency"))
  ) {
    payment.nextPaymentDate = computeNextPaymentDate(payment.startDate, payment.frequency);
  }

  await payment.save();
  return sendSuccess(res, { statusCode: 200, data: { payment } });
});

export const deleteRecurringPayment = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const id = req.params.id;
  const payment = await RecurringPaymentModel.findOneAndDelete({ _id: id, userId: req.user.profileId });
  if (!payment) return next(new AppError("Recurring payment not found", 404));

  return sendSuccess(res, { statusCode: 200, message: "Recurring payment deleted" });
});
