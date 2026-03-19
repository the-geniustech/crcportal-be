import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import { randomId } from "../utils/crypto.js";
import { TransactionModel } from "../models/Transaction.js";
import { computeSavingsBalances, sumDepositsForMonth, sumInterestAllTime } from "../utils/finance.js";

function getDefaultAnnualInterestRatePct() {
  const raw = process.env.SAVINGS_INTEREST_RATE_ANNUAL_PCT;
  const n = raw == null ? 0 : Number(raw);
  if (!Number.isFinite(n) || n < 0) return 0;
  return n;
}

function getInterestReference(profileId, year, month1to12) {
  return `INT-${String(profileId)}-${String(year)}-${String(month1to12).padStart(2, "0")}`;
}

export const getMySavingsSummary = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const now = new Date();
  const year = now.getUTCFullYear();
  const month = now.getUTCMonth() + 1;

  const [balances, monthlyDeposits, interestEarned] = await Promise.all([
    computeSavingsBalances(req.user.profileId),
    sumDepositsForMonth(req.user.profileId, year, month),
    sumInterestAllTime(req.user.profileId),
  ]);

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      ...balances,
      monthlyDeposits,
      interestEarned,
      annualInterestRatePct: getDefaultAnnualInterestRatePct(),
    },
  });
});

export const createDeposit = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const amount = Number(req.body?.amount);
  if (!amount || amount <= 0) return next(new AppError("amount is required", 400));

  const reference = String(req.body?.reference || `DEP-${randomId(8)}`).trim();
  const channel = req.body?.channel ? String(req.body.channel).trim() : null;
  const description = req.body?.description ? String(req.body.description).trim() : "Savings deposit";
  const gateway = req.body?.gateway ? String(req.body.gateway).trim() : "paystack";

  const tx = await TransactionModel.create({
    userId: req.user.profileId,
    reference,
    amount,
    type: "deposit",
    status: "pending",
    description,
    channel,
    gateway,
    metadata: req.body?.metadata ?? null,
  });

  return sendSuccess(res, {
    statusCode: 201,
    data: { transaction: tx },
  });
});

export const confirmDeposit = catchAsync(async (req, res, next) => {
  const reference = String(req.body?.reference || req.params?.reference || "").trim();
  if (!reference) return next(new AppError("reference is required", 400));

  const status = String(req.body?.status || "success").toLowerCase();
  if (!["success", "failed"].includes(status)) {
    return next(new AppError("Invalid status", 400));
  }

  const tx = await TransactionModel.findOne({ reference });
  if (!tx) return next(new AppError("Transaction not found", 404));
  if (tx.type !== "deposit") return next(new AppError("Not a deposit transaction", 400));

  if (tx.status !== "success") {
    tx.status = status;
    tx.channel = req.body?.channel ? String(req.body.channel).trim() : tx.channel;
    tx.gateway = req.body?.gateway ? String(req.body.gateway).trim() : tx.gateway;
    tx.metadata = req.body?.metadata ?? tx.metadata;
    await tx.save();
  }

  return sendSuccess(res, { statusCode: 200, data: { transaction: tx } });
});

export const verifyMyDeposit = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const reference = String(req.body?.reference || req.params?.reference || "").trim();
  if (!reference) return next(new AppError("reference is required", 400));

  const tx = await TransactionModel.findOne({
    reference,
    userId: req.user.profileId,
    type: "deposit",
  });

  if (!tx) return next(new AppError("Transaction not found", 404));

  if (tx.status === "pending") {
    tx.status = "success";
    await tx.save();
  }

  return sendSuccess(res, { statusCode: 200, data: { transaction: tx } });
});

export const applyMonthlyInterest = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const profileId = req.user.profileId;

  const now = new Date();
  const year = now.getUTCFullYear();
  const month = now.getUTCMonth() + 1;
  const reference = getInterestReference(profileId, year, month);

  const annualRate = getDefaultAnnualInterestRatePct();
  const { ledgerBalance } = await computeSavingsBalances(profileId);
  const monthlyRate = annualRate / 100 / 12;
  const interestAmount = Math.floor(ledgerBalance * monthlyRate);

  if (interestAmount <= 0) {
    return sendSuccess(res, { statusCode: 200, data: { applied: false, interestAmount: 0 } });
  }

  const existing = await TransactionModel.findOne({ reference });
  if (existing) {
    return sendSuccess(res, { statusCode: 200, data: { applied: false, transaction: existing } });
  }

  const tx = await TransactionModel.create({
    userId: profileId,
    reference,
    amount: interestAmount,
    type: "interest",
    status: "success",
    description: `Monthly savings interest (${year}-${String(month).padStart(2, "0")})`,
    channel: "system",
    gateway: "internal",
    metadata: { year, month, annualRatePct: annualRate },
  });

  return sendSuccess(res, { statusCode: 201, data: { applied: true, transaction: tx } });
});
