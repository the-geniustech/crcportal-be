import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import { BankAccountModel } from "../models/BankAccount.js";

function pick(obj, allowedKeys) {
  const out = {};
  for (const key of allowedKeys) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) out[key] = obj[key];
  }
  return out;
}

export const listMyBankAccounts = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const accounts = await BankAccountModel.find({ userId: req.user.profileId }).sort({ isPrimary: -1, createdAt: -1 });
  return sendSuccess(res, { statusCode: 200, results: accounts.length, data: { accounts } });
});

export const createMyBankAccount = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const payload = pick(req.body || {}, ["bankName", "accountNumber", "accountName", "isPrimary"]);

  if (!payload.bankName) return next(new AppError("bankName is required", 400));
  if (!payload.accountNumber) return next(new AppError("accountNumber is required", 400));
  if (!payload.accountName) return next(new AppError("accountName is required", 400));

  const isPrimary = Boolean(payload.isPrimary);

  if (isPrimary) {
    await BankAccountModel.updateMany({ userId: req.user.profileId }, { $set: { isPrimary: false } });
  }

  const account = await BankAccountModel.create({
    userId: req.user.profileId,
    bankName: String(payload.bankName).trim(),
    accountNumber: String(payload.accountNumber).trim(),
    accountName: String(payload.accountName).trim(),
    isPrimary,
  });

  return sendSuccess(res, { statusCode: 201, data: { account } });
});

export const updateMyBankAccount = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const accountId = req.params.id;

  const allowed = ["bankName", "accountNumber", "accountName", "isPrimary"];
  const updates = pick(req.body || {}, allowed);

  if (typeof updates.isPrimary !== "undefined" && Boolean(updates.isPrimary)) {
    await BankAccountModel.updateMany({ userId: req.user.profileId }, { $set: { isPrimary: false } });
  }

  const account = await BankAccountModel.findOneAndUpdate(
    { _id: accountId, userId: req.user.profileId },
    updates,
    { new: true, runValidators: true },
  );

  if (!account) return next(new AppError("Bank account not found", 404));

  return sendSuccess(res, { statusCode: 200, data: { account } });
});

export const deleteMyBankAccount = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const accountId = req.params.id;
  const account = await BankAccountModel.findOneAndDelete({ _id: accountId, userId: req.user.profileId });
  if (!account) return next(new AppError("Bank account not found", 404));

  if (account.isPrimary) {
    const latest = await BankAccountModel.findOne({ userId: req.user.profileId }).sort({ createdAt: -1 });
    if (latest) {
      latest.isPrimary = true;
      await latest.save();
    }
  }

  return sendSuccess(res, { statusCode: 200, message: "Bank account deleted" });
});

