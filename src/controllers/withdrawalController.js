import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import { WithdrawalRequestModel, WithdrawalStatuses } from "../models/WithdrawalRequest.js";
import { BankAccountModel } from "../models/BankAccount.js";
import { TransactionModel } from "../models/Transaction.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { GroupModel } from "../models/Group.js";
import { computeContributionBalances } from "../utils/finance.js";
import { randomId } from "../utils/crypto.js";
import { normalizeContributionType } from "../utils/contributionPolicy.js";
import { hasUserRole } from "../utils/roles.js";
import {
  createTransferRecipient,
  initiateTransfer,
  listBanks as listPaystackBanks,
  finalizeTransfer,
  resendTransferOtp,
  verifyTransfer,
} from "../services/paystack.js";

const OTP_RESEND_COOLDOWN_MS = (() => {
  const secondsRaw = Number(process.env.WITHDRAWAL_OTP_RESEND_COOLDOWN_SECONDS);
  if (Number.isFinite(secondsRaw) && secondsRaw > 0) {
    return Math.round(secondsRaw * 1000);
  }
  const msRaw = Number(process.env.WITHDRAWAL_OTP_RESEND_COOLDOWN_MS);
  if (Number.isFinite(msRaw) && msRaw > 0) return Math.round(msRaw);
  return 60_000;
})();

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

async function getManagedUserIds(manageableGroupIds) {
  if (!manageableGroupIds) return null;
  if (manageableGroupIds.length === 0) return [];

  const ids = await GroupMembershipModel.distinct("userId", {
    groupId: { $in: manageableGroupIds },
    status: "active",
  });

  return ids.map((id) => String(id));
}

async function ensureWithdrawalAccess(req, withdrawal) {
  const manageableGroupIds = await getManageableGroupIds(req);
  if (!manageableGroupIds) return;
  if (manageableGroupIds.length === 0) {
    throw new AppError("You cannot manage withdrawals for this member", 403);
  }

  if (withdrawal.groupId) {
    const groupId = String(withdrawal.groupId);
    if (!manageableGroupIds.includes(groupId)) {
      throw new AppError("You cannot manage withdrawals for this group", 403);
    }
    return;
  }

  const isMember = await GroupMembershipModel.exists({
    userId: withdrawal.userId,
    groupId: { $in: manageableGroupIds },
    status: "active",
  });

  if (!isMember) {
    throw new AppError("You cannot manage withdrawals for this member", 403);
  }
}

function normalizeBankName(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function buildTransferReference() {
  const stamp = Date.now().toString(36);
  const rand = randomId(4);
  return `wdr_${stamp}_${rand}`.slice(0, 50);
}

function sanitizeTransferReference(input) {
  const raw = String(input || "").toLowerCase().replace(/[^a-z0-9_-]/g, "");
  if (raw.length >= 16 && raw.length <= 50) return raw;
  return buildTransferReference();
}

async function resolveBankCode({ bankCode, bankName } = {}) {
  if (bankCode) return String(bankCode).trim();
  if (!bankName) return null;
  const paystackRes = await listPaystackBanks({ country: "nigeria" });
  const banks = Array.isArray(paystackRes?.data) ? paystackRes.data : [];
  if (banks.length === 0) return null;
  const target = normalizeBankName(bankName);
  const match = banks.find(
    (bank) =>
      normalizeBankName(bank?.name) === target ||
      normalizeBankName(bank?.slug) === target,
  );
  return match?.code ? String(match.code) : null;
}

export const listMyWithdrawals = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const withdrawals = await WithdrawalRequestModel.find({ userId: req.user.profileId }).sort({ createdAt: -1 });
  const otpResendCooldownSeconds = OTP_RESEND_COOLDOWN_MS > 0
    ? Math.ceil(OTP_RESEND_COOLDOWN_MS / 1000)
    : 0;
  return sendSuccess(res, {
    statusCode: 200,
    results: withdrawals.length,
    data: { withdrawals, otpResendCooldownSeconds },
  });
});

export const getMyWithdrawalBalance = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const groupId = req.query?.groupId || req.query?.group_id || null;
  const contributionTypeRaw = req.query?.contributionType || req.query?.contribution_type || null;
  const contributionType = contributionTypeRaw
    ? normalizeContributionType(contributionTypeRaw)
    : null;

  if (contributionTypeRaw && !contributionType) {
    return next(new AppError("Invalid contributionType", 400));
  }

  if (groupId) {
    const membership = await GroupMembershipModel.exists({
      userId: req.user.profileId,
      groupId,
      status: "active",
    });
    if (!membership) {
      return next(new AppError("You are not an active member of the selected group", 400));
    }
  }

  const balance = await computeContributionBalances(req.user.profileId, {
    groupId,
    contributionType,
  });

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      ...balance,
      groupId: groupId || null,
      contributionType: contributionType || null,
    },
  });
});

export const createWithdrawalRequest = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const amount = Number(req.body?.amount);
  if (!amount || amount <= 0) return next(new AppError("amount is required", 400));
  if (amount < 1000) return next(new AppError("Minimum withdrawal amount is 1000", 400));

  const contributionTypeRaw = req.body?.contributionType ?? req.body?.contribution_type ?? null;
  const contributionType = normalizeContributionType(contributionTypeRaw);
  if (!contributionType) {
    return next(new AppError("contributionType is required", 400));
  }

  const groupId = req.body?.groupId || req.body?.group_id || null;
  let groupName = null;
  if (groupId) {
    const membership = await GroupMembershipModel.findOne({
      userId: req.user.profileId,
      groupId,
      status: "active",
    }).lean();
    if (!membership) {
      return next(new AppError("You are not an active member of the selected group", 400));
    }
    const group = await GroupModel.findById(groupId, { groupName: 1 }).lean();
    groupName = group?.groupName ?? null;
  }

  const { availableBalance } = await computeContributionBalances(req.user.profileId, {
    groupId,
    contributionType,
  });
  if (amount > availableBalance) return next(new AppError("Amount exceeds available balance", 400));

  const bankAccountId = req.body?.bankAccountId || req.body?.bank_account_id || null;
  if (!bankAccountId) return next(new AppError("bankAccountId is required", 400));

  const account = await BankAccountModel.findOne({ _id: bankAccountId, userId: req.user.profileId });
  if (!account) return next(new AppError("Bank account not found", 404));

  const request = await WithdrawalRequestModel.create({
    userId: req.user.profileId,
    bankAccountId: account._id,
    groupId,
    groupName,
    contributionType,
    amount,
    bankName: account.bankName,
    bankCode: account.bankCode ?? null,
    accountNumber: account.accountNumber,
    accountName: account.accountName,
    reason: req.body?.reason ? String(req.body.reason).trim() : null,
    status: "pending",
  });

  return sendSuccess(res, { statusCode: 201, data: { withdrawal: request } });
});

export const listWithdrawals = catchAsync(async (req, res) => {
  if (!req.user) throw new AppError("Not authenticated", 401);

  const filter = {};

  if (typeof req.query?.status === "string" && req.query.status.trim()) {
    const s = req.query.status.trim();
    if (WithdrawalStatuses.includes(s)) filter.status = s;
  }

  const userIdParam =
    typeof req.query?.userId === "string" && req.query.userId.trim()
      ? req.query.userId.trim()
      : null;

  const manageableGroupIds = await getManageableGroupIds(req);
  if (manageableGroupIds) {
    const managedUserIds = await getManagedUserIds(manageableGroupIds);
    if (managedUserIds.length === 0) {
      return sendSuccess(res, {
        statusCode: 200,
        results: 0,
        data: { withdrawals: [] },
      });
    }

    if (userIdParam) {
      if (!managedUserIds.includes(userIdParam)) {
        throw new AppError("You cannot manage withdrawals for this member", 403);
      }
      filter.userId = userIdParam;
      filter.$or = [
        { groupId: { $in: manageableGroupIds } },
        { groupId: null },
      ];
    } else {
      filter.$or = [
        { groupId: { $in: manageableGroupIds } },
        { groupId: null, userId: { $in: managedUserIds } },
      ];
    }
  } else if (userIdParam) {
    filter.userId = userIdParam;
  }

  const withdrawals = await WithdrawalRequestModel.find(filter).sort({ createdAt: -1 });
  const otpResendCooldownSeconds = OTP_RESEND_COOLDOWN_MS > 0
    ? Math.ceil(OTP_RESEND_COOLDOWN_MS / 1000)
    : 0;
  return sendSuccess(res, {
    statusCode: 200,
    results: withdrawals.length,
    data: { withdrawals, otpResendCooldownSeconds },
  });
});

export const approveWithdrawal = catchAsync(async (req, res, next) => {
  const id = req.params.id;
  const withdrawal = await WithdrawalRequestModel.findById(id);
  if (!withdrawal) return next(new AppError("Withdrawal request not found", 404));

  await ensureWithdrawalAccess(req, withdrawal);

  if (!["pending"].includes(withdrawal.status)) {
    return next(new AppError("Withdrawal request is not pending", 400));
  }

  withdrawal.status = "approved";
  withdrawal.adminNotes = req.body?.adminNotes ? String(req.body.adminNotes).trim() : withdrawal.adminNotes;
  withdrawal.approvedAt = new Date();
  await withdrawal.save();

  return sendSuccess(res, { statusCode: 200, data: { withdrawal } });
});

export const rejectWithdrawal = catchAsync(async (req, res, next) => {
  const id = req.params.id;
  const withdrawal = await WithdrawalRequestModel.findById(id);
  if (!withdrawal) return next(new AppError("Withdrawal request not found", 404));

  await ensureWithdrawalAccess(req, withdrawal);

  if (!["pending", "approved", "processing"].includes(withdrawal.status)) {
    return next(new AppError("Withdrawal request cannot be rejected in current status", 400));
  }

  const rejectionReason = String(req.body?.rejectionReason || "").trim();
  if (!rejectionReason) return next(new AppError("rejectionReason is required", 400));

  withdrawal.status = "rejected";
  withdrawal.rejectionReason = rejectionReason;
  withdrawal.adminNotes = req.body?.adminNotes ? String(req.body.adminNotes).trim() : withdrawal.adminNotes;
  await withdrawal.save();

  return sendSuccess(res, { statusCode: 200, data: { withdrawal } });
});

export const markWithdrawalProcessing = catchAsync(async (req, res, next) => {
  const id = req.params.id;
  const withdrawal = await WithdrawalRequestModel.findById(id);
  if (!withdrawal) return next(new AppError("Withdrawal request not found", 404));

  await ensureWithdrawalAccess(req, withdrawal);

  if (!["approved"].includes(withdrawal.status)) {
    return next(new AppError("Withdrawal request must be approved first", 400));
  }

  withdrawal.status = "processing";
  await withdrawal.save();

  return sendSuccess(res, { statusCode: 200, data: { withdrawal } });
});

export const completeWithdrawal = catchAsync(async (req, res, next) => {
  const id = req.params.id;
  const withdrawal = await WithdrawalRequestModel.findById(id);
  if (!withdrawal) return next(new AppError("Withdrawal request not found", 404));

  await ensureWithdrawalAccess(req, withdrawal);

  if (!["approved", "processing"].includes(withdrawal.status)) {
    return next(new AppError("Withdrawal request must be approved or processing", 400));
  }

  const gateway = req.body?.gateway ? String(req.body.gateway).trim().toLowerCase() : "paystack";
  const requestedReference = req.body?.reference ? String(req.body.reference).trim() : null;

  if (gateway !== "paystack") {
    const reference = requestedReference || `WDR-${randomId(8)}`;
    const existing = await TransactionModel.findOne({ reference });
    if (existing) return next(new AppError("Duplicate transaction reference", 409));

    const tx = await TransactionModel.create({
      userId: withdrawal.userId,
      reference,
      amount: withdrawal.amount,
      type: "withdrawal",
      status: "success",
      description: `Withdrawal to ${withdrawal.bankName} (${String(withdrawal.accountNumber).slice(-4)})`,
      channel: "transfer",
      gateway,
      groupId: withdrawal.groupId ?? null,
      groupName: withdrawal.groupName ?? null,
      metadata: {
        withdrawalRequestId: withdrawal._id,
        contributionType: withdrawal.contributionType ?? null,
      },
    });

    withdrawal.status = "completed";
    withdrawal.completedAt = new Date();
    withdrawal.payoutReference = reference;
    withdrawal.payoutGateway = gateway;
    withdrawal.payoutStatus = "success";
    await withdrawal.save();

    return sendSuccess(res, { statusCode: 200, data: { withdrawal, transaction: tx } });
  }

  const account = withdrawal.bankAccountId
    ? await BankAccountModel.findById(withdrawal.bankAccountId)
    : null;
  const bankCode = await resolveBankCode({
    bankCode: withdrawal.bankCode || account?.bankCode,
    bankName: withdrawal.bankName,
  });
  if (!bankCode) {
    return next(
      new AppError(
        "Bank code is required for Paystack transfers. Please update the bank account.",
        400,
      ),
    );
  }
  if (account && !account.bankCode) {
    account.bankCode = bankCode;
    await account.save();
  }

  const existingReference =
    withdrawal.payoutReference && withdrawal.payoutStatus !== "failed"
      ? withdrawal.payoutReference
      : null;

  if (existingReference) {
    const verifyRes = await verifyTransfer(existingReference);
    const transfer = verifyRes?.data;
    if (!transfer) return next(new AppError("Invalid Paystack transfer response", 502));

    const status = String(transfer?.status || "").toLowerCase();
    const transferCode = transfer?.transfer_code || withdrawal.payoutTransferCode || null;

    let tx = await TransactionModel.findOne({ reference: existingReference });
    if (!tx) {
      tx = await TransactionModel.create({
        userId: withdrawal.userId,
        reference: existingReference,
        amount: withdrawal.amount,
        type: "withdrawal",
        status: status === "success" ? "success" : status === "failed" ? "failed" : "pending",
        description: `Withdrawal to ${withdrawal.bankName} (${String(withdrawal.accountNumber).slice(-4)})`,
        channel: "transfer",
        gateway: "paystack",
        groupId: withdrawal.groupId ?? null,
        groupName: withdrawal.groupName ?? null,
        metadata: {
          withdrawalRequestId: withdrawal._id,
          contributionType: withdrawal.contributionType ?? null,
          paystackTransfer: {
            reference: existingReference,
            transferCode,
            status,
            transferId: transfer?.id ?? null,
          },
        },
      });
    } else {
      tx.status = status === "success" ? "success" : status === "failed" ? "failed" : "pending";
      tx.gateway = "paystack";
      tx.channel = tx.channel || "transfer";
      tx.metadata = {
        ...(tx.metadata || {}),
        paystackTransfer: {
          reference: existingReference,
          transferCode,
          status,
          transferId: transfer?.id ?? null,
        },
      };
      await tx.save();
    }

    withdrawal.payoutReference = existingReference;
    withdrawal.payoutGateway = "paystack";
    withdrawal.payoutTransferCode = transferCode;
    withdrawal.payoutStatus = status || null;
    if (status === "success") {
      withdrawal.status = "completed";
      withdrawal.completedAt = new Date();
    } else if (status === "failed") {
      withdrawal.status = "approved";
    } else {
      withdrawal.status = "processing";
    }
    await withdrawal.save();

    return sendSuccess(res, { statusCode: 200, data: { withdrawal, transaction: tx } });
  }

  const reference = sanitizeTransferReference(requestedReference);
  const duplicate = await TransactionModel.findOne({ reference });
  if (duplicate) return next(new AppError("Duplicate transaction reference", 409));

  const recipientRes = await createTransferRecipient({
    type: "nuban",
    name: withdrawal.accountName,
    account_number: withdrawal.accountNumber,
    bank_code: bankCode,
    currency: "NGN",
  });
  const recipientCode = recipientRes?.data?.recipient_code;
  if (!recipientCode) return next(new AppError("Unable to create transfer recipient", 502));

  const transferRes = await initiateTransfer({
    source: "balance",
    amount: Math.round(Number(withdrawal.amount || 0) * 100),
    recipient: recipientCode,
    reference,
    reason: `Withdrawal ${withdrawal._id}`,
  });
  const transfer = transferRes?.data;
  if (!transfer) return next(new AppError("Invalid Paystack transfer response", 502));

  const status = String(transfer?.status || "").toLowerCase();
  const transferCode = transfer?.transfer_code || null;

  const tx = await TransactionModel.create({
    userId: withdrawal.userId,
    reference,
    amount: withdrawal.amount,
    type: "withdrawal",
    status: status === "success" ? "success" : status === "failed" ? "failed" : "pending",
    description: `Withdrawal to ${withdrawal.bankName} (${String(withdrawal.accountNumber).slice(-4)})`,
    channel: "transfer",
    gateway: "paystack",
    groupId: withdrawal.groupId ?? null,
    groupName: withdrawal.groupName ?? null,
    metadata: {
      withdrawalRequestId: withdrawal._id,
      contributionType: withdrawal.contributionType ?? null,
      paystackTransfer: {
        reference,
        transferCode,
        status,
        transferId: transfer?.id ?? null,
        recipientCode,
      },
    },
  });

  withdrawal.payoutReference = reference;
  withdrawal.payoutGateway = "paystack";
  withdrawal.payoutTransferCode = transferCode;
  withdrawal.payoutStatus = status || null;
  if (status === "success") {
    withdrawal.status = "completed";
    withdrawal.completedAt = new Date();
  } else if (status === "failed") {
    withdrawal.status = "approved";
  } else {
    withdrawal.status = "processing";
  }
  withdrawal.bankCode = withdrawal.bankCode || bankCode;
  await withdrawal.save();

  return sendSuccess(res, { statusCode: 200, data: { withdrawal, transaction: tx } });
});

export const finalizeWithdrawalOtp = catchAsync(async (req, res, next) => {
  const id = req.params.id;
  const withdrawal = await WithdrawalRequestModel.findById(id);
  if (!withdrawal) return next(new AppError("Withdrawal request not found", 404));

  await ensureWithdrawalAccess(req, withdrawal);

  if (!["approved", "processing"].includes(withdrawal.status)) {
    return next(new AppError("Withdrawal request must be approved or processing", 400));
  }

  const otp = String(req.body?.otp || "").trim();
  if (!otp) return next(new AppError("otp is required", 400));

  const transferCodeRaw =
    req.body?.transferCode ||
    req.body?.transfer_code ||
    withdrawal.payoutTransferCode ||
    null;
  const transferCode = transferCodeRaw ? String(transferCodeRaw).trim() : "";
  if (!transferCode) return next(new AppError("transfer_code is required", 400));

  const finalizeRes = await finalizeTransfer({
    transfer_code: transferCode,
    otp,
  });
  const transfer = finalizeRes?.data;
  if (!transfer) return next(new AppError("Invalid Paystack transfer response", 502));

  const status = String(transfer?.status || "").toLowerCase();
  const reference = transfer?.reference ? String(transfer.reference) : withdrawal.payoutReference;
  const resolvedTransferCode = transfer?.transfer_code || transferCode;

  let tx = reference ? await TransactionModel.findOne({ reference }) : null;
  if (!tx && reference) {
    tx = await TransactionModel.create({
      userId: withdrawal.userId,
      reference,
      amount: withdrawal.amount,
      type: "withdrawal",
      status: status === "success" ? "success" : status === "failed" ? "failed" : "pending",
      description: `Withdrawal to ${withdrawal.bankName} (${String(withdrawal.accountNumber).slice(-4)})`,
      channel: "transfer",
      gateway: "paystack",
      groupId: withdrawal.groupId ?? null,
      groupName: withdrawal.groupName ?? null,
      metadata: {
        withdrawalRequestId: withdrawal._id,
        contributionType: withdrawal.contributionType ?? null,
        paystackTransfer: {
          reference,
          transferCode: resolvedTransferCode,
          status,
          transferId: transfer?.id ?? null,
        },
      },
    });
  } else if (tx) {
    tx.status = status === "success" ? "success" : status === "failed" ? "failed" : "pending";
    tx.gateway = "paystack";
    tx.channel = tx.channel || "transfer";
    tx.metadata = {
      ...(tx.metadata || {}),
      paystackTransfer: {
        reference: reference || tx.reference,
        transferCode: resolvedTransferCode,
        status,
        transferId: transfer?.id ?? null,
      },
    };
    await tx.save();
  }

  if (reference) {
    withdrawal.payoutReference = reference;
  }
  withdrawal.payoutGateway = "paystack";
  withdrawal.payoutTransferCode = resolvedTransferCode || withdrawal.payoutTransferCode;
  withdrawal.payoutStatus = status || withdrawal.payoutStatus;

  if (status === "success") {
    withdrawal.status = "completed";
    withdrawal.completedAt = new Date();
  } else if (status === "failed" || status === "reversed") {
    withdrawal.status = "approved";
  } else {
    withdrawal.status = "processing";
  }

  await withdrawal.save();

  return sendSuccess(res, { statusCode: 200, data: { withdrawal, transaction: tx } });
});

export const resendWithdrawalOtp = catchAsync(async (req, res, next) => {
  const id = req.params.id;
  const withdrawal = await WithdrawalRequestModel.findById(id);
  if (!withdrawal) return next(new AppError("Withdrawal request not found", 404));

  await ensureWithdrawalAccess(req, withdrawal);

  if (!["approved", "processing"].includes(withdrawal.status)) {
    return next(new AppError("Withdrawal request must be approved or processing", 400));
  }

  if (OTP_RESEND_COOLDOWN_MS > 0 && withdrawal.payoutOtpResentAt) {
    const lastResentAt = new Date(withdrawal.payoutOtpResentAt).getTime();
    if (Number.isFinite(lastResentAt)) {
      const elapsedMs = Date.now() - lastResentAt;
      if (elapsedMs >= 0 && elapsedMs < OTP_RESEND_COOLDOWN_MS) {
        const retryAfterSeconds = Math.ceil(
          (OTP_RESEND_COOLDOWN_MS - elapsedMs) / 1000,
        );
        res.set("Retry-After", String(retryAfterSeconds));
        return next(
          new AppError(
            `Please wait ${retryAfterSeconds}s before resending OTP.`,
            429,
          ),
        );
      }
    }
  }

  const transferCodeRaw =
    req.body?.transferCode ||
    req.body?.transfer_code ||
    withdrawal.payoutTransferCode ||
    null;
  const transferCode = transferCodeRaw ? String(transferCodeRaw).trim() : "";
  if (!transferCode) return next(new AppError("transfer_code is required", 400));

  const reasonRaw = req.body?.reason ? String(req.body.reason).trim() : "";
  const normalizedReason = reasonRaw.toLowerCase();
  let reason = "transfer";
  if (normalizedReason === "disable_otp") {
    reason = "disable_otp";
  } else if (normalizedReason === "transfer") {
    reason = "transfer";
  } else if (normalizedReason === "resend_otp") {
    reason = "transfer";
  }

  await resendTransferOtp({
    transfer_code: transferCode,
    reason,
  });

  withdrawal.payoutTransferCode = transferCode;
  withdrawal.payoutStatus = "otp";
  withdrawal.payoutGateway = "paystack";
  withdrawal.payoutOtpResentAt = new Date();
  if (withdrawal.status !== "processing") {
    withdrawal.status = "processing";
  }
  await withdrawal.save();

  if (OTP_RESEND_COOLDOWN_MS > 0) {
    res.set(
      "Retry-After",
      String(Math.ceil(OTP_RESEND_COOLDOWN_MS / 1000)),
    );
  }

  return sendSuccess(res, { statusCode: 200, data: { withdrawal } });
});
