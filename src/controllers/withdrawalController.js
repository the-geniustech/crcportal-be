import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import { WithdrawalRequestModel, WithdrawalStatuses } from "../models/WithdrawalRequest.js";
import { BankAccountModel } from "../models/BankAccount.js";
import { TransactionModel } from "../models/Transaction.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { GroupModel } from "../models/Group.js";
import { UserModel } from "../models/User.js";
import { computeContributionBalances } from "../utils/finance.js";
import { sendAdminAuthorizationOtp } from "../services/otp/sendAdminAuthorizationOtp.js";
import { randomId, sha256 } from "../utils/crypto.js";
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

const MANUAL_WITHDRAWAL_PAYOUT_OTP_TTL_MINUTES = (() => {
  const raw = Number(
    process.env.WITHDRAWAL_MANUAL_PAYOUT_OTP_TTL_MINUTES ||
      process.env.LOAN_MANUAL_DISBURSEMENT_OTP_TTL_MINUTES,
  );
  if (Number.isFinite(raw) && raw > 0) return Math.round(raw);
  return 10;
})();

const ManualWithdrawalPayoutMethods = [
  "cash",
  "bank_transfer",
  "bank_settlement",
  "cheque",
  "pos",
  "other",
];

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

function buildManualPayoutReference(withdrawal) {
  const base = String(withdrawal?._id || "withdrawal")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
  return `manual_${base}_${Date.now().toString(36)}_${randomId(3)}`.slice(
    0,
    50,
  );
}

function getAuthenticatedUserId(req) {
  return String(req?.user?._id || req?.user?.id || "");
}

function generateOtp() {
  return String(Math.floor(100000 + Math.random() * 900000));
}

function normalizeManualPayoutMethod(value) {
  const method = String(value || "")
    .trim()
    .toLowerCase();
  if (!ManualWithdrawalPayoutMethods.includes(method)) {
    throw new AppError("Invalid manual payout method", 400);
  }
  return method;
}

function formatManualPayoutMethodLabel(method) {
  switch (String(method || "").trim().toLowerCase()) {
    case "bank_transfer":
      return "bank transfer";
    case "bank_settlement":
      return "bank settlement";
    default:
      return String(method || "manual payout")
        .replace(/_/g, " ")
        .trim();
  }
}

function isManualPayoutGateway(value) {
  const gateway = String(value || "")
    .trim()
    .toLowerCase();
  return gateway === "manual" || ManualWithdrawalPayoutMethods.includes(gateway);
}

function clearManualPayoutAuthorization(withdrawal) {
  if (!withdrawal) return;
  withdrawal.manualPayoutOtpHash = null;
  withdrawal.manualPayoutOtpExpiresAt = null;
}

function clearPendingManualPayoutState(withdrawal) {
  if (!withdrawal) return;
  const previousStatus =
    String(withdrawal.manualPayout?.previousStatus || "").trim().toLowerCase() ||
    "approved";
  withdrawal.status = previousStatus === "processing" ? "processing" : "approved";
  withdrawal.payoutReference = null;
  withdrawal.payoutGateway = null;
  withdrawal.payoutTransferCode = null;
  withdrawal.payoutStatus = null;
  withdrawal.payoutOtpResentAt = null;
  withdrawal.manualPayout = null;
  clearManualPayoutAuthorization(withdrawal);
}

function isMissingOrStalePaystackTransferError(error) {
  const message = String(error?.message || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();

  return (
    message.includes("transfer not found") ||
    message.includes("could not find transfer") ||
    message.includes("invalid paystack transfer response") ||
    message.includes("invalid transfer response") ||
    message.includes("transfer reference does not exist") ||
    message.includes("reference not found")
  );
}

function appendPayoutEvent(withdrawal, req, event = {}) {
  if (!withdrawal) return;
  const occurredAt = event.occurredAt instanceof Date
    ? event.occurredAt
    : event.occurredAt
      ? new Date(event.occurredAt)
      : new Date();
  const actorUserId = getAuthenticatedUserId(req) || null;
  const actorProfileId = req?.user?.profileId || null;

  withdrawal.payoutEvents = Array.isArray(withdrawal.payoutEvents)
    ? withdrawal.payoutEvents
    : [];
  withdrawal.payoutEvents.push({
    eventType: event.eventType || "payout_event",
    gateway: event.gateway || withdrawal.payoutGateway || null,
    status: event.status || withdrawal.payoutStatus || null,
    reference: event.reference || withdrawal.payoutReference || null,
    transferCode: event.transferCode || withdrawal.payoutTransferCode || null,
    message: event.message || null,
    actorUserId,
    actorProfileId,
    occurredAt,
    metadata: event.metadata || null,
  });

  if (withdrawal.payoutEvents.length > 100) {
    withdrawal.payoutEvents = withdrawal.payoutEvents.slice(-100);
  }
}

function ensureManualPayoutInitiator(withdrawal, req) {
  const actorUserId = getAuthenticatedUserId(req);
  const initiatedByUserId = String(
    withdrawal?.manualPayout?.initiatedByUserId || "",
  );

  if (!initiatedByUserId || !actorUserId || initiatedByUserId !== actorUserId) {
    throw new AppError(
      "Only the admin who requested this manual payout OTP can finalize or resend it.",
      403,
    );
  }
}

function getWithdrawalBaseMetadata(withdrawal) {
  return {
    withdrawalRequestId: withdrawal._id,
    contributionType: withdrawal.contributionType ?? null,
  };
}

function buildWithdrawalDescription(withdrawal, options = {}) {
  const manualPayout = options.manualPayout || withdrawal.manualPayout || null;
  const method = String(manualPayout?.method || "").trim().toLowerCase();
  const beneficiary = `${withdrawal.bankName} (${String(withdrawal.accountNumber).slice(-4)})`;

  if (manualPayout) {
    if (["bank_transfer", "bank_settlement"].includes(method)) {
      return `Manual withdrawal payout to ${beneficiary}`;
    }
    return `Manual withdrawal payout via ${formatManualPayoutMethodLabel(method)}`;
  }

  return `Withdrawal to ${beneficiary}`;
}

async function upsertWithdrawalTransaction({
  withdrawal,
  reference,
  status,
  gateway,
  channel = "transfer",
  metadata = {},
  description = null,
}) {
  if (!reference) return null;

  const txStatus =
    status === "success" ? "success" : ["failed", "reversed"].includes(status) ? "failed" : "pending";
  const baseMetadata = {
    ...getWithdrawalBaseMetadata(withdrawal),
    ...metadata,
  };

  let tx = await TransactionModel.findOne({ reference });
  if (!tx) {
    tx = await TransactionModel.create({
      userId: withdrawal.userId,
      reference,
      amount: withdrawal.amount,
      type: "withdrawal",
      status: txStatus,
      description:
        description ||
        buildWithdrawalDescription(withdrawal, {
          manualPayout: metadata.manualPayout || null,
        }),
      channel,
      gateway,
      groupId: withdrawal.groupId ?? null,
      groupName: withdrawal.groupName ?? null,
      metadata: baseMetadata,
    });
    return tx;
  }

  tx.status = txStatus;
  tx.gateway = gateway;
  tx.channel = channel || tx.channel || "transfer";
  tx.description =
    description ||
    tx.description ||
    buildWithdrawalDescription(withdrawal, {
      manualPayout: metadata.manualPayout || null,
    });
  tx.metadata = {
    ...(tx.metadata || {}),
    ...baseMetadata,
  };
  await tx.save();
  return tx;
}

function applyPaystackTransferState(withdrawal, status) {
  if (status === "success") {
    withdrawal.status = "completed";
    withdrawal.completedAt = new Date();
    return;
  }

  withdrawal.completedAt = null;
  if (["failed", "reversed"].includes(status)) {
    withdrawal.status = "approved";
  } else {
    withdrawal.status = "processing";
  }
}

async function sendManualWithdrawalPayoutOtp({ user, otp }) {
  return sendAdminAuthorizationOtp({
    user,
    otp,
    ttlMinutes: MANUAL_WITHDRAWAL_PAYOUT_OTP_TTL_MINUTES,
    purpose: "manual withdrawal payout confirmation",
  });
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
  appendPayoutEvent(withdrawal, req, {
    eventType: "withdrawal_approved",
    message: "Withdrawal request approved for payout processing.",
    status: "approved",
  });
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
  appendPayoutEvent(withdrawal, req, {
    eventType: "withdrawal_rejected",
    message: "Withdrawal request was rejected.",
    status: "rejected",
    metadata: {
      rejectionReason,
    },
  });
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
  appendPayoutEvent(withdrawal, req, {
    eventType: "withdrawal_processing_marked",
    message: "Withdrawal manually marked as processing.",
    status: "processing",
  });
  await withdrawal.save();

  return sendSuccess(res, { statusCode: 200, data: { withdrawal } });
});

function appendOperationalNote(existing, extra) {
  const base = String(existing || "").trim();
  const addition = String(extra || "").trim();
  if (!addition) return base || null;
  if (!base) return addition;
  if (base.includes(addition)) return base;
  return `${base}\n${addition}`;
}

function buildSupersededPaystackAttemptNote({
  reference = null,
  transferCode = null,
  status = null,
} = {}) {
  const fragments = ["Superseded Paystack withdrawal payout attempt"];
  if (reference) fragments.push(`ref: ${reference}`);
  if (transferCode) fragments.push(`code: ${transferCode}`);
  if (status) fragments.push(`status: ${status}`);
  return fragments.join(" | ");
}

async function loadWithdrawalWithManualOtpState(id) {
  return WithdrawalRequestModel.findById(id).select(
    "+manualPayoutOtpHash +manualPayoutOtpExpiresAt",
  );
}

async function verifyAndSyncPaystackWithdrawal(withdrawal, options = {}) {
  const requestedReference = withdrawal?.payoutReference
    ? String(withdrawal.payoutReference).trim()
    : "";
  if (!requestedReference) {
    throw new AppError("Withdrawal payout reference not found", 400);
  }

  const verifyRes = await verifyTransfer(requestedReference);
  const transfer = verifyRes?.data;
  if (!transfer) throw new AppError("Invalid Paystack transfer response", 502);

  const reference = transfer?.reference
    ? String(transfer.reference)
    : requestedReference;
  const status = String(transfer?.status || "").toLowerCase();
  const transferCode =
    transfer?.transfer_code || withdrawal.payoutTransferCode || null;

  const tx = await upsertWithdrawalTransaction({
    withdrawal,
    reference,
    status,
    gateway: "paystack",
    channel: "transfer",
    metadata: {
      paystackTransfer: {
        reference,
        transferCode,
        status,
        transferId: transfer?.id ?? null,
      },
    },
  });

  withdrawal.payoutReference = reference;
  withdrawal.payoutGateway = "paystack";
  withdrawal.payoutTransferCode = transferCode;
  withdrawal.payoutStatus = status || null;
  applyPaystackTransferState(withdrawal, status);
  if (options.req) {
    appendPayoutEvent(withdrawal, options.req, {
      eventType: options.eventType || "paystack_verified",
      gateway: "paystack",
      status,
      reference,
      transferCode,
      message:
        options.message ||
        (status === "success"
          ? "Paystack transfer verification confirmed a successful payout."
          : `Paystack transfer verification returned status ${status || "unknown"}.`),
      metadata: {
        transferId: transfer?.id ?? null,
        ...(options.metadata || {}),
      },
    });
  }

  return { transaction: tx, transfer };
}

async function reconcilePaystackAttemptForManualSwitch(withdrawal, req) {
  const payoutGateway = String(withdrawal?.payoutGateway || "")
    .trim()
    .toLowerCase();
  const payoutStatus = String(withdrawal?.payoutStatus || "")
    .trim()
    .toLowerCase();

  if (payoutGateway !== "paystack" || !withdrawal?.payoutReference) {
    return { canSwitch: true, note: null };
  }

  if (["failed", "reversed"].includes(payoutStatus)) {
    return {
      canSwitch: true,
      note: buildSupersededPaystackAttemptNote({
        reference: withdrawal.payoutReference,
        transferCode: withdrawal.payoutTransferCode,
        status: withdrawal.payoutStatus,
      }),
    };
  }

  if (payoutStatus === "success") {
    return {
      canSwitch: false,
      note: null,
      reason: "This Paystack payout has already been completed successfully.",
    };
  }

  try {
    const { transfer } = await verifyAndSyncPaystackWithdrawal(withdrawal, {
      req,
      eventType: "paystack_verified_for_manual_switch",
      message:
        "Verified existing Paystack payout before attempting manual payout fallback.",
    });
    const verifiedStatus = String(transfer?.status || "").toLowerCase();

    if (verifiedStatus === "success") {
      return {
        canSwitch: false,
        note: null,
        reason:
          "The Paystack payout has already completed successfully. Manual payout is no longer required.",
      };
    }

    if (["failed", "reversed"].includes(verifiedStatus)) {
      return {
        canSwitch: true,
        note: buildSupersededPaystackAttemptNote({
          reference: withdrawal.payoutReference,
          transferCode: withdrawal.payoutTransferCode,
          status: verifiedStatus,
        }),
      };
    }

    appendPayoutEvent(withdrawal, req, {
      eventType: "paystack_superseded_for_manual_switch",
      gateway: "paystack",
      status: verifiedStatus || "active",
      reference: withdrawal.payoutReference,
      transferCode: withdrawal.payoutTransferCode,
      message:
        "A still-active Paystack payout was intentionally superseded so the withdrawal could be settled manually.",
      metadata: {
        verifiedStatus: verifiedStatus || null,
      },
    });
    return {
      canSwitch: true,
      note: buildSupersededPaystackAttemptNote({
        reference: withdrawal.payoutReference,
        transferCode: withdrawal.payoutTransferCode,
        status: verifiedStatus || "active",
      }),
    };
  } catch (error) {
    if (isMissingOrStalePaystackTransferError(error)) {
      appendPayoutEvent(withdrawal, req, {
        eventType: "paystack_stale_before_manual_switch",
        gateway: "paystack",
        status: "stale",
        reference: withdrawal.payoutReference,
        transferCode: withdrawal.payoutTransferCode,
        message:
          "A stale or missing Paystack transfer was detected and manual payout fallback was allowed.",
        metadata: {
          error: String(error?.message || error || ""),
        },
      });
      return {
        canSwitch: true,
        note: buildSupersededPaystackAttemptNote({
          reference: withdrawal.payoutReference,
          transferCode: withdrawal.payoutTransferCode,
          status: "stale",
        }),
      };
    }

    throw error;
  }
}

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
  const payoutGateway = String(withdrawal.payoutGateway || "").trim().toLowerCase();
  const payoutStatus = String(withdrawal.payoutStatus || "").trim().toLowerCase();

  if (payoutGateway === "manual" && payoutStatus === "otp") {
    return next(
      new AppError(
        "A manual payout is awaiting OTP authorization. Finalize or cancel it before switching to Paystack.",
        400,
      ),
    );
  }

  if (gateway !== "paystack") {
    if (isManualPayoutGateway(gateway)) {
      return next(
        new AppError(
          "Manual payout requires OTP authorization. Use the manual payout initiation endpoint.",
          400,
        ),
      );
    }

    const reference = requestedReference || `WDR-${randomId(8)}`;
    const existing = await TransactionModel.findOne({ reference });
    if (existing) return next(new AppError("Duplicate transaction reference", 409));

    const tx = await upsertWithdrawalTransaction({
      withdrawal,
      reference,
      status: "success",
      gateway,
      channel: "transfer",
    });

    withdrawal.status = "completed";
    withdrawal.completedAt = new Date();
    withdrawal.payoutReference = reference;
    withdrawal.payoutGateway = gateway;
    withdrawal.payoutTransferCode = null;
    withdrawal.payoutStatus = "success";
    withdrawal.payoutOtpResentAt = null;
    withdrawal.manualPayout = null;
    clearManualPayoutAuthorization(withdrawal);
    appendPayoutEvent(withdrawal, req, {
      eventType: "external_payout_completed",
      gateway,
      status: "success",
      reference,
      message: `Withdrawal payout was completed through ${gateway}.`,
    });
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
    withdrawal.payoutReference &&
    payoutGateway === "paystack" &&
    !["failed", "reversed", "success"].includes(payoutStatus)
      ? withdrawal.payoutReference
      : null;

  if (existingReference) {
    const { transaction } = await verifyAndSyncPaystackWithdrawal(withdrawal, {
      req,
      eventType: "paystack_verified_during_completion",
      message:
        "Verified the existing Paystack payout instead of creating a new transfer.",
    });
    await withdrawal.save();

    return sendSuccess(res, {
      statusCode: 200,
      data: { withdrawal, transaction },
    });
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

  const tx = await upsertWithdrawalTransaction({
    withdrawal,
    reference,
    status,
    gateway: "paystack",
    channel: "transfer",
    metadata: {
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
  applyPaystackTransferState(withdrawal, status);
  withdrawal.bankCode = withdrawal.bankCode || bankCode;
  withdrawal.payoutOtpResentAt = null;
  appendPayoutEvent(withdrawal, req, {
    eventType: "paystack_payout_initiated",
    gateway: "paystack",
    status,
    reference,
    transferCode,
    message:
      status === "otp"
        ? "Paystack payout was initiated and requires OTP authorization."
        : `Paystack payout was initiated with status ${status || "unknown"}.`,
    metadata: {
      recipientCode,
      transferId: transfer?.id ?? null,
    },
  });
  await withdrawal.save();

  return sendSuccess(res, { statusCode: 200, data: { withdrawal, transaction: tx } });
});

export const verifyWithdrawalTransfer = catchAsync(async (req, res, next) => {
  const id = req.params.id;
  const withdrawal = await WithdrawalRequestModel.findById(id);
  if (!withdrawal) return next(new AppError("Withdrawal request not found", 404));

  await ensureWithdrawalAccess(req, withdrawal);

  if (!["approved", "processing"].includes(withdrawal.status)) {
    return next(new AppError("Withdrawal request must be approved or processing", 400));
  }
  if (
    withdrawal.payoutGateway &&
    String(withdrawal.payoutGateway).toLowerCase() !== "paystack"
  ) {
    return next(
      new AppError(
        "This payout uses manual OTP authorization. Use the manual payout actions instead.",
        400,
      ),
    );
  }
  if (!withdrawal.payoutReference) {
    return next(new AppError("Withdrawal payout reference not found", 400));
  }

  const { transaction } = await verifyAndSyncPaystackWithdrawal(withdrawal, {
    req,
    eventType: "paystack_verified",
  });
  await withdrawal.save();

  return sendSuccess(res, {
    statusCode: 200,
    data: { withdrawal, transaction },
  });
});

export const initiateManualWithdrawalPayout = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const id = req.params.id;
  const withdrawal = await WithdrawalRequestModel.findById(id);
  if (!withdrawal) return next(new AppError("Withdrawal request not found", 404));

  await ensureWithdrawalAccess(req, withdrawal);

  if (!["approved", "processing"].includes(withdrawal.status)) {
    return next(new AppError("Withdrawal request must be approved or processing", 400));
  }

  const payoutGateway = String(withdrawal.payoutGateway || "").trim().toLowerCase();
  const payoutStatus = String(withdrawal.payoutStatus || "").trim().toLowerCase();
  if (payoutGateway === "manual" && payoutStatus === "otp") {
    return next(
      new AppError(
        "A manual payout is already awaiting OTP authorization. Finalize, resend, or cancel it first.",
        400,
      ),
    );
  }
  const paystackReconciliation = await reconcilePaystackAttemptForManualSwitch(
    withdrawal,
    req,
  );
  if (!paystackReconciliation.canSwitch) {
    await withdrawal.save();
    return next(
      new AppError(
        paystackReconciliation.reason ||
          "A Paystack payout is already in progress. Verify its status before switching to manual payout.",
        400,
      ),
    );
  }

  const method = normalizeManualPayoutMethod(req.body?.method || req.body?.payoutMethod);
  const occurredAt = req.body?.occurredAt ? new Date(req.body.occurredAt) : new Date();
  if (Number.isNaN(occurredAt.getTime())) {
    return next(new AppError("occurredAt must be a valid date", 400));
  }

  const externalReference = req.body?.externalReference
    ? String(req.body.externalReference).trim()
    : null;
  let notes = req.body?.notes ? String(req.body.notes).trim() : null;
  if (payoutGateway === "paystack" && withdrawal.payoutReference) {
    notes = appendOperationalNote(
      notes,
      paystackReconciliation.note ||
        buildSupersededPaystackAttemptNote({
          reference: withdrawal.payoutReference,
          transferCode: withdrawal.payoutTransferCode,
          status: withdrawal.payoutStatus,
        }),
    );
  }

  const actingUser = await UserModel.findById(getAuthenticatedUserId(req))
    .select("email phone emailVerifiedAt phoneVerifiedAt")
    .lean();
  if (!actingUser) {
    return next(new AppError("Authorized user not found", 404));
  }

  const otp = generateOtp();
  const delivery = await sendManualWithdrawalPayoutOtp({
    user: actingUser,
    otp,
  });

  const previousStatus = ["approved", "processing"].includes(withdrawal.status)
    ? withdrawal.status
    : "approved";

  withdrawal.status = "processing";
  withdrawal.completedAt = null;
  withdrawal.payoutGateway = "manual";
  withdrawal.payoutStatus = "otp";
  withdrawal.payoutTransferCode = null;
  withdrawal.payoutReference = buildManualPayoutReference(withdrawal);
  withdrawal.payoutOtpResentAt = new Date();
  withdrawal.manualPayout = {
    status: "pending_otp",
    method,
    amount: withdrawal.amount,
    externalReference,
    occurredAt,
    notes,
    previousStatus,
    initiatedByUserId: getAuthenticatedUserId(req),
    initiatedBy: req.user.profileId,
    authorizedBy: null,
    initiatedAt: new Date(),
    completedAt: null,
    otpChannel: delivery.channel,
    otpRecipient: delivery.maskedRecipient,
    otpSentAt: new Date(),
  };
  withdrawal.manualPayoutOtpHash = sha256(otp);
  withdrawal.manualPayoutOtpExpiresAt = new Date(
    Date.now() + MANUAL_WITHDRAWAL_PAYOUT_OTP_TTL_MINUTES * 60 * 1000,
  );
  appendPayoutEvent(withdrawal, req, {
    eventType: "manual_payout_initiated",
    gateway: "manual",
    status: "otp",
    reference: withdrawal.payoutReference,
    message: `Manual payout initiated via ${formatManualPayoutMethodLabel(method)} and OTP authorization was sent.`,
    metadata: {
      method,
      occurredAt: occurredAt.toISOString(),
      externalReference,
      otpRecipient: delivery.maskedRecipient,
    },
  });

  await withdrawal.save();

  return sendSuccess(res, {
    statusCode: 200,
    message: `Manual withdrawal payout OTP sent to ${delivery.maskedRecipient}.`,
    data: { withdrawal },
  });
});

export const finalizeWithdrawalOtp = catchAsync(async (req, res, next) => {
  const id = req.params.id;
  const withdrawal = await WithdrawalRequestModel.findById(id);
  if (!withdrawal) return next(new AppError("Withdrawal request not found", 404));

  await ensureWithdrawalAccess(req, withdrawal);

  if (!["approved", "processing"].includes(withdrawal.status)) {
    return next(new AppError("Withdrawal request must be approved or processing", 400));
  }
  if (
    withdrawal.payoutGateway &&
    String(withdrawal.payoutGateway).toLowerCase() !== "paystack"
  ) {
    return next(
      new AppError(
        "This payout uses manual OTP authorization. Use the manual finalization endpoint instead.",
        400,
      ),
    );
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

  const tx = await upsertWithdrawalTransaction({
    withdrawal,
    reference,
    status,
    gateway: "paystack",
    channel: "transfer",
    metadata: {
      paystackTransfer: {
        reference: reference || withdrawal.payoutReference,
        transferCode: resolvedTransferCode,
        status,
        transferId: transfer?.id ?? null,
      },
    },
  });

  if (reference) {
    withdrawal.payoutReference = reference;
  }
  withdrawal.payoutGateway = "paystack";
  withdrawal.payoutTransferCode = resolvedTransferCode || withdrawal.payoutTransferCode;
  withdrawal.payoutStatus = status || withdrawal.payoutStatus;
  applyPaystackTransferState(withdrawal, status);
  appendPayoutEvent(withdrawal, req, {
    eventType: "paystack_otp_finalized",
    gateway: "paystack",
    status,
    reference: reference || withdrawal.payoutReference,
    transferCode: resolvedTransferCode || withdrawal.payoutTransferCode,
    message:
      status === "success"
        ? "Paystack OTP was finalized and the payout completed successfully."
        : `Paystack OTP finalization returned status ${status || "unknown"}.`,
    metadata: {
      transferId: transfer?.id ?? null,
    },
  });
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
  if (
    withdrawal.payoutGateway &&
    String(withdrawal.payoutGateway).toLowerCase() !== "paystack"
  ) {
    return next(
      new AppError(
        "This payout uses manual OTP authorization. Use the manual resend endpoint instead.",
        400,
      ),
    );
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
  appendPayoutEvent(withdrawal, req, {
    eventType: "paystack_otp_resent",
    gateway: "paystack",
    status: "otp",
    transferCode,
    message: "A fresh Paystack OTP was requested for this payout.",
  });
  await withdrawal.save();

  if (OTP_RESEND_COOLDOWN_MS > 0) {
    res.set(
      "Retry-After",
      String(Math.ceil(OTP_RESEND_COOLDOWN_MS / 1000)),
    );
  }

  return sendSuccess(res, { statusCode: 200, data: { withdrawal } });
});

export const resendManualWithdrawalPayoutOtp = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const id = req.params.id;
  const withdrawal = await loadWithdrawalWithManualOtpState(id);
  if (!withdrawal) return next(new AppError("Withdrawal request not found", 404));

  await ensureWithdrawalAccess(req, withdrawal);

  if (!["approved", "processing"].includes(withdrawal.status)) {
    return next(new AppError("Withdrawal request must be approved or processing", 400));
  }
  if (String(withdrawal.payoutGateway || "").toLowerCase() !== "manual") {
    return next(new AppError("No pending manual payout found", 400));
  }
  if (String(withdrawal.payoutStatus || "").toLowerCase() !== "otp") {
    return next(new AppError("Manual payout is not currently awaiting OTP", 400));
  }

  ensureManualPayoutInitiator(withdrawal, req);

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

  const actingUser = await UserModel.findById(getAuthenticatedUserId(req))
    .select("email phone emailVerifiedAt phoneVerifiedAt")
    .lean();
  if (!actingUser) {
    return next(new AppError("Authorized user not found", 404));
  }

  const otp = generateOtp();
  const delivery = await sendManualWithdrawalPayoutOtp({
    user: actingUser,
    otp,
  });

  withdrawal.manualPayoutOtpHash = sha256(otp);
  withdrawal.manualPayoutOtpExpiresAt = new Date(
    Date.now() + MANUAL_WITHDRAWAL_PAYOUT_OTP_TTL_MINUTES * 60 * 1000,
  );
  withdrawal.payoutOtpResentAt = new Date();
  withdrawal.manualPayout = {
    ...(withdrawal.manualPayout || {}),
    status: "pending_otp",
    otpChannel: delivery.channel,
    otpRecipient: delivery.maskedRecipient,
    otpSentAt: new Date(),
  };
  appendPayoutEvent(withdrawal, req, {
    eventType: "manual_payout_otp_resent",
    gateway: "manual",
    status: "otp",
    reference: withdrawal.payoutReference,
    message: "A fresh manual payout OTP was sent to the initiating admin.",
    metadata: {
      otpRecipient: delivery.maskedRecipient,
    },
  });

  await withdrawal.save();

  if (OTP_RESEND_COOLDOWN_MS > 0) {
    res.set("Retry-After", String(Math.ceil(OTP_RESEND_COOLDOWN_MS / 1000)));
  }

  return sendSuccess(res, {
    statusCode: 200,
    message: `Manual withdrawal payout OTP sent to ${delivery.maskedRecipient}.`,
    data: { withdrawal },
  });
});

export const cancelManualWithdrawalPayout = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const id = req.params.id;
  const withdrawal = await WithdrawalRequestModel.findById(id);
  if (!withdrawal) return next(new AppError("Withdrawal request not found", 404));

  await ensureWithdrawalAccess(req, withdrawal);

  if (!["approved", "processing"].includes(withdrawal.status)) {
    return next(new AppError("Withdrawal request must be approved or processing", 400));
  }
  if (String(withdrawal.payoutGateway || "").toLowerCase() !== "manual") {
    return next(new AppError("No pending manual payout found", 400));
  }
  if (String(withdrawal.payoutStatus || "").toLowerCase() !== "otp") {
    return next(new AppError("Manual payout is not currently awaiting OTP", 400));
  }

  ensureManualPayoutInitiator(withdrawal, req);

  appendPayoutEvent(withdrawal, req, {
    eventType: "manual_payout_cancelled",
    gateway: "manual",
    status: "cancelled",
    reference: withdrawal.payoutReference,
    message: "Pending manual payout authorization was cancelled.",
    metadata: {
      method: withdrawal.manualPayout?.method || null,
    },
  });
  clearPendingManualPayoutState(withdrawal);
  await withdrawal.save();

  return sendSuccess(res, {
    statusCode: 200,
    message:
      "Pending manual withdrawal payout authorization cancelled. You can switch back to Paystack now.",
    data: { withdrawal },
  });
});

export const finalizeManualWithdrawalPayout = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const id = req.params.id;
  const withdrawal = await loadWithdrawalWithManualOtpState(id);
  if (!withdrawal) return next(new AppError("Withdrawal request not found", 404));

  await ensureWithdrawalAccess(req, withdrawal);

  if (!["approved", "processing"].includes(withdrawal.status)) {
    return next(new AppError("Withdrawal request must be approved or processing", 400));
  }
  if (String(withdrawal.payoutGateway || "").toLowerCase() !== "manual") {
    return next(new AppError("No pending manual payout found", 400));
  }
  if (String(withdrawal.payoutStatus || "").toLowerCase() !== "otp") {
    return next(new AppError("Manual payout is not currently awaiting OTP", 400));
  }

  const otpRaw = req.body?.otp ? String(req.body.otp).trim() : "";
  if (!otpRaw) return next(new AppError("otp is required", 400));

  ensureManualPayoutInitiator(withdrawal, req);

  if (!withdrawal.manualPayoutOtpHash || !withdrawal.manualPayoutOtpExpiresAt) {
    return next(
      new AppError(
        "Manual payout OTP has not been issued. Please request a new OTP.",
        400,
      ),
    );
  }
  if (withdrawal.manualPayoutOtpExpiresAt.getTime() <= Date.now()) {
    clearManualPayoutAuthorization(withdrawal);
    await withdrawal.save();
    return next(
      new AppError(
        "OTP has expired. Please request a new code to finalize this manual payout.",
        400,
      ),
    );
  }
  if (sha256(otpRaw) !== withdrawal.manualPayoutOtpHash) {
    return next(new AppError("Invalid OTP", 400));
  }

  const manualPayout = withdrawal.manualPayout || {};
  const reference =
    withdrawal.payoutReference || buildManualPayoutReference(withdrawal);
  const completedAt = new Date();

  const tx = await upsertWithdrawalTransaction({
    withdrawal,
    reference,
    status: "success",
    gateway: "manual",
    channel: String(manualPayout.method || "manual").trim().toLowerCase() || "manual",
    metadata: {
      manualPayout: {
        status: "completed",
        method: manualPayout.method || null,
        amount: manualPayout.amount ?? withdrawal.amount,
        externalReference: manualPayout.externalReference || null,
        occurredAt:
          manualPayout.occurredAt?.toISOString?.() || manualPayout.occurredAt || null,
        notes: manualPayout.notes || null,
        initiatedAt:
          manualPayout.initiatedAt?.toISOString?.() || manualPayout.initiatedAt || null,
        completedAt: completedAt.toISOString(),
      },
    },
    description: buildWithdrawalDescription(withdrawal, { manualPayout }),
  });

  withdrawal.status = "completed";
  withdrawal.completedAt = completedAt;
  withdrawal.payoutGateway = "manual";
  withdrawal.payoutStatus = "success";
  withdrawal.payoutTransferCode = null;
  withdrawal.payoutReference = reference;
  withdrawal.payoutOtpResentAt = null;
  withdrawal.manualPayout = {
    ...manualPayout,
    status: "completed",
    authorizedBy: req.user.profileId,
    completedAt,
    otpSentAt: manualPayout.otpSentAt || new Date(),
  };
  clearManualPayoutAuthorization(withdrawal);
  appendPayoutEvent(withdrawal, req, {
    eventType: "manual_payout_finalized",
    gateway: "manual",
    status: "success",
    reference,
    message: "Manual withdrawal payout was finalized successfully.",
    metadata: {
      method: manualPayout.method || null,
      externalReference: manualPayout.externalReference || null,
      completedAt: completedAt.toISOString(),
    },
  });

  await withdrawal.save();

  return sendSuccess(res, {
    statusCode: 200,
    message: "Manual withdrawal payout finalized successfully.",
    data: { withdrawal, transaction: tx },
  });
});
