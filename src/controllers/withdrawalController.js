import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import { WithdrawalRequestModel, WithdrawalStatuses } from "../models/WithdrawalRequest.js";
import { BankAccountModel } from "../models/BankAccount.js";
import { TransactionModel } from "../models/Transaction.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { GroupModel } from "../models/Group.js";
import { computeSavingsBalances } from "../utils/finance.js";
import { randomId } from "../utils/crypto.js";
import { normalizeContributionType } from "../utils/contributionPolicy.js";

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

export const listMyWithdrawals = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const withdrawals = await WithdrawalRequestModel.find({ userId: req.user.profileId }).sort({ createdAt: -1 });
  return sendSuccess(res, { statusCode: 200, results: withdrawals.length, data: { withdrawals } });
});

export const createWithdrawalRequest = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const amount = Number(req.body?.amount);
  if (!amount || amount <= 0) return next(new AppError("amount is required", 400));
  if (amount < 1000) return next(new AppError("Minimum withdrawal amount is 1000", 400));

  const { availableBalance } = await computeSavingsBalances(req.user.profileId);
  if (amount > availableBalance) return next(new AppError("Amount exceeds available balance", 400));

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
  return sendSuccess(res, { statusCode: 200, results: withdrawals.length, data: { withdrawals } });
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

  const reference = String(req.body?.reference || `WDR-${randomId(8)}`).trim();
  const gateway = req.body?.gateway ? String(req.body.gateway).trim() : "manual";

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
  await withdrawal.save();

  return sendSuccess(res, { statusCode: 200, data: { withdrawal, transaction: tx } });
});
