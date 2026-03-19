import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import { ContributionModel, ContributionStatuses } from "../models/Contribution.js";
import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { TransactionModel } from "../models/Transaction.js";
import { generateGroupContributionReportPdfBuffer } from "../services/pdf/groupContributionReportPdf.js";

function formatCurrency(amount) {
  const value = Number(amount || 0);
  if (!Number.isFinite(value)) return "NGN 0.00";
  return `NGN ${value.toLocaleString("en-NG", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function formatDate(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat("en-NG", {
    day: "numeric",
    month: "short",
    year: "numeric",
  }).format(date);
}

function shouldCountTowardSavings(status) {
  return status === "completed" || status === "verified";
}

function mapContributionStatusToTxStatus(status) {
  if (status === "completed" || status === "verified") return "success";
  if (status === "pending") return "pending";
  if (status === "overdue") return "failed";
  return "pending";
}

export const listGroupContributions = catchAsync(async (req, res) => {
  const group = req.group;

  const filter = { groupId: group._id };
  if (req.query?.year) filter.year = parseInt(String(req.query.year), 10);
  if (req.query?.month) filter.month = parseInt(String(req.query.month), 10);
  if (req.query?.status) filter.status = String(req.query.status);
  if (req.query?.contributionType) filter.contributionType = String(req.query.contributionType);
  if (req.query?.userId) filter.userId = String(req.query.userId);

  const contributions = await ContributionModel.find(filter)
    .sort({ year: -1, month: -1, createdAt: -1 })
    .populate("userId")
    .populate("verifiedBy");

  return sendSuccess(res, {
    statusCode: 200,
    results: contributions.length,
    data: { contributions },
  });
});

export const createGroupContribution = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const group = req.group;
  const {
    userId,
    month,
    year,
    amount,
    contributionType,
    status = "completed",
    paymentReference = null,
    paymentMethod = null,
    notes = null,
  } = req.body || {};

  const targetUserId = userId || req.user.profileId;

  if (!month || !year || typeof amount !== "number" || !contributionType) {
    return next(new AppError("month, year, amount, contributionType are required", 400));
  }

  if (status && !ContributionStatuses.includes(status)) {
    return next(
      new AppError(`Invalid status. Allowed: ${ContributionStatuses.join(", ")}`, 400),
    );
  }

  const membership = await GroupMembershipModel.findOne({
    groupId: group._id,
    userId: targetUserId,
    status: "active",
  });
  if (!membership) return next(new AppError("User is not an active group member", 400));

  const existing = await ContributionModel.findOne({
    groupId: group._id,
    userId: targetUserId,
    month,
    year,
    contributionType,
  });
  if (existing) return next(new AppError("Contribution already exists for this period/type", 409));

  const contribution = await ContributionModel.create({
    groupId: group._id,
    userId: targetUserId,
    month,
    year,
    amount,
    contributionType,
    status,
    paymentReference,
    paymentMethod,
    notes,
  });

  const contributionRef = String(paymentReference || `GC-${contribution._id}`);
  await TransactionModel.create({
    userId: targetUserId,
    reference: contributionRef,
    amount,
    type: "group_contribution",
    status: mapContributionStatusToTxStatus(status),
    description: `Group contribution - ${group.groupName}`,
    channel: paymentMethod ? String(paymentMethod) : null,
    groupId: group._id,
    groupName: group.groupName,
    metadata: {
      contributionId: contribution._id,
      month,
      year,
      contributionType,
    },
    gateway: "internal",
  });

  if (shouldCountTowardSavings(status)) {
    await Promise.all([
      GroupModel.findByIdAndUpdate(group._id, { $inc: { totalSavings: amount } }),
      GroupMembershipModel.findByIdAndUpdate(membership._id, {
        $inc: { totalContributed: amount },
      }),
    ]);
  }

  return sendSuccess(res, {
    statusCode: 201,
    message: "Contribution recorded",
    data: { contribution },
  });
});

export const updateContribution = catchAsync(async (req, res, next) => {
  const group = req.group;
  const { contributionId } = req.params;

  const allowed = ["status", "paymentReference", "paymentMethod", "notes", "amount"];
  const updates = {};
  for (const key of allowed) {
    if (Object.prototype.hasOwnProperty.call(req.body || {}, key)) updates[key] = req.body[key];
  }

  if (Object.keys(updates).length === 0) {
    return next(new AppError("No updatable fields provided", 400));
  }

  const existing = await ContributionModel.findOne({ _id: contributionId, groupId: group._id });
  if (!existing) return next(new AppError("Contribution not found", 404));

  const wasCounted = shouldCountTowardSavings(existing.status);

  const updated = await ContributionModel.findOneAndUpdate(
    { _id: contributionId, groupId: group._id },
    updates,
    { new: true, runValidators: true },
  );

  const isCounted = shouldCountTowardSavings(updated.status);

  await TransactionModel.findOneAndUpdate(
    { "metadata.contributionId": existing._id },
    {
      $set: {
        amount: updated.amount,
        status: mapContributionStatusToTxStatus(updated.status),
        description: `Group contribution - ${group.groupName}`,
        channel: updated.paymentMethod ? String(updated.paymentMethod) : null,
        groupId: group._id,
        groupName: group.groupName,
      },
    },
  );

  // Adjust totals only if crossing the "counted" boundary; amount edits are treated as admin ops and not auto-reconciled.
  if (!wasCounted && isCounted) {
    await Promise.all([
      GroupModel.findByIdAndUpdate(group._id, { $inc: { totalSavings: updated.amount } }),
      GroupMembershipModel.findOneAndUpdate(
        { groupId: group._id, userId: updated.userId },
        { $inc: { totalContributed: updated.amount } },
      ),
    ]);
  } else if (wasCounted && !isCounted) {
    await Promise.all([
      GroupModel.findByIdAndUpdate(group._id, { $inc: { totalSavings: -existing.amount } }),
      GroupMembershipModel.findOneAndUpdate(
        { groupId: group._id, userId: existing.userId },
        { $inc: { totalContributed: -existing.amount } },
      ),
    ]);
  }

  return sendSuccess(res, { statusCode: 200, data: { contribution: updated } });
});

export const verifyContribution = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const group = req.group;
  const { contributionId } = req.params;

  const existing = await ContributionModel.findOne({ _id: contributionId, groupId: group._id });
  if (!existing) return next(new AppError("Contribution not found", 404));

  const wasCounted = shouldCountTowardSavings(existing.status);

  existing.status = "verified";
  existing.verifiedBy = req.user.profileId;
  existing.verifiedAt = new Date();
  await existing.save({ validateBeforeSave: true });

  if (!wasCounted) {
    await Promise.all([
      GroupModel.findByIdAndUpdate(group._id, { $inc: { totalSavings: existing.amount } }),
      GroupMembershipModel.findOneAndUpdate(
        { groupId: group._id, userId: existing.userId },
        { $inc: { totalContributed: existing.amount } },
      ),
    ]);
  }

  await TransactionModel.findOneAndUpdate(
    { "metadata.contributionId": existing._id },
    { $set: { status: "success" } },
  );

  return sendSuccess(res, { statusCode: 200, data: { contribution: existing } });
});

export const downloadGroupContributionReportPdf = catchAsync(async (req, res, next) => {
  const group = req.group;
  if (!group) return next(new AppError("Group not found", 404));

  const now = new Date();
  const year = Number.isFinite(Number(req.query?.year))
    ? Number(req.query?.year)
    : now.getFullYear();
  const month = Number.isFinite(Number(req.query?.month))
    ? Number(req.query?.month)
    : now.getMonth() + 1;
  const contributionType = req.query?.contributionType
    ? String(req.query.contributionType)
    : null;

  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) {
    return next(new AppError("Invalid year or month provided", 400));
  }

  const [memberships, contributions] = await Promise.all([
    GroupMembershipModel.find({ groupId: group._id, status: "active" })
      .populate("userId")
      .lean(),
    ContributionModel.find({
      groupId: group._id,
      year,
      month,
      ...(contributionType ? { contributionType } : {}),
    })
      .sort({ createdAt: -1 })
      .populate("userId")
      .lean(),
  ]);

  const contributionByUserId = new Map();
  contributions.forEach((c) => {
    const userObj = c.userId && typeof c.userId === "object" ? c.userId : null;
    const userId = userObj?._id || c.userId;
    if (!userId) return;
    const key = String(userId);
    if (!contributionByUserId.has(key)) contributionByUserId.set(key, c);
  });

  let paidCount = 0;
  let overdueCount = 0;
  let totalCollected = 0;

  const rows = memberships.map((member) => {
    const userObj =
      member.userId && typeof member.userId === "object" ? member.userId : null;
    const userId = userObj?._id || member.userId;
    const name =
      userObj?.fullName ||
      userObj?.full_name ||
      member.fullName ||
      member.name ||
      "Member";

    const contribution = contributionByUserId.get(String(userId));
    const statusRaw = contribution?.status || "pending";
    const status =
      statusRaw === "overdue"
        ? "Overdue"
        : statusRaw === "pending"
          ? "Pending"
          : "Paid";
    const amount = Number(contribution?.amount ?? 0);
    const paidDate =
      status === "Paid" ? contribution?.updatedAt || contribution?.createdAt : null;

    if (status === "Paid") {
      paidCount += 1;
      totalCollected += amount;
    } else if (status === "Overdue") {
      overdueCount += 1;
    }

    return {
      member: name,
      status,
      amount: formatCurrency(amount),
      paidDate: paidDate ? formatDate(paidDate) : "-",
    };
  });

  const pendingCount = Math.max(0, memberships.length - paidCount - overdueCount);
  const totalExpected = Number(group.monthlyContribution ?? 0) * memberships.length;
  const collectionRate =
    totalExpected > 0 ? Math.round((totalCollected / totalExpected) * 100) : 0;

  const periodLabel = new Date(year, month - 1, 1).toLocaleDateString("en-US", {
    month: "long",
    year: "numeric",
  });

  const pdfBuffer = await generateGroupContributionReportPdfBuffer({
    groupName: group.groupName || "Group",
    periodLabel,
    generatedAt: now,
    summary: {
      totalExpected,
      totalCollected,
      collectionRate: Math.min(100, Math.max(0, collectionRate)),
      paidCount,
      pendingCount,
      overdueCount,
    },
    rows,
  });

  const filename = `contribution-report-${String(group.groupName || "group")
    .toLowerCase()
    .replace(/\s+/g, "-")}-${year}-${String(month).padStart(2, "0")}.pdf`;

  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
  res.status(200).send(pdfBuffer);
});
