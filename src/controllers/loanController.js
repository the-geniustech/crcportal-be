import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import {
  LoanApplicationModel,
  LoanApplicationStatuses,
} from "../models/LoanApplication.js";
import { LoanGuarantorModel } from "../models/LoanGuarantor.js";
import { GuarantorNotificationModel } from "../models/GuarantorNotification.js";
import { LoanRepaymentScheduleItemModel } from "../models/LoanRepaymentScheduleItem.js";
import { TransactionModel } from "../models/Transaction.js";
import { ProfileModel } from "../models/Profile.js";
import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { ContributionModel } from "../models/Contribution.js";
import { createNotification } from "../services/notificationService.js";

function pick(obj, allowedKeys) {
  const out = {};
  for (const key of allowedKeys) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) out[key] = obj[key];
  }
  return out;
}

function formatLoanCode(n) {
  const num = Number(n) || 0;
  return `L${String(num).padStart(3, "0")}`;
}

async function getNextLoanNumber() {
  const last = await LoanApplicationModel.findOne({
    loanNumber: { $ne: null },
  })
    .sort({ loanNumber: -1 })
    .select("loanNumber");
  return (last?.loanNumber ?? 0) + 1;
}

function parseDateOrNull(value) {
  if (!value) return null;
  const d = new Date(value);
  // eslint-disable-next-line no-restricted-globals
  return isNaN(d.getTime()) ? null : d;
}

function addMonths(date, months) {
  const d = new Date(date);
  const day = d.getDate();
  d.setMonth(d.getMonth() + months);
  if (d.getDate() < day) d.setDate(0);
  return d;
}

function buildAmortizedSchedule({
  principal,
  annualRatePct,
  months,
  startDate,
}) {
  const P = Number(principal);
  const n = Math.max(1, Number(months) | 0);
  const annual = Math.max(0, Number(annualRatePct) || 0);
  const r = annual / 100 / 12;

  const payment =
    r === 0 ? P / n : (P * r * Math.pow(1 + r, n)) / (Math.pow(1 + r, n) - 1);

  let balance = P;
  const items = [];

  for (let i = 1; i <= n; i += 1) {
    const interest = r === 0 ? 0 : balance * r;
    let principalPaid = payment - interest;

    if (i === n) {
      principalPaid = balance;
    }

    const total = principalPaid + interest;
    balance = Math.max(0, balance - principalPaid);

    const dueDate = addMonths(startDate, i - 1);

    items.push({
      installmentNumber: i,
      dueDate,
      principalAmount: Math.round(principalPaid),
      interestAmount: Math.round(interest),
      totalAmount: Math.round(total),
      status: i === 1 ? "pending" : "upcoming",
    });
  }

  const totalRepayable = items.reduce((sum, it) => sum + it.totalAmount, 0);
  const monthlyPayment = items[0]?.totalAmount ?? Math.round(payment);

  return { items, totalRepayable, monthlyPayment };
}

async function ensureActiveMember(profileId) {
  const profile =
    await ProfileModel.findById(profileId).select("membershipStatus");
  if (!profile) throw new AppError("User profile not found", 400);
  if (profile.membershipStatus !== "active") {
    throw new AppError("Membership is not active", 403);
  }
  return profile;
}

export const getLoanEligibility = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const profile = await ProfileModel.findById(req.user.profileId).select(
    "createdAt membershipStatus",
  );
  const membership = await GroupMembershipModel.findOne({
    userId: req.user.profileId,
    status: "active",
  });

  if (!profile) return next(new AppError("User profile not found", 400));
  if (!membership) {
    return next(
      new AppError("You must be an active member to apply for a loan", 403),
    );
  }

  const now = new Date();
  // const createdAt = profile.createdAt ? new Date(profile.createdAt) : now;
  const joinedAt = membership.joinedAt ? new Date(membership.joinedAt) : now;
  const membershipDuration = Math.max(
    0,
    (now.getFullYear() - joinedAt.getFullYear()) * 12 +
      (now.getMonth() - joinedAt.getMonth()),
  );
  console.log("Membership Duration: ", membershipDuration);

  const [groupsJoined, contributionAgg, previousLoans, defaultedLoans] =
    await Promise.all([
      GroupMembershipModel.countDocuments({
        userId: req.user.profileId,
        status: "active",
      }),
      ContributionModel.aggregate([
        {
          $match: {
            userId: profile._id,
            status: { $in: ["completed", "verified"] },
          },
        },
        { $group: { _id: null, sum: { $sum: "$amount" } } },
      ]),
      LoanApplicationModel.countDocuments({
        userId: req.user.profileId,
        status: { $in: ["disbursed", "completed", "defaulted"] },
      }),
      LoanApplicationModel.countDocuments({
        userId: req.user.profileId,
        status: "defaulted",
      }),
    ]);

  const totalContributions = Number(contributionAgg?.[0]?.sum ?? 0);

  const eligibility = {
    savingsBalance: totalContributions,
    totalContributions,
    membershipDuration,
    groupsJoined,
    attendanceRate: 92,
    contributionStreak: 12,
    previousLoans,
    defaultedLoans,
    creditScore: 850,
  };

  return sendSuccess(res, { statusCode: 200, data: { eligibility } });
});

export const createLoanApplication = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  if (req.user.role !== "admin") {
    await ensureActiveMember(req.user.profileId);
  }

  const allowed = [
    "groupId",
    "groupName",
    "loanAmount",
    "loanPurpose",
    "purposeDescription",
    "repaymentPeriod",
    "interestRate",
    "monthlyIncome",
    "documents",
    "guarantors",
  ];

  const payload = pick(req.body || {}, allowed);

  if (!payload.loanAmount || Number(payload.loanAmount) <= 0) {
    return next(new AppError("loanAmount is required", 400));
  }
  if (!payload.loanPurpose)
    return next(new AppError("loanPurpose is required", 400));
  if (!payload.repaymentPeriod || Number(payload.repaymentPeriod) <= 0) {
    return next(new AppError("repaymentPeriod is required", 400));
  }

  let group = null;
  if (payload.groupId) {
    group = await GroupModel.findById(payload.groupId);
    if (!group) return next(new AppError("Group not found", 404));

    if (req.user.role !== "admin") {
      const membership = await GroupMembershipModel.findOne({
        groupId: group._id,
        userId: req.user.profileId,
        status: "active",
      });
      if (!membership) {
        return next(
          new AppError("You must be an active member of this group", 403),
        );
      }
    }

    payload.groupName = payload.groupName || group.groupName;
  }

  const guarantors = Array.isArray(payload.guarantors)
    ? payload.guarantors
    : [];
  const memberGuarantors = guarantors.filter((g) => g && g.type === "member");

  let liabilitySum = 0;
  const seenProfiles = new Set();

  for (const g of memberGuarantors) {
    if (!g.profileId) {
      return next(
        new AppError("Member guarantors must include profileId", 400),
      );
    }
    const profileId = String(g.profileId);
    if (seenProfiles.has(profileId)) {
      return next(new AppError("Duplicate guarantor profileId", 400));
    }
    seenProfiles.add(profileId);

    const pct = Number(g.liabilityPercentage);
    if (!pct || pct < 1 || pct > 100) {
      return next(new AppError("Invalid guarantor liabilityPercentage", 400));
    }
    liabilitySum += pct;
  }

  if (liabilitySum > 100) {
    return next(
      new AppError("Total liabilityPercentage cannot exceed 100", 400),
    );
  }

  if (group) {
    for (const g of memberGuarantors) {
      const membership = await GroupMembershipModel.findOne({
        groupId: group._id,
        userId: g.profileId,
        status: "active",
      });
      if (!membership) {
        return next(
          new AppError(
            "All member guarantors must be active group members",
            400,
          ),
        );
      }
    }
  }

  const loanNumber = await getNextLoanNumber();
  const loanCode = formatLoanCode(loanNumber);

  const application = await LoanApplicationModel.create({
    ...payload,
    userId: req.user.profileId,
    groupId: payload.groupId || null,
    groupName: payload.groupName || null,
    loanNumber,
    loanCode,
    status: "pending",
    remainingBalance: 0,
  });

  const guarantorOps = [];
  for (const g of memberGuarantors) {
    guarantorOps.push({
      loanApplicationId: application._id,
      guarantorUserId: g.profileId,
      guarantorName: g.name,
      guarantorEmail: g.email || null,
      guarantorPhone: g.phone || null,
      liabilityPercentage: Number(g.liabilityPercentage),
      requestMessage: g.requestMessage || null,
      status: "pending",
    });
  }

  const guarantorRecords = guarantorOps.length
    ? await LoanGuarantorModel.insertMany(guarantorOps, { ordered: false })
    : [];

  if (guarantorRecords.length) {
    const notifications = guarantorRecords.map((gr) => ({
      guarantorId: gr._id,
      notificationType: "new_request",
      message: `You have a new guarantor request for loan ${application.loanCode}.`,
      sentVia: [],
      readAt: null,
    }));
    await GuarantorNotificationModel.insertMany(notifications, {
      ordered: false,
    });
  }

  createNotification({
    userId: req.user.profileId,
    title: "Loan application received",
    message: `Your loan application ${application.loanCode} has been received and is pending review.`,
    type: "loan_application",
    metadata: {
      loanId: application._id,
      loanCode: application.loanCode,
      status: application.status,
    },
  }).catch((err) => {
    // eslint-disable-next-line no-console
    console.error("Failed to create loan application notification", err);
  });

  return sendSuccess(res, {
    statusCode: 201,
    data: {
      application,
      guarantorRequests: guarantorRecords,
    },
  });
});

export const listMyLoanApplications = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const apps = await LoanApplicationModel.find({
    userId: req.user.profileId,
  }).sort({ createdAt: -1 });
  return sendSuccess(res, {
    statusCode: 200,
    results: apps.length,
    data: { applications: apps },
  });
});

export const listLoanApplications = catchAsync(async (req, res) => {
  const filter = {};

  if (typeof req.query?.status === "string" && req.query.status.trim()) {
    const status = req.query.status.trim();
    if (LoanApplicationStatuses.includes(status)) filter.status = status;
  }

  const search =
    typeof req.query?.search === "string" ? req.query.search.trim() : "";
  if (search) {
    filter.$or = [
      { loanCode: { $regex: search, $options: "i" } },
      { groupName: { $regex: search, $options: "i" } },
      { loanPurpose: { $regex: search, $options: "i" } },
    ];
  }

  const page = Math.max(1, parseInt(String(req.query?.page ?? "1"), 10) || 1);
  const limit = Math.min(
    200,
    Math.max(1, parseInt(String(req.query?.limit ?? "50"), 10) || 50),
  );
  const skip = (page - 1) * limit;

  const [applications, total] = await Promise.all([
    LoanApplicationModel.find(filter)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limit),
    LoanApplicationModel.countDocuments(filter),
  ]);

  return sendSuccess(res, {
    statusCode: 200,
    results: applications.length,
    total,
    page,
    limit,
    data: { applications },
  });
});

export const getLoanApplication = catchAsync(async (req, res, next) => {
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  const [guarantors, schedule] = await Promise.all([
    LoanGuarantorModel.find({
      loanApplicationId: req.loanApplication._id,
    }).sort({ createdAt: -1 }),
    LoanRepaymentScheduleItemModel.find({
      loanApplicationId: req.loanApplication._id,
    }).sort({
      installmentNumber: 1,
    }),
  ]);

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      application: req.loanApplication,
      guarantors,
      schedule,
    },
  });
});

export const reviewLoanApplication = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  const { status, reviewNotes, approvedAmount, approvedInterestRate } =
    req.body || {};

  const allowedStatuses = new Set(["under_review", "approved", "rejected"]);
  if (!status || !allowedStatuses.has(String(status))) {
    return next(new AppError("Invalid review status", 400));
  }

  req.loanApplication.status = status;
  req.loanApplication.reviewNotes =
    reviewNotes ?? req.loanApplication.reviewNotes;
  req.loanApplication.reviewedBy = req.user.profileId;
  req.loanApplication.reviewedAt = new Date();

  if (status === "approved") {
    if (typeof approvedAmount !== "undefined" && approvedAmount !== null) {
      req.loanApplication.approvedAmount = Number(approvedAmount);
    }
    if (
      typeof approvedInterestRate !== "undefined" &&
      approvedInterestRate !== null
    ) {
      req.loanApplication.approvedInterestRate = Number(approvedInterestRate);
    }
    req.loanApplication.approvedAt = new Date();
  }

  await req.loanApplication.save();

  const statusLabel =
    status === "approved"
      ? "approved"
      : status === "rejected"
        ? "rejected"
        : "under review";

  createNotification({
    userId: req.loanApplication.userId,
    title: "Loan application update",
    message: `Your loan application ${req.loanApplication.loanCode} is ${statusLabel}.`,
    type: "loan_status",
    metadata: {
      loanId: req.loanApplication._id,
      loanCode: req.loanApplication.loanCode,
      status: req.loanApplication.status,
    },
  }).catch((err) => {
    // eslint-disable-next-line no-console
    console.error("Failed to create loan status notification", err);
  });

  return sendSuccess(res, {
    statusCode: 200,
    data: { application: req.loanApplication },
  });
});

export const disburseLoan = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  if (req.loanApplication.status !== "approved") {
    return next(
      new AppError(
        "Loan application must be approved before disbursement",
        400,
      ),
    );
  }

  const principal = Number(
    req.loanApplication.approvedAmount ?? req.loanApplication.loanAmount,
  );
  const rate = Number(
    req.loanApplication.approvedInterestRate ??
      req.loanApplication.interestRate ??
      0,
  );
  const termMonths = Number(req.loanApplication.repaymentPeriod);

  const guarantors = await LoanGuarantorModel.find({
    loanApplicationId: req.loanApplication._id,
  });
  if (!guarantors.length) {
    return next(
      new AppError(
        "At least one guarantor is required to disburse this loan",
        400,
      ),
    );
  }

  const liabilityTotal = guarantors.reduce(
    (sum, g) => sum + Number(g.liabilityPercentage || 0),
    0,
  );
  const allAccepted = guarantors.every((g) => g.status === "accepted");

  if (liabilityTotal !== 100) {
    return next(
      new AppError(
        "Guarantor liabilityPercentage must total 100 to disburse this loan",
        400,
      ),
    );
  }
  if (!allAccepted) {
    return next(
      new AppError("All guarantors must accept before disbursement", 400),
    );
  }

  const repaymentStartDate =
    parseDateOrNull(req.body?.repaymentStartDate) || addMonths(new Date(), 1);

  const { items, totalRepayable, monthlyPayment } = buildAmortizedSchedule({
    principal,
    annualRatePct: rate,
    months: termMonths,
    startDate: repaymentStartDate,
  });

  await LoanRepaymentScheduleItemModel.deleteMany({
    loanApplicationId: req.loanApplication._id,
  });
  await LoanRepaymentScheduleItemModel.insertMany(
    items.map((it) => ({
      loanApplicationId: req.loanApplication._id,
      ...it,
    })),
    { ordered: false },
  );

  req.loanApplication.status = "disbursed";
  req.loanApplication.disbursedAt = new Date();
  req.loanApplication.disbursedBy = req.user.profileId;
  req.loanApplication.repaymentStartDate = repaymentStartDate;
  req.loanApplication.monthlyPayment = monthlyPayment;
  req.loanApplication.totalRepayable = totalRepayable;
  req.loanApplication.remainingBalance = totalRepayable;

  await req.loanApplication.save();

  createNotification({
    userId: req.loanApplication.userId,
    title: "Loan disbursed",
    message: `Your loan ${req.loanApplication.loanCode} has been disbursed.`,
    type: "loan_disbursed",
    metadata: {
      loanId: req.loanApplication._id,
      loanCode: req.loanApplication.loanCode,
      amount: principal,
    },
  }).catch((err) => {
    // eslint-disable-next-line no-console
    console.error("Failed to create loan disbursement notification", err);
  });

  return sendSuccess(res, {
    statusCode: 200,
    data: { application: req.loanApplication },
  });
});

export const listLoanSchedule = catchAsync(async (req, res, next) => {
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  const schedule = await LoanRepaymentScheduleItemModel.find({
    loanApplicationId: req.loanApplication._id,
  }).sort({ installmentNumber: 1 });

  return sendSuccess(res, {
    statusCode: 200,
    results: schedule.length,
    data: { schedule },
  });
});

export const recordLoanRepayment = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

  if (!["disbursed", "defaulted"].includes(req.loanApplication.status)) {
    return next(new AppError("Loan is not active", 400));
  }

  const amount = Number(req.body?.amount);
  const reference = String(req.body?.reference || "").trim();
  if (!amount || amount <= 0)
    return next(new AppError("amount is required", 400));
  if (!reference) return next(new AppError("reference is required", 400));

  const nextItem = await LoanRepaymentScheduleItemModel.findOne({
    loanApplicationId: req.loanApplication._id,
    status: { $in: ["pending", "upcoming", "overdue"] },
  }).sort({ installmentNumber: 1 });

  if (!nextItem) {
    return next(new AppError("No pending repayments found", 400));
  }

  if (amount < nextItem.totalAmount) {
    return next(
      new AppError("Repayment amount must cover the next installment", 400),
    );
  }

  const tx = await TransactionModel.create({
    userId: req.loanApplication.userId,
    reference,
    amount: nextItem.totalAmount,
    type: "loan_repayment",
    status: "success",
    description: `Loan repayment for ${req.loanApplication.loanCode}`,
    channel: req.body?.channel || null,
    loanId: req.loanApplication._id,
    loanName: req.loanApplication.loanCode,
    metadata: { installmentNumber: nextItem.installmentNumber },
  });

  nextItem.status = "paid";
  nextItem.paidAt = new Date();
  nextItem.paidAmount = nextItem.totalAmount;
  nextItem.transactionId = tx._id;
  nextItem.reference = reference;
  await nextItem.save();

  const remaining = Math.max(
    0,
    Number(req.loanApplication.remainingBalance || 0) - nextItem.totalAmount,
  );
  req.loanApplication.remainingBalance = remaining;

  const hasMore = await LoanRepaymentScheduleItemModel.exists({
    loanApplicationId: req.loanApplication._id,
    status: { $in: ["pending", "upcoming", "overdue"] },
    _id: { $ne: nextItem._id },
  });

  if (!hasMore && remaining === 0) {
    req.loanApplication.status = "completed";
  } else {
    await LoanRepaymentScheduleItemModel.updateOne(
      {
        loanApplicationId: req.loanApplication._id,
        status: "upcoming",
        installmentNumber: nextItem.installmentNumber + 1,
      },
      { $set: { status: "pending" } },
    );
  }

  await req.loanApplication.save();

  createNotification({
    userId: req.loanApplication.userId,
    title: "Payment received",
    message: `We received your loan repayment for ${req.loanApplication.loanCode}.`,
    type: "payment_received",
    metadata: {
      loanId: req.loanApplication._id,
      loanCode: req.loanApplication.loanCode,
      amount: nextItem.totalAmount,
      reference,
      installmentNumber: nextItem.installmentNumber,
    },
  }).catch((err) => {
    // eslint-disable-next-line no-console
    console.error("Failed to create repayment notification", err);
  });

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      transaction: tx,
      scheduleItem: nextItem,
      application: req.loanApplication,
    },
  });
});
