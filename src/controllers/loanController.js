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
import {
  ContributionWindow,
  LoanFacilityTypes,
  getLoanFacility,
  getLoanInterestConfig,
  getLoanRepaymentDeadline,
  isInterestRateAllowed,
  isLoanFacilityAvailable,
  resolveInterestRate,
} from "../utils/loanPolicy.js";

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

function withRepaymentToDate(loan) {
  const plain = loan && typeof loan.toObject === "function" ? loan.toObject() : loan;
  if (!plain) return plain;
  const totalRepayable = Number(plain.totalRepayable ?? 0);
  const remainingBalance = Number(plain.remainingBalance ?? 0);
  const repaymentToDate =
    Number.isFinite(totalRepayable) &&
    totalRepayable > 0 &&
    Number.isFinite(remainingBalance)
      ? Math.max(0, totalRepayable - remainingBalance)
      : null;
  return { ...plain, repaymentToDate };
}

async function buildNextPaymentMap(apps) {
  if (!Array.isArray(apps) || apps.length === 0) return new Map();
  const ids = apps.map((app) => app._id);
  const scheduleItems = await LoanRepaymentScheduleItemModel.find({
    loanApplicationId: { $in: ids },
    status: { $in: ["pending", "upcoming", "overdue"] },
  })
    .sort({ dueDate: 1 })
    .lean();

  const map = new Map();
  for (const item of scheduleItems) {
    const key = String(item.loanApplicationId);
    if (!map.has(key)) {
      map.set(key, item);
    }
  }
  return map;
}

function buildRepaymentSchedule({
  principal,
  ratePct,
  rateType,
  months,
  startDate,
}) {
  const P = Number(principal);
  const n = Math.max(1, Number(months) | 0);
  const rate = Math.max(0, Number(ratePct) || 0);
  const normalizedType = rateType || "annual";

  const items = [];

  if (normalizedType === "total") {
    const totalInterest = Math.round(P * (rate / 100));
    const basePayment = (P + totalInterest) / n;
    let remainingPrincipal = P;
    let remainingInterest = totalInterest;

    for (let i = 1; i <= n; i += 1) {
      const interest =
        i === n
          ? remainingInterest
          : Math.round(totalInterest / n);
      const principalPaid =
        i === n
          ? remainingPrincipal
          : Math.round(basePayment - interest);
      const total = principalPaid + interest;

      remainingPrincipal = Math.max(0, remainingPrincipal - principalPaid);
      remainingInterest = Math.max(0, remainingInterest - interest);

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
    const monthlyPayment = items[0]?.totalAmount ?? Math.round(basePayment);

    return { items, totalRepayable, monthlyPayment };
  }

  const monthlyRate =
    normalizedType === "monthly" ? rate / 100 : rate / 100 / 12;

  const payment =
    monthlyRate === 0
      ? P / n
      : (P * monthlyRate * Math.pow(1 + monthlyRate, n)) /
        (Math.pow(1 + monthlyRate, n) - 1);

  let balance = P;

  for (let i = 1; i <= n; i += 1) {
    const interest = monthlyRate === 0 ? 0 : balance * monthlyRate;
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
    await ProfileModel.findById(profileId).select("membershipStatus email phone");
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

  const [
    groupsJoined,
    contributionAgg,
    previousLoans,
    defaultedLoans,
    overdueContributions,
    activeLoans,
  ] = await Promise.all([
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
    ContributionModel.countDocuments({
      userId: req.user.profileId,
      status: "overdue",
    }),
    LoanApplicationModel.find(
      {
        userId: req.user.profileId,
        status: { $in: ["disbursed", "defaulted"] },
      },
      { _id: 1 },
    ).lean(),
  ]);

  const totalContributions = Number(contributionAgg?.[0]?.sum ?? 0);
  const activeLoanIds = activeLoans.map((l) => l._id);
  const overdueRepayments =
    activeLoanIds.length === 0
      ? 0
      : await LoanRepaymentScheduleItemModel.countDocuments({
          loanApplicationId: { $in: activeLoanIds },
          $or: [
            { status: "overdue" },
            {
              status: { $in: ["pending", "upcoming"] },
              dueDate: { $lt: now },
            },
          ],
        });

  const eligibility = {
    savingsBalance: totalContributions,
    totalContributions,
    membershipDuration,
    groupsJoined,
    attendanceRate: 92,
    contributionStreak: 12,
    previousLoans,
    defaultedLoans,
    overdueContributions,
    overdueRepayments,
    creditScore: 850,
    contributionWindow: {
      startDay: ContributionWindow.startDay,
      endDay: ContributionWindow.endDay,
      isOpen: now.getDate() >= ContributionWindow.startDay || now.getDate() <= ContributionWindow.endDay,
    },
  };

  return sendSuccess(res, { statusCode: 200, data: { eligibility } });
});

export const createLoanApplication = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  let borrowerProfile = null;
  if (req.user.role !== "admin") {
    borrowerProfile = await ensureActiveMember(req.user.profileId);
  } else if (req.user.profileId) {
    borrowerProfile = await ProfileModel.findById(req.user.profileId).select(
      "email phone",
    );
  }

  const allowed = [
    "groupId",
    "groupName",
    "loanType",
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

  const loanTypeRaw = String(payload.loanType || "").trim().toLowerCase();
  if (!loanTypeRaw) {
    return next(new AppError("loanType is required", 400));
  }
  if (!LoanFacilityTypes.includes(loanTypeRaw)) {
    return next(new AppError("Invalid loanType", 400));
  }
  if (!isLoanFacilityAvailable(loanTypeRaw, new Date())) {
    const facility = getLoanFacility(loanTypeRaw);
    const label = facility?.label || "This loan type";
    return next(new AppError(`${label} is not available at this time`, 400));
  }
  payload.loanType = loanTypeRaw;

  if (req.user.role !== "admin" && !payload.groupId) {
    return next(
      new AppError("groupId is required for member loan applications", 400),
    );
  }

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

  const interestConfig = getLoanInterestConfig(payload.loanType);
  if (
    interestConfig.termMonths &&
    Number(payload.repaymentPeriod) !== Number(interestConfig.termMonths)
  ) {
    return next(
      new AppError(
        `repaymentPeriod must be ${interestConfig.termMonths} months for this loan type`,
        400,
      ),
    );
  }

  if (
    typeof payload.interestRate !== "undefined" &&
    payload.interestRate !== null &&
    !isInterestRateAllowed(payload.loanType, payload.interestRate)
  ) {
    return next(new AppError("interestRate is not allowed for this loan type", 400));
  }

  const resolvedInterest = resolveInterestRate(
    payload.loanType,
    payload.interestRate,
  );
  payload.interestRate = resolvedInterest.rate;
  payload.interestRateType = resolvedInterest.rateType;

  const now = new Date();
  const [contributionAgg, overdueContribs, defaultedLoans, activeLoans] =
    await Promise.all([
      ContributionModel.aggregate([
        {
          $match: {
            userId: req.user.profileId,
            status: { $in: ["completed", "verified"] },
          },
        },
        { $group: { _id: null, sum: { $sum: "$amount" } } },
      ]),
      ContributionModel.countDocuments({
        userId: req.user.profileId,
        status: "overdue",
      }),
      LoanApplicationModel.countDocuments({
        userId: req.user.profileId,
        status: "defaulted",
      }),
      LoanApplicationModel.find(
        {
          userId: req.user.profileId,
          status: { $in: ["disbursed", "defaulted"] },
        },
        { _id: 1 },
      ).lean(),
    ]);

  const totalContributions = Number(contributionAgg?.[0]?.sum ?? 0);

  if (
    payload.loanType === "revolving" &&
    Number(payload.loanAmount) > totalContributions
  ) {
    return next(
      new AppError(
        "Revolving loans cannot exceed your total contributions",
        400,
      ),
    );
  }

  if (overdueContribs > 0) {
    return next(
      new AppError(
        `Outstanding contributions detected. Contributions must be paid between ${ContributionWindow.startDay}th and ${ContributionWindow.endDay}th.`,
        400,
      ),
    );
  }

  if (defaultedLoans > 0) {
    return next(new AppError("Defaulted loans must be resolved first", 400));
  }

  const activeLoanIds = activeLoans.map((l) => l._id);
  if (activeLoanIds.length > 0) {
    const overdueScheduleCount =
      await LoanRepaymentScheduleItemModel.countDocuments({
        loanApplicationId: { $in: activeLoanIds },
        $or: [
          { status: "overdue" },
          {
            status: { $in: ["pending", "upcoming"] },
            dueDate: { $lt: now },
          },
        ],
      });

    if (overdueScheduleCount > 0) {
      return next(
        new AppError(
          "Loan repayments are overdue. Please clear overdue installments before applying.",
          400,
        ),
      );
    }
  }

  const guarantors = Array.isArray(payload.guarantors)
    ? payload.guarantors
    : [];
  const memberGuarantors = guarantors.filter((g) => g && g.type === "member");
  const externalGuarantors = guarantors.filter((g) => g && g.type === "external");
  const borrowerProfileId = String(req.user.profileId);

  const normalizeEmail = (value) => String(value || "").trim().toLowerCase();
  const normalizePhone = (value) =>
    String(value || "")
      .replace(/\s+/g, "")
      .replace(/[^0-9+]/g, "");
  const borrowerEmail = normalizeEmail(borrowerProfile?.email);
  const borrowerPhone = normalizePhone(borrowerProfile?.phone);

  let liabilitySum = 0;
  const seenProfiles = new Set();
  const seenExternal = new Set();

  for (const g of memberGuarantors) {
    if (!g.profileId) {
      return next(
        new AppError("Member guarantors must include profileId", 400),
      );
    }
    const profileId = String(g.profileId);
    if (profileId === borrowerProfileId) {
      return next(new AppError("Borrower cannot be a guarantor", 400));
    }
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

  for (const g of externalGuarantors) {
    const emailKey = normalizeEmail(g.email);
    const phoneKey = normalizePhone(g.phone);
    if (borrowerEmail && emailKey && emailKey === borrowerEmail) {
      return next(new AppError("Borrower cannot be a guarantor", 400));
    }
    if (borrowerPhone && phoneKey && phoneKey === borrowerPhone) {
      return next(new AppError("Borrower cannot be a guarantor", 400));
    }
    const key =
      emailKey || phoneKey ? `${emailKey}::${phoneKey}` : null;
    if (key && seenExternal.has(key)) {
      return next(new AppError("Duplicate external guarantor", 400));
    }
    if (key) seenExternal.add(key);
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
  })
    .sort({ createdAt: -1 })
    .lean();
  const nextPaymentMap = await buildNextPaymentMap(apps);
  const enriched = apps.map((app) => {
    const base = withRepaymentToDate(app);
    const next = nextPaymentMap.get(String(app._id));
    return {
      ...base,
      nextPaymentDueDate: next?.dueDate ?? null,
      nextPaymentAmount: next?.totalAmount ?? null,
      nextPaymentStatus: next?.status ?? null,
    };
  });
  return sendSuccess(res, {
    statusCode: 200,
    results: enriched.length,
    data: { applications: enriched },
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
      .limit(limit)
      .lean(),
    LoanApplicationModel.countDocuments(filter),
  ]);
  const nextPaymentMap = await buildNextPaymentMap(applications);
  const enriched = applications.map((app) => {
    const base = withRepaymentToDate(app);
    const next = nextPaymentMap.get(String(app._id));
    return {
      ...base,
      nextPaymentDueDate: next?.dueDate ?? null,
      nextPaymentAmount: next?.totalAmount ?? null,
      nextPaymentStatus: next?.status ?? null,
    };
  });

  return sendSuccess(res, {
    statusCode: 200,
    results: enriched.length,
    total,
    page,
    limit,
    data: { applications: enriched },
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
      application: withRepaymentToDate(req.loanApplication),
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
      if (
        !isInterestRateAllowed(
          req.loanApplication.loanType || "revolving",
          approvedInterestRate,
        )
      ) {
        return next(
          new AppError(
            "approvedInterestRate is not allowed for this loan type",
            400,
          ),
        );
      }
      req.loanApplication.approvedInterestRate = Number(approvedInterestRate);
    }
    if (!req.loanApplication.interestRateType) {
      const cfg = getLoanInterestConfig(
        req.loanApplication.loanType || "revolving",
      );
      req.loanApplication.interestRateType = cfg.rateType;
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

  const loanType = req.loanApplication.loanType || "revolving";
  if (!isLoanFacilityAvailable(loanType, new Date())) {
    const facility = getLoanFacility(loanType);
    const label = facility?.label || "This loan type";
    return next(new AppError(`${label} is not available at this time`, 400));
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
  const interestCfg = getLoanInterestConfig(loanType);

  if (
    interestCfg.termMonths &&
    Number(termMonths) !== Number(interestCfg.termMonths)
  ) {
    return next(
      new AppError(
        `repaymentPeriod must be ${interestCfg.termMonths} months for this loan type`,
        400,
      ),
    );
  }

  if (!isInterestRateAllowed(loanType, rate)) {
    return next(
      new AppError("interestRate is not allowed for this loan type", 400),
    );
  }

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

  const rateType =
    req.loanApplication.interestRateType || interestCfg.rateType || "annual";

  const { items, totalRepayable, monthlyPayment } = buildRepaymentSchedule({
    principal,
    ratePct: rate,
    rateType,
    months: termMonths,
    startDate: repaymentStartDate,
  });

  const deadline = getLoanRepaymentDeadline(loanType, repaymentStartDate);
  const lastDueDate = items.length ? items[items.length - 1].dueDate : null;
  if (deadline && lastDueDate && new Date(lastDueDate).getTime() > deadline.getTime()) {
    return next(
      new AppError(
        loanType === "bridging"
          ? "Bridging loans must be fully repaid by January"
          : "Loans must be fully repaid by October",
        400,
      ),
    );
  }

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
  req.loanApplication.interestRateType = rateType;
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
