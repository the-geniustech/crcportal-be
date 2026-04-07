import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { LoanApplicationModel, LoanApplicationStatuses } from "../models/LoanApplication.js";
import { LoanApplicationEditRequestModel } from "../models/LoanApplicationEditRequest.js";
import { LoanGuarantorModel } from "../models/LoanGuarantor.js";
import { GuarantorNotificationModel } from "../models/GuarantorNotification.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { ProfileModel } from "../models/Profile.js";
import { BankAccountModel } from "../models/BankAccount.js";
import { generateLoanApplicationPdfBuffer } from "../services/pdf/loanApplicationPdf.js";
import { sendEmail } from "../services/mail/resendClient.js";
import { createNotification } from "../services/notificationService.js";
import {
  getLoanInterestConfig,
  isInterestRateAllowed,
} from "../utils/loanPolicy.js";
import { hasUserRole } from "../utils/roles.js";

const LOAN_OTP_RESEND_COOLDOWN_MS = (() => {
  const secondsRaw = Number(
    process.env.LOAN_OTP_RESEND_COOLDOWN_SECONDS ||
      process.env.WITHDRAWAL_OTP_RESEND_COOLDOWN_SECONDS,
  );
  if (Number.isFinite(secondsRaw) && secondsRaw > 0) {
    return Math.round(secondsRaw * 1000);
  }
  const msRaw = Number(
    process.env.LOAN_OTP_RESEND_COOLDOWN_MS ||
      process.env.WITHDRAWAL_OTP_RESEND_COOLDOWN_MS,
  );
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

  const memberships = await GroupMembershipModel.find(
    { userId: req.user.profileId, role: "coordinator", status: "active" },
    { groupId: 1 },
  ).lean();

  return memberships.map((m) => String(m.groupId));
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value || "").trim());
}

function parseEmailList(input) {
  if (!input) return [];
  if (Array.isArray(input)) {
    return input.flatMap((item) => parseEmailList(item));
  }
  if (typeof input === "string") {
    return input
      .split(/[,\n;]/g)
      .map((value) => value.trim())
      .filter(Boolean);
  }
  return [];
}

function normalizeBankAccountId(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text ? text : null;
}

function applyDisbursementSnapshot(target, account) {
  if (!target || !account) return;
  target.disbursementBankAccountId = account._id;
  target.disbursementBankName = account.bankName;
  target.disbursementBankCode = account.bankCode || null;
  target.disbursementAccountNumber = account.accountNumber;
  target.disbursementAccountName = account.accountName;
}

function resetLoanForResubmission(loan, { notes } = {}) {
  loan.status = "pending";
  loan.reviewNotes = notes ?? null;
  loan.reviewedBy = null;
  loan.reviewedAt = null;
  loan.approvedAmount = null;
  loan.approvedInterestRate = null;
  loan.approvedAt = null;
  loan.disbursementBankAccountId = null;
  loan.disbursementBankName = null;
  loan.disbursementBankCode = null;
  loan.disbursementAccountNumber = null;
  loan.disbursementAccountName = null;
  loan.disbursedAt = null;
  loan.disbursedBy = null;
  loan.repaymentStartDate = null;
  loan.monthlyPayment = null;
  loan.totalRepayable = null;
  loan.remainingBalance = 0;
  loan.payoutReference = null;
  loan.payoutGateway = null;
  loan.payoutTransferCode = null;
  loan.payoutStatus = null;
  loan.payoutOtpResentAt = null;
}

function buildLoanEmailHtml({ loan, applicant, recipientsLabel }) {
  return `
    <div style="font-family: Arial, sans-serif; color: #111827;">
      <div style="padding: 16px 24px; background: #0f766e; color: #fff;">
        <h2 style="margin: 0;">CRC Loan Application Summary</h2>
        <p style="margin: 4px 0 0;">${loan.loanCode || loan.loanNumber || loan._id || "Loan"}</p>
      </div>
      <div style="padding: 24px;">
        <p>Hello${recipientsLabel ? ` ${recipientsLabel}` : ""},</p>
        <p>Please find attached the latest loan application summary.</p>
        <div style="background: #f9fafb; padding: 16px; border-radius: 12px;">
          <p style="margin: 0 0 6px;"><strong>Applicant:</strong> ${applicant?.fullName || "Member"}</p>
          <p style="margin: 0 0 6px;"><strong>Group:</strong> ${loan.groupName || "-"}</p>
          <p style="margin: 0 0 6px;"><strong>Amount:</strong> NGN ${Number(loan.loanAmount || 0).toLocaleString("en-NG")}</p>
          <p style="margin: 0;"><strong>Status:</strong> ${String(loan.status || "pending").replace(/_/g, " ").toUpperCase()}</p>
        </div>
        <p style="margin-top: 16px; color: #6b7280; font-size: 12px;">If you have any questions, please reply to this email.</p>
      </div>
    </div>
  `;
}

function buildLoanEmailText({ loan, applicant }) {
  return [
    "CRC Loan Application Summary",
    `Loan: ${loan.loanCode || loan.loanNumber || loan._id || "Loan"}`,
    `Applicant: ${applicant?.fullName || "Member"}`,
    `Group: ${loan.groupName || "-"}`,
    `Amount: NGN ${Number(loan.loanAmount || 0).toLocaleString("en-NG")}`,
    `Status: ${String(loan.status || "pending").replace(/_/g, " ").toUpperCase()}`,
    "",
    "The loan application PDF summary is attached.",
  ].join("\n");
}

export const ensureAdminLoanAccess = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (hasUserRole(req.user, "admin")) return next();

  if (!hasUserRole(req.user, "groupCoordinator")) {
    return next(new AppError("Insufficient permissions", 403));
  }

  const loan = req.loanApplication;
  if (!loan) return next(new AppError("Missing loan context", 500));
  if (!loan.groupId) {
    return next(new AppError("Only admins can manage this loan", 403));
  }

  const manageableGroupIds = await getManageableGroupIds(req);
  if (!manageableGroupIds || !manageableGroupIds.includes(String(loan.groupId))) {
    return next(new AppError("You cannot manage loans for this group", 403));
  }

  return next();
});

export const listAdminLoanApplications = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const manageableGroupIds = await getManageableGroupIds(req);

  const filterBase = {};

  if (manageableGroupIds) {
    filterBase.groupId = { $in: manageableGroupIds };
  }

  const groupId =
    typeof req.query?.groupId === "string" ? req.query.groupId.trim() : "";
  if (groupId) {
    if (manageableGroupIds && !manageableGroupIds.includes(groupId)) {
      return next(new AppError("You cannot manage loans for this group", 403));
    }
    filterBase.groupId = groupId;
  }

  const year =
    typeof req.query?.year === "string" && req.query.year.trim()
      ? Number(req.query.year)
      : null;
  const month =
    typeof req.query?.month === "string" && req.query.month.trim()
      ? Number(req.query.month)
      : null;
  if (year || month) {
    const now = new Date();
    const safeYear =
      Number.isFinite(year) && year ? year : now.getFullYear();
    const safeMonth =
      Number.isFinite(month) && month ? Math.min(12, Math.max(1, month)) : null;
    const start = safeMonth
      ? new Date(Date.UTC(safeYear, safeMonth - 1, 1))
      : new Date(Date.UTC(safeYear, 0, 1));
    const end = safeMonth
      ? new Date(Date.UTC(safeYear, safeMonth, 1))
      : new Date(Date.UTC(safeYear + 1, 0, 1));
    filterBase.createdAt = { $gte: start, $lt: end };
  }

  const search =
    typeof req.query?.search === "string" ? req.query.search.trim() : "";
  if (search) {
    const regex = new RegExp(search, "i");
    const profileMatches = await ProfileModel.find(
      {
        $or: [{ fullName: regex }, { email: regex }, { phone: regex }],
      },
      { _id: 1 },
    ).lean();
    const profileIds = profileMatches.map((profile) => profile._id);

    filterBase.$or = [
      { loanCode: { $regex: regex } },
      { groupName: { $regex: regex } },
      { loanPurpose: { $regex: regex } },
    ];
    if (profileIds.length > 0) {
      filterBase.$or.push({ userId: { $in: profileIds } });
    }
  }

  const filter = { ...filterBase };

  if (typeof req.query?.status === "string" && req.query.status.trim()) {
    const status = String(req.query.status).trim();
    if (LoanApplicationStatuses.includes(status)) filter.status = status;
  }
  if (!filter.status) {
    filter.status = { $ne: "draft" };
  }

  const page = Math.max(1, parseInt(String(req.query?.page ?? "1"), 10) || 1);
  const limit = Math.min(200, Math.max(1, parseInt(String(req.query?.limit ?? "50"), 10) || 50));
  const skip = (page - 1) * limit;

  const [applications, total, summaryAgg] = await Promise.all([
    LoanApplicationModel.find(filter)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limit)
      .lean(),
    LoanApplicationModel.countDocuments(filter),
    LoanApplicationModel.aggregate([
      {
        $match: {
          ...filterBase,
          status:
            req.query?.status === "draft" ? "draft" : { $ne: "draft" },
        },
      },
      {
        $group: {
          _id: "$status",
          count: { $sum: 1 },
          totalRequested: { $sum: "$loanAmount" },
        },
      },
    ]),
  ]);

  const profileIds = applications.map((a) => a.userId).filter(Boolean);
  const profiles = await ProfileModel.find(
    { _id: { $in: profileIds } },
    { fullName: 1, email: 1, phone: 1 },
  ).lean();
  const profileById = new Map(profiles.map((p) => [String(p._id), p]));

  const editRequests = await LoanApplicationEditRequestModel.find({
    loanApplicationId: { $in: applications.map((a) => a._id) },
  })
    .sort({ requestedAt: -1 })
    .lean();
  const latestEditMap = new Map();
  for (const request of editRequests) {
    const key = String(request.loanApplicationId);
    if (!latestEditMap.has(key)) {
      latestEditMap.set(key, request);
    }
  }

  const summaryByStatus = summaryAgg.reduce((acc, row) => {
    acc[String(row._id)] = {
      count: Number(row.count || 0),
      totalRequested: Number(row.totalRequested || 0),
    };
    return acc;
  }, {});

  const enriched = applications.map((a) => {
    const latest = latestEditMap.get(String(a._id));
    return {
      ...a,
      applicant: profileById.get(String(a.userId)) || null,
      latestEditRequest: latest
        ? {
            id: latest._id,
            status: latest.status,
            requestedAt: latest.requestedAt,
            reviewedAt: latest.reviewedAt,
            reviewNotes: latest.reviewNotes ?? null,
            changes: latest.changes ?? [],
            documents: Array.isArray(latest.payload?.documents)
              ? latest.payload.documents.map((doc) => ({
                  name: doc.name,
                  type: doc.type,
                  size: doc.size,
                  status: doc.status ?? "uploaded",
                  url: doc.url ?? null,
                }))
              : [],
          }
        : null,
    };
  });

  const pendingCount = summaryByStatus.pending?.count ?? 0;
  const underReviewCount = summaryByStatus.under_review?.count ?? 0;
  const approvedCount =
    (summaryByStatus.approved?.count ?? 0) +
    (summaryByStatus.disbursed?.count ?? 0);
  const totalRequested =
    (summaryByStatus.pending?.totalRequested ?? 0) +
    (summaryByStatus.under_review?.totalRequested ?? 0);

  return sendSuccess(res, {
    statusCode: 200,
    results: enriched.length,
    total,
    page,
    limit,
    data: {
      applications: enriched,
      summary: {
        pendingCount,
        underReviewCount,
        approvedCount,
        totalRequested,
      },
      otpResendCooldownSeconds:
        LOAN_OTP_RESEND_COOLDOWN_MS > 0
          ? Math.ceil(LOAN_OTP_RESEND_COOLDOWN_MS / 1000)
          : 0,
    },
  });
});

export const reviewAdminLoanApplication = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const { applicationId } = req.params;
  const app = await LoanApplicationModel.findById(applicationId);
  if (!app) return next(new AppError("Loan application not found", 404));

  if (!hasUserRole(req.user, "admin")) {
    if (!app.groupId) return next(new AppError("Only admins can review this loan", 403));

    const manageableGroupIds = await getManageableGroupIds(req);
    if (!manageableGroupIds || !manageableGroupIds.includes(String(app.groupId))) {
      return next(new AppError("You cannot review loans for this group", 403));
    }
  }

  const { status, reviewNotes, approvedAmount, approvedInterestRate } = req.body || {};

  const allowedStatuses = new Set(["under_review", "approved", "rejected"]);
  if (!status || !allowedStatuses.has(String(status))) {
    return next(new AppError("Invalid review status", 400));
  }

  app.status = status;
  app.reviewNotes = reviewNotes ?? app.reviewNotes;
  app.reviewedBy = req.user.profileId;
  app.reviewedAt = new Date();

  if (status === "approved") {
    if (typeof approvedAmount !== "undefined" && approvedAmount !== null) {
      app.approvedAmount = Number(approvedAmount);
    }
    if (typeof approvedInterestRate !== "undefined" && approvedInterestRate !== null) {
      if (!isInterestRateAllowed(app.loanType || "revolving", approvedInterestRate)) {
        return next(new AppError("approvedInterestRate is not allowed for this loan type", 400));
      }
      app.approvedInterestRate = Number(approvedInterestRate);
    }
    if (!app.interestRateType) {
      const cfg = getLoanInterestConfig(app.loanType || "revolving");
      app.interestRateType = cfg.rateType;
    }
    app.approvedAt = new Date();
  }

  await app.save();

  return sendSuccess(res, { statusCode: 200, data: { application: app } });
});

export const reconcileAdminLoanApplication = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));
  if (!req.loanApplication) return next(new AppError("Missing loan context", 500));

  const loan = req.loanApplication;
  if (loan.status !== "rejected") {
    return next(new AppError("Only rejected loans can be reconciled", 400));
  }

  resetLoanForResubmission(loan, {
    notes:
      typeof req.body?.notes === "string" && req.body.notes.trim()
        ? req.body.notes.trim()
        : null,
  });

  await loan.save();

  return sendSuccess(res, {
    statusCode: 200,
    message: "Loan reconciled and returned to pending review",
    data: { application: loan },
  });
});

export const reviewLoanEditRequest = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));
  if (!req.loanApplication) return next(new AppError("Missing loan context", 500));

  const { requestId } = req.params;
  const decision = String(req.body?.status || "").trim();
  if (!["approved", "rejected"].includes(decision)) {
    return next(new AppError("Invalid edit request status", 400));
  }

  const editRequest = await LoanApplicationEditRequestModel.findOne({
    _id: requestId,
    loanApplicationId: req.loanApplication._id,
  });
  if (!editRequest) {
    return next(new AppError("Edit request not found", 404));
  }
  if (editRequest.status !== "pending") {
    return next(new AppError("Edit request has already been reviewed", 400));
  }

  if (decision === "approved") {
    const payload = editRequest.payload || {};
    const updatePayload = { ...payload };

    if (Object.prototype.hasOwnProperty.call(updatePayload, "loanAmount")) {
      const amount = Number(updatePayload.loanAmount);
      if (!amount || amount <= 0) {
        return next(new AppError("loanAmount must be greater than 0", 400));
      }
    }
    if (Object.prototype.hasOwnProperty.call(updatePayload, "repaymentPeriod")) {
      const period = Number(updatePayload.repaymentPeriod);
      if (!period || period <= 0) {
        return next(new AppError("repaymentPeriod must be greater than 0", 400));
      }
    }

    const hasGuarantorUpdate = Object.prototype.hasOwnProperty.call(
      updatePayload,
      "guarantors",
    );
    const hasBankUpdate = Object.prototype.hasOwnProperty.call(
      updatePayload,
      "bankAccountId",
    );
    const normalizedBankId = hasBankUpdate
      ? normalizeBankAccountId(updatePayload.bankAccountId)
      : null;
    let bankAccount = null;

    if (hasBankUpdate) {
      if (!normalizedBankId) {
        return next(
          new AppError(
            "bankAccountId is required to update disbursement details",
            400,
          ),
        );
      }
      bankAccount = await BankAccountModel.findOne({
        _id: normalizedBankId,
        userId: req.loanApplication.userId,
      });
      if (!bankAccount) {
        return next(new AppError("Bank account not found", 404));
      }
    }

    const existingDisbursement = {
      disbursementBankAccountId: req.loanApplication.disbursementBankAccountId,
      disbursementBankName: req.loanApplication.disbursementBankName,
      disbursementBankCode: req.loanApplication.disbursementBankCode,
      disbursementAccountNumber: req.loanApplication.disbursementAccountNumber,
      disbursementAccountName: req.loanApplication.disbursementAccountName,
    };

    delete updatePayload.bankAccountId;
    delete updatePayload.disbursementBankAccountId;
    delete updatePayload.disbursementBankName;
    delete updatePayload.disbursementBankCode;
    delete updatePayload.disbursementAccountNumber;
    delete updatePayload.disbursementAccountName;

    resetLoanForResubmission(req.loanApplication);
    req.loanApplication.set(updatePayload);
    if (hasBankUpdate && bankAccount) {
      applyDisbursementSnapshot(req.loanApplication, bankAccount);
    } else {
      req.loanApplication.disbursementBankAccountId =
        existingDisbursement.disbursementBankAccountId ?? null;
      req.loanApplication.disbursementBankName =
        existingDisbursement.disbursementBankName ?? null;
      req.loanApplication.disbursementBankCode =
        existingDisbursement.disbursementBankCode ?? null;
      req.loanApplication.disbursementAccountNumber =
        existingDisbursement.disbursementAccountNumber ?? null;
      req.loanApplication.disbursementAccountName =
        existingDisbursement.disbursementAccountName ?? null;
    }
    req.loanApplication.status = "pending";
    await req.loanApplication.save();

    if (hasGuarantorUpdate) {
      await LoanGuarantorModel.deleteMany({
        loanApplicationId: req.loanApplication._id,
      });

      const guarantors = Array.isArray(updatePayload.guarantors)
        ? updatePayload.guarantors
        : [];
      const memberGuarantors = guarantors.filter(
        (g) => g && g.type === "member" && g.profileId,
      );
      const guarantorOps = memberGuarantors.map((g) => ({
        loanApplicationId: req.loanApplication._id,
        guarantorUserId: g.profileId,
        guarantorName: g.name,
        guarantorEmail: g.email || null,
        guarantorPhone: g.phone || null,
        liabilityPercentage: Number(g.liabilityPercentage),
        requestMessage: g.requestMessage || null,
        status: "pending",
      }));

      const guarantorRecords = guarantorOps.length
        ? await LoanGuarantorModel.insertMany(guarantorOps, { ordered: false })
        : [];

      if (guarantorRecords.length) {
        const notifications = guarantorRecords.map((gr) => ({
          guarantorId: gr._id,
          notificationType: "new_request",
          message: `You have a new guarantor request for loan ${req.loanApplication.loanCode}.`,
          sentVia: [],
          readAt: null,
        }));
        await GuarantorNotificationModel.insertMany(notifications, {
          ordered: false,
        });
      }
    }
  }

  editRequest.status = decision;
  editRequest.reviewNotes =
    typeof req.body?.reviewNotes === "string" && req.body.reviewNotes.trim()
      ? req.body.reviewNotes.trim()
      : null;
  editRequest.reviewedAt = new Date();
  editRequest.reviewedBy = req.user.profileId;
  await editRequest.save();

  const reviewLabel = decision === "approved" ? "approved" : "rejected";
  const note =
    editRequest.reviewNotes && editRequest.reviewNotes.trim()
      ? ` Note: ${editRequest.reviewNotes.trim()}`
      : "";
  createNotification({
    userId: req.loanApplication.userId,
    title: `Loan edit request ${reviewLabel}`,
    message:
      decision === "approved"
        ? `Your loan edit request has been approved and the application is back in review.${note}`
        : `Your loan edit request has been rejected.${note}`,
    type: "loan_edit_request",
    metadata: {
      loanId: req.loanApplication._id,
      requestId: editRequest._id,
      status: editRequest.status,
    },
  }).catch((err) => {
    // eslint-disable-next-line no-console
    console.error("Failed to create edit request notification", err);
  });

  return sendSuccess(res, {
    statusCode: 200,
    message:
      decision === "approved"
        ? "Edit request approved and applied"
        : "Edit request rejected",
    data: {
      editRequest,
      application: req.loanApplication,
    },
  });
});

export const exportAdminLoanApplications = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const manageableGroupIds = await getManageableGroupIds(req);
  const filterBase = {};

  if (manageableGroupIds) {
    filterBase.groupId = { $in: manageableGroupIds };
  }

  const groupId =
    typeof req.query?.groupId === "string" ? req.query.groupId.trim() : "";
  if (groupId) {
    if (manageableGroupIds && !manageableGroupIds.includes(groupId)) {
      return next(new AppError("You cannot manage loans for this group", 403));
    }
    filterBase.groupId = groupId;
  }

  const year =
    typeof req.query?.year === "string" && req.query.year.trim()
      ? Number(req.query.year)
      : null;
  const month =
    typeof req.query?.month === "string" && req.query.month.trim()
      ? Number(req.query.month)
      : null;
  if (year || month) {
    const now = new Date();
    const safeYear =
      Number.isFinite(year) && year ? year : now.getFullYear();
    const safeMonth =
      Number.isFinite(month) && month ? Math.min(12, Math.max(1, month)) : null;
    const start = safeMonth
      ? new Date(Date.UTC(safeYear, safeMonth - 1, 1))
      : new Date(Date.UTC(safeYear, 0, 1));
    const end = safeMonth
      ? new Date(Date.UTC(safeYear, safeMonth, 1))
      : new Date(Date.UTC(safeYear + 1, 0, 1));
    filterBase.createdAt = { $gte: start, $lt: end };
  }

  const search =
    typeof req.query?.search === "string" ? req.query.search.trim() : "";
  if (search) {
    const regex = new RegExp(search, "i");
    const profileMatches = await ProfileModel.find(
      {
        $or: [{ fullName: regex }, { email: regex }, { phone: regex }],
      },
      { _id: 1 },
    ).lean();
    const profileIds = profileMatches.map((profile) => profile._id);

    filterBase.$or = [
      { loanCode: { $regex: regex } },
      { groupName: { $regex: regex } },
      { loanPurpose: { $regex: regex } },
    ];
    if (profileIds.length > 0) {
      filterBase.$or.push({ userId: { $in: profileIds } });
    }
  }

  const filter = { ...filterBase };
  if (typeof req.query?.status === "string" && req.query.status.trim()) {
    const status = String(req.query.status).trim();
    if (LoanApplicationStatuses.includes(status)) filter.status = status;
  }
  if (!filter.status) {
    filter.status = { $ne: "draft" };
  }

  const applications = await LoanApplicationModel.find(filter)
    .sort({ createdAt: -1 })
    .lean();

  const profileIds = applications.map((a) => a.userId).filter(Boolean);
  const profiles = await ProfileModel.find(
    { _id: { $in: profileIds } },
    { fullName: 1, email: 1, phone: 1 },
  ).lean();
  const profileById = new Map(profiles.map((p) => [String(p._id), p]));

  const headers = [
    "Loan Code",
    "Status",
    "Applicant",
    "Applicant Email",
    "Applicant Phone",
    "Group",
    "Loan Type",
    "Amount",
    "Repayment Term (Months)",
    "Interest Rate",
    "Created At",
    "Approved At",
    "Disbursed At",
  ];

  const formatDate = (value) => {
    if (!value) return "";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return "";
    return date.toISOString().slice(0, 10);
  };
  const csvEscape = (value) => {
    const raw = String(value ?? "");
    if (/[",\n]/.test(raw)) {
      return `"${raw.replace(/"/g, '""')}"`;
    }
    return raw;
  };

  const rows = applications.map((loan) => {
    const applicant = profileById.get(String(loan.userId));
    return [
      loan.loanCode || loan.loanNumber || loan._id || "",
      String(loan.status || "").replace(/_/g, " "),
      applicant?.fullName || "",
      applicant?.email || "",
      applicant?.phone || "",
      loan.groupName || "",
      loan.loanType || "",
      Number(loan.loanAmount || 0),
      Number(loan.repaymentPeriod || 0),
      loan.interestRate != null ? Number(loan.interestRate) : "",
      formatDate(loan.createdAt),
      formatDate(loan.approvedAt),
      formatDate(loan.disbursedAt),
    ];
  });

  const csvBody = [headers, ...rows]
    .map((row) => row.map((value) => csvEscape(value)).join(","))
    .join("\n");
  const csv = `\uFEFF${csvBody}`;

  const label = new Date().toISOString().slice(0, 10);
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename=\"loan-applications-${label}.csv\"`,
  );
  res.status(200).send(csv);
});

export const downloadAdminLoanApplicationPdf = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.loanApplication) {
    return next(new AppError("Missing loan context", 500));
  }

  const loanDoc = req.loanApplication;
  const applicant = loanDoc.userId
    ? await ProfileModel.findById(loanDoc.userId, {
        fullName: 1,
        email: 1,
        phone: 1,
      }).lean()
    : null;

  const loan = typeof loanDoc.toObject === "function" ? loanDoc.toObject() : loanDoc;
  const pdfBuffer = await generateLoanApplicationPdfBuffer({
    loan,
    applicant,
    organization: {
      name: "Cooperative Resource Center",
      subtitle: "Loan Processing Desk",
    },
  });

  const reference = loan.loanCode || loan.loanNumber || loan._id || "loan";
  const safeRef = String(reference).toLowerCase().replace(/[^a-z0-9]+/g, "-");

  res.setHeader("Content-Type", "application/pdf");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename=\"loan-application-${safeRef}.pdf\"`,
  );
  res.status(200).send(pdfBuffer);
});

export const emailAdminLoanApplicationPdf = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.loanApplication) {
    return next(new AppError("Missing loan context", 500));
  }

  const sendApplicant =
    typeof req.body?.sendApplicant === "boolean"
      ? req.body.sendApplicant
      : true;
  const sendGuarantors =
    typeof req.body?.sendGuarantors === "boolean"
      ? req.body.sendGuarantors
      : true;
  const extraEmails = parseEmailList(req.body?.extraEmails || req.body?.emails);

  const loanDoc = req.loanApplication;
  const applicant = loanDoc.userId
    ? await ProfileModel.findById(loanDoc.userId, {
        fullName: 1,
        email: 1,
        phone: 1,
      }).lean()
    : null;

  const recipients = [];

  if (sendApplicant && applicant?.email) {
    recipients.push(applicant.email);
  }

  if (sendGuarantors && Array.isArray(loanDoc.guarantors)) {
    loanDoc.guarantors.forEach((g) => {
      if (g?.email) recipients.push(g.email);
    });
  }

  recipients.push(...extraEmails);

  const uniqueEmails = Array.from(
    new Set(
      recipients
        .map((value) => normalizeEmail(value))
        .filter((value) => value && isValidEmail(value)),
    ),
  );

  if (uniqueEmails.length === 0) {
    return next(new AppError("No valid email recipients found", 400));
  }

  if (uniqueEmails.length > 10) {
    return next(new AppError("Too many email recipients. Maximum is 10.", 400));
  }

  const loan = typeof loanDoc.toObject === "function" ? loanDoc.toObject() : loanDoc;
  const pdfBuffer = await generateLoanApplicationPdfBuffer({
    loan,
    applicant,
    organization: {
      name: "Cooperative Resource Center",
      subtitle: "Loan Processing Desk",
    },
  });

  const reference = loan.loanCode || loan.loanNumber || loan._id || "loan";
  const subject = `Loan Application Summary - ${reference}`;

  await sendEmail({
    to: uniqueEmails,
    subject,
    html: buildLoanEmailHtml({
      loan,
      applicant,
      recipientsLabel: "",
    }),
    text: buildLoanEmailText({ loan, applicant }),
    attachments: [
      {
        filename: `loan-application-${String(reference).toLowerCase()}.pdf`,
        content: pdfBuffer.toString("base64"),
        contentType: "application/pdf",
      },
    ],
  });

  return sendSuccess(res, {
    statusCode: 200,
    message: "Loan summary email queued",
    data: { ok: true, recipients: uniqueEmails },
  });
});
