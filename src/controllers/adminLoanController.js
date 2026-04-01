import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { LoanApplicationModel, LoanApplicationStatuses } from "../models/LoanApplication.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { ProfileModel } from "../models/Profile.js";
import { generateLoanApplicationPdfBuffer } from "../services/pdf/loanApplicationPdf.js";
import { sendEmail } from "../services/mail/resendClient.js";
import {
  getLoanInterestConfig,
  isInterestRateAllowed,
} from "../utils/loanPolicy.js";

async function getManageableGroupIds(req) {
  if (!req.user) throw new AppError("Not authenticated", 401);
  if (!req.user.profileId) throw new AppError("User profile not found", 400);

  if (req.user.role === "admin") return null;

  if (req.user.role !== "groupCoordinator") {
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
  if (req.user.role === "admin") return next();

  if (req.user.role !== "groupCoordinator") {
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

  const filter = {};

  if (manageableGroupIds) {
    filter.groupId = { $in: manageableGroupIds };
  }

  if (typeof req.query?.status === "string" && req.query.status.trim()) {
    const status = String(req.query.status).trim();
    if (LoanApplicationStatuses.includes(status)) filter.status = status;
  }

  const search = typeof req.query?.search === "string" ? req.query.search.trim() : "";
  if (search) {
    filter.$or = [
      { loanCode: { $regex: search, $options: "i" } },
      { groupName: { $regex: search, $options: "i" } },
      { loanPurpose: { $regex: search, $options: "i" } },
    ];
  }

  const page = Math.max(1, parseInt(String(req.query?.page ?? "1"), 10) || 1);
  const limit = Math.min(200, Math.max(1, parseInt(String(req.query?.limit ?? "50"), 10) || 50));
  const skip = (page - 1) * limit;

  const [applications, total] = await Promise.all([
    LoanApplicationModel.find(filter).sort({ createdAt: -1 }).skip(skip).limit(limit).lean(),
    LoanApplicationModel.countDocuments(filter),
  ]);

  const profileIds = applications.map((a) => a.userId).filter(Boolean);
  const profiles = await ProfileModel.find(
    { _id: { $in: profileIds } },
    { fullName: 1, email: 1, phone: 1 },
  ).lean();
  const profileById = new Map(profiles.map((p) => [String(p._id), p]));

  const enriched = applications.map((a) => ({
    ...a,
    applicant: profileById.get(String(a.userId)) || null,
  }));

  return sendSuccess(res, {
    statusCode: 200,
    results: enriched.length,
    total,
    page,
    limit,
    data: { applications: enriched },
  });
});

export const reviewAdminLoanApplication = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const { applicationId } = req.params;
  const app = await LoanApplicationModel.findById(applicationId);
  if (!app) return next(new AppError("Loan application not found", 404));

  if (req.user.role !== "admin") {
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
