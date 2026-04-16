import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import {
  LoanApplicationModel,
  LoanApplicationStatuses,
} from "../models/LoanApplication.js";
import { LoanApplicationEditRequestModel } from "../models/LoanApplicationEditRequest.js";
import { LoanGuarantorModel } from "../models/LoanGuarantor.js";
import { GuarantorNotificationModel } from "../models/GuarantorNotification.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { LoanRepaymentScheduleItemModel } from "../models/LoanRepaymentScheduleItem.js";
import { ProfileModel } from "../models/Profile.js";
import { BankAccountModel } from "../models/BankAccount.js";
import { TransactionModel } from "../models/Transaction.js";
import { generateLoanApplicationPdfBuffer } from "../services/pdf/loanApplicationPdf.js";
import { generateLoanRepaymentHistoryPdfBuffer } from "../services/pdf/loanRepaymentHistoryPdf.js";
import { generateReceiptPdfBuffer } from "../services/pdf/receiptPdf.js";
import { sendEmail } from "../services/mail/resendClient.js";
import { createNotification } from "../services/notificationService.js";
import {
  buildReceiptEmailHtml,
  buildReceiptEmailText,
  buildReceiptPayload,
  receiptOrganizationInfo,
} from "../services/receiptService.js";
import {
  applyLoanRepayment,
  buildLoanNextPaymentMap,
  getLoanScheduleOutstandingAmount,
} from "../services/loanRepaymentService.js";
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
  return String(value || "")
    .trim()
    .toLowerCase();
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

function escapeRegex(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function formatUploadedRepaymentReceipt(upload, file = null) {
  if (!upload && !file) return null;

  const uploadName =
    typeof upload?.originalFilename === "string"
      ? upload.originalFilename.trim()
      : "";
  const fileName =
    typeof file?.originalname === "string" ? file.originalname.trim() : "";
  const rawType =
    typeof file?.mimetype === "string" && file.mimetype.trim()
      ? file.mimetype.trim()
      : upload?.resourceType === "raw"
        ? "application/pdf"
        : typeof upload?.format === "string" && upload.format.trim()
          ? `image/${upload.format.trim()}`
          : "application/octet-stream";
  const sizeRaw =
    file && Number.isFinite(Number(file.size))
      ? Number(file.size)
      : Number(upload?.bytes || 0);
  const url = typeof upload?.url === "string" ? upload.url.trim() : "";

  if (!url) return null;

  return {
    name: fileName || uploadName || "receipt",
    type: rawType,
    size: Number.isFinite(sizeRaw) && sizeRaw > 0 ? sizeRaw : 0,
    status: "uploaded",
    url,
    publicId:
      typeof upload?.publicId === "string" && upload.publicId.trim()
        ? upload.publicId.trim()
        : null,
    resourceType:
      typeof upload?.resourceType === "string" && upload.resourceType.trim()
        ? upload.resourceType.trim()
        : null,
    format:
      typeof upload?.format === "string" && upload.format.trim()
        ? upload.format.trim()
        : null,
  };
}

function normalizeManualRepaymentReceipt(input) {
  if (!input || typeof input !== "object") return null;
  return formatUploadedRepaymentReceipt(
    {
      url: input.url,
      publicId: input.publicId,
      resourceType: input.resourceType,
      format: input.format,
      bytes: input.size,
      originalFilename: input.name,
    },
    {
      originalname: input.name,
      mimetype: input.type,
      size: input.size,
    },
  );
}

const ManualLoanPaymentMethods = new Set([
  "bank_transfer",
  "cash",
  "card",
  "pos",
  "mobile_money",
  "cheque",
  "other",
]);

async function buildAdminLoanBaseFilter(req, { includeDraft = false } = {}) {
  const manageableGroupIds = await getManageableGroupIds(req);
  const filterBase = {};

  if (manageableGroupIds) {
    filterBase.groupId = { $in: manageableGroupIds };
  }

  const groupId =
    typeof req.query?.groupId === "string" ? req.query.groupId.trim() : "";
  if (groupId) {
    if (manageableGroupIds && !manageableGroupIds.includes(groupId)) {
      throw new AppError("You cannot manage loans for this group", 403);
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
    const safeYear = Number.isFinite(year) && year ? year : now.getFullYear();
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
    const regex = new RegExp(escapeRegex(search), "i");
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

  if (!includeDraft) {
    filterBase.status = { $ne: "draft" };
  }

  return { manageableGroupIds, filterBase };
}

function normalizeBankAccountId(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text ? text : null;
}

function generateReference(prefix = "CRC-LMAN") {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10).toUpperCase()}`;
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
  loan.manualDisbursement = null;
  loan.manualDisbursementOtpHash = null;
  loan.manualDisbursementOtpExpiresAt = null;
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
          <p style="margin: 0;"><strong>Status:</strong> ${String(
            loan.status || "pending",
          )
            .replace(/_/g, " ")
            .toUpperCase()}</p>
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
    `Status: ${String(loan.status || "pending")
      .replace(/_/g, " ")
      .toUpperCase()}`,
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
  if (
    !manageableGroupIds ||
    !manageableGroupIds.includes(String(loan.groupId))
  ) {
    return next(new AppError("You cannot manage loans for this group", 403));
  }

  return next();
});

function formatRepaymentHistoryCsvValue(value) {
  const raw = String(value ?? "");
  if (/[",\n]/.test(raw)) {
    return `"${raw.replace(/"/g, '""')}"`;
  }
  return raw;
}

function formatRepaymentHistoryDate(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toISOString().replace("T", " ").slice(0, 16);
}

function buildLoanRepaymentHistoryCsv({ loan, summary, repayments }) {
  const header = [
    "Reference",
    "Amount",
    "Status",
    "Manual",
    "Payment Method",
    "Payment Reference",
    "Received At",
    "Recorded At",
    "Recorded By",
    "Settled Installments",
    "Balance After Payment",
    "Allocations",
    "Receipt URL",
    "Notes",
  ];

  const rows = Array.isArray(repayments)
    ? repayments.map((repayment) => [
        repayment.reference || "",
        Number(repayment.amount || 0),
        repayment.status || "",
        repayment.manual ? "Yes" : "No",
        repayment.paymentMethod || repayment.gateway || "",
        repayment.paymentReference || "",
        formatRepaymentHistoryDate(repayment.receivedAt),
        formatRepaymentHistoryDate(repayment.recordedAt),
        repayment.recordedBy?.name || "",
        Number(repayment.settledInstallmentCount || 0),
        repayment.remainingBalanceAfterPayment != null
          ? Number(repayment.remainingBalanceAfterPayment)
          : "",
        Array.isArray(repayment.allocations)
          ? repayment.allocations
              .map(
                (allocation) =>
                  `#${allocation.installmentNumber}:${Number(
                    allocation.appliedAmount || 0,
                  ).toFixed(2)}`,
              )
              .join(" | ")
          : "",
        repayment.receipt?.url || "",
        repayment.notes || "",
      ])
    : [];

  const lines = [
    ["Loan Code", loan?.loanCode || ""],
    ["Borrower", loan?.borrowerName || ""],
    ["Borrower Email", loan?.borrowerEmail || ""],
    ["Borrower Phone", loan?.borrowerPhone || ""],
    ["Loan Type", loan?.loanType || ""],
    ["Group", loan?.groupName || ""],
    ["Status", loan?.loanStatus || ""],
    ["Approved Amount", Number(loan?.approvedAmount || 0)],
    ["Total Repayable", Number(loan?.totalRepayable || 0)],
    ["Remaining Balance", Number(summary?.remainingBalance || 0)],
    ["Repaid So Far", Number(summary?.repaidSoFar || 0)],
    ["Next Payment Amount", Number(summary?.nextPaymentAmount || 0)],
    ["Next Payment Due Date", formatRepaymentHistoryDate(summary?.nextPaymentDueDate)],
    [""],
    header,
    ...rows,
  ];

  return lines
    .map((row) =>
      row.map((cell) => formatRepaymentHistoryCsvValue(cell)).join(","),
    )
    .join("\n");
}

async function findLoanRepaymentTransaction(loanDoc, repaymentId) {
  if (!loanDoc?._id) throw new AppError("Loan application not found", 404);

  const tx = await TransactionModel.findOne({
    _id: repaymentId,
    loanId: loanDoc._id,
    type: "loan_repayment",
    status: "success",
  });

  if (!tx) {
    throw new AppError("Repayment record not found", 404);
  }

  return tx;
}

async function buildAdminLoanRepaymentHistoryData(loanDoc) {
  const loan =
    typeof loanDoc?.toObject === "function" ? loanDoc.toObject() : loanDoc;

  if (!loan?._id) {
    throw new AppError("Loan application not found", 404);
  }

  const [borrower, nextPaymentMap, scheduleItems, transactions] =
    await Promise.all([
      loan.userId
        ? ProfileModel.findById(loan.userId, {
            fullName: 1,
            email: 1,
            phone: 1,
          }).lean()
        : null,
      buildLoanNextPaymentMap([loan]),
      LoanRepaymentScheduleItemModel.find({
        loanApplicationId: loan._id,
      })
        .sort({ installmentNumber: 1 })
        .lean(),
      TransactionModel.find({
        loanId: loan._id,
        type: "loan_repayment",
        status: "success",
      })
        .sort({ date: -1 })
        .lean(),
    ]);

  const recordedByIds = Array.from(
    new Set(
      transactions
        .map((tx) => {
          const metadata =
            tx?.metadata && typeof tx.metadata === "object" ? tx.metadata : null;
          return metadata?.recordedBy ? String(metadata.recordedBy) : null;
        })
        .filter(Boolean),
    ),
  );

  const recordedByProfiles = recordedByIds.length
    ? await ProfileModel.find(
        { _id: { $in: recordedByIds } },
        { fullName: 1, email: 1 },
      ).lean()
    : [];
  const recordedByMap = new Map(
    recordedByProfiles.map((profile) => [String(profile._id), profile]),
  );

  const now = new Date();
  const scheduleSummary = scheduleItems.reduce(
    (acc, item) => {
      const dueAmount = getLoanScheduleOutstandingAmount(item);
      acc.totalInstallments += 1;
      if (dueAmount <= 0) {
        acc.settledInstallments += 1;
      } else {
        const dueDate = item.dueDate ? new Date(item.dueDate) : null;
        if (dueDate && dueDate.getTime() < now.getTime()) {
          acc.overdueInstallments += 1;
        }
      }
      return acc;
    },
    {
      totalInstallments: 0,
      settledInstallments: 0,
      overdueInstallments: 0,
    },
  );

  const repayments = transactions.map((tx) => {
    const metadata =
      tx?.metadata && typeof tx.metadata === "object" ? tx.metadata : {};
    const recordedById = metadata?.recordedBy
      ? String(metadata.recordedBy)
      : null;
    const recordedByProfile = recordedById
      ? recordedByMap.get(recordedById)
      : null;
    const receipt = normalizeManualRepaymentReceipt(metadata?.receipt);
    const allocations = Array.isArray(metadata?.allocations)
      ? metadata.allocations.map((allocation) => ({
          scheduleItemId: allocation?.scheduleItemId
            ? String(allocation.scheduleItemId)
            : "",
          installmentNumber: Number(allocation?.installmentNumber || 0),
          dueDate: allocation?.dueDate || null,
          appliedAmount: Number(allocation?.appliedAmount || 0),
          remainingInstallmentBalance: Number(
            allocation?.remainingInstallmentBalance || 0,
          ),
        }))
      : [];
    const remainingAfterPaymentRaw = Number(
      metadata?.remainingBalanceAfterPayment,
    );

    return {
      id: String(tx._id),
      reference: tx.reference,
      amount: Number(tx.amount || 0),
      status: tx.status,
      description: tx.description || null,
      paymentMethod:
        typeof metadata?.paymentMethod === "string" &&
        metadata.paymentMethod.trim()
          ? metadata.paymentMethod.trim()
          : tx.channel || null,
      paymentReference:
        typeof metadata?.manualPaymentReference === "string" &&
        metadata.manualPaymentReference.trim()
          ? metadata.manualPaymentReference.trim()
          : null,
      channel: tx.channel || null,
      gateway: tx.gateway || null,
      manual: Boolean(metadata?.manual),
      receipt,
      recordedAt: tx.date || null,
      receivedAt: metadata?.paidAt || tx.date || null,
      recordedBy: recordedById
        ? {
            id: recordedById,
            name: recordedByProfile?.fullName || "Admin user",
            email: recordedByProfile?.email || null,
          }
        : Boolean(metadata?.manual)
          ? {
              id: null,
              name: "Admin / Coordinator",
              email: null,
            }
          : {
              id: loan.userId ? String(loan.userId) : null,
              name: borrower?.fullName || "Member self-service",
              email: borrower?.email || null,
            },
      allocations,
      settledInstallmentCount: Number(metadata?.settledInstallmentCount || 0),
      remainingBalanceAfterPayment: Number.isFinite(remainingAfterPaymentRaw)
        ? remainingAfterPaymentRaw
        : null,
      notes:
        typeof metadata?.notes === "string" && metadata.notes.trim()
          ? metadata.notes.trim()
          : null,
    };
  });

  const totalRepayable = Number(
    loan.totalRepayable ?? loan.approvedAmount ?? loan.loanAmount ?? 0,
  );
  const remainingBalance = Number(loan.remainingBalance ?? 0);
  const repaidSoFar = Math.max(0, totalRepayable - remainingBalance);
  const nextPayment = nextPaymentMap.get(String(loan._id)) || null;
  const totalCollected = repayments.reduce(
    (sum, repayment) => sum + Number(repayment.amount || 0),
    0,
  );

  return {
    borrower,
    loan: {
      id: String(loan._id),
      loanCode: loan.loanCode || null,
      loanType: loan.loanType || null,
      loanStatus: loan.status,
      borrowerName: borrower?.fullName || "Member",
      borrowerEmail: borrower?.email || null,
      borrowerPhone: borrower?.phone || null,
      groupName: loan.groupName || null,
      approvedAmount: Number(loan.approvedAmount ?? loan.loanAmount ?? 0),
      totalRepayable,
      remainingBalance,
      repaidSoFar,
      disbursedAt: loan.disbursedAt || null,
      repaymentStartDate: loan.repaymentStartDate || null,
      nextPaymentAmount: Number(nextPayment?.amountDue ?? 0),
      nextPaymentDueDate: nextPayment?.dueDate ?? null,
      nextPaymentStatus: nextPayment?.status ?? null,
    },
    summary: {
      totalRepayments: repayments.length,
      totalCollected,
      lastRepaymentAt:
        repayments[0]?.receivedAt || repayments[0]?.recordedAt || null,
      settledInstallments: scheduleSummary.settledInstallments,
      totalInstallments: scheduleSummary.totalInstallments,
      overdueInstallments: scheduleSummary.overdueInstallments,
      remainingBalance,
      repaidSoFar,
      nextPaymentAmount: Number(nextPayment?.amountDue ?? 0),
      nextPaymentDueDate: nextPayment?.dueDate ?? null,
      nextPaymentStatus: nextPayment?.status ?? null,
    },
    repayments,
  };
}

export const uploadAdminLoanRepaymentReceipt = catchAsync(
  async (req, res, next) => {
    const receipt = formatUploadedRepaymentReceipt(
      req.body?.receiptUpload,
      req.file || null,
    );

    if (!receipt) {
      return next(new AppError("Unable to upload repayment receipt", 400));
    }

    return sendSuccess(res, {
      statusCode: 201,
      data: { receipt },
    });
  },
);

export const listAdminLoanRepayments = catchAsync(async (req, res, next) => {
  if (!req.loanApplication) {
    return next(new AppError("Missing loan context", 500));
  }
  const history = await buildAdminLoanRepaymentHistoryData(req.loanApplication);

  return sendSuccess(res, {
    statusCode: 200,
    results: history.repayments.length,
    data: history,
  });
});

export const downloadAdminLoanRepaymentReceiptPdf = catchAsync(
  async (req, res, next) => {
    if (!req.loanApplication) {
      return next(new AppError("Missing loan context", 500));
    }

    const tx = await findLoanRepaymentTransaction(
      req.loanApplication,
      req.params.repaymentId,
    );
    const borrower = req.loanApplication.userId
      ? await ProfileModel.findById(req.loanApplication.userId, {
          fullName: 1,
          email: 1,
          phone: 1,
        }).lean()
      : null;

    const payload = buildReceiptPayload({ tx, profile: borrower });
    const pdfBuffer = await generateReceiptPdfBuffer(payload);
    const filename = `loan-repayment-receipt-${String(tx.reference || tx._id)
      .toLowerCase()
      .replace(/[^a-z0-9-]+/g, "-")}.pdf`;

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    return res.status(200).send(pdfBuffer);
  },
);

export const emailAdminLoanRepaymentReceipt = catchAsync(
  async (req, res, next) => {
    if (!req.loanApplication) {
      return next(new AppError("Missing loan context", 500));
    }

    const tx = await findLoanRepaymentTransaction(
      req.loanApplication,
      req.params.repaymentId,
    );
    const borrower = req.loanApplication.userId
      ? await ProfileModel.findById(req.loanApplication.userId, {
          fullName: 1,
          email: 1,
          phone: 1,
        }).lean()
      : null;

    const rawEmails = parseEmailList(req.body?.emails || req.body?.email);
    const recipients = rawEmails.length
      ? rawEmails
      : borrower?.email
        ? [borrower.email]
        : [];
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
      return next(
        new AppError("Too many email recipients. Maximum is 10.", 400),
      );
    }

    const payload = buildReceiptPayload({ tx, profile: borrower });
    const pdfBuffer = await generateReceiptPdfBuffer(payload);

    await sendEmail({
      to: uniqueEmails,
      subject: `CRC Repayment Receipt - ${tx.reference}`,
      html: buildReceiptEmailHtml(payload),
      text: buildReceiptEmailText(payload),
      attachments: [
        {
          filename: `loan-repayment-receipt-${String(tx.reference || tx._id)
            .toLowerCase()
            .replace(/[^a-z0-9-]+/g, "-")}.pdf`,
          content: pdfBuffer.toString("base64"),
          contentType: "application/pdf",
        },
      ],
    });

    return sendSuccess(res, {
      statusCode: 200,
      message: "Repayment receipt email queued",
      data: { ok: true, recipients: uniqueEmails },
    });
  },
);

export const exportAdminLoanRepaymentHistory = catchAsync(
  async (req, res, next) => {
    if (!req.loanApplication) {
      return next(new AppError("Missing loan context", 500));
    }

    const format = String(req.query?.format || "csv").trim().toLowerCase();
    if (!["csv", "pdf"].includes(format)) {
      return next(new AppError("Invalid format. Use csv or pdf.", 400));
    }

    const history = await buildAdminLoanRepaymentHistoryData(req.loanApplication);
    const safeRef = String(
      history.loan?.loanCode || req.loanApplication.loanCode || req.loanApplication._id,
    )
      .toLowerCase()
      .replace(/[^a-z0-9-]+/g, "-");

    if (format === "csv") {
      const csv = buildLoanRepaymentHistoryCsv(history);
      res.setHeader("Content-Type", "text/csv; charset=utf-8");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="loan-repayment-history-${safeRef}.csv"`,
      );
      return res.status(200).send(`\uFEFF${csv}`);
    }

    const pdfBuffer = await generateLoanRepaymentHistoryPdfBuffer({
      organization: {
        name: receiptOrganizationInfo.name,
        subtitle: "Loan Operations Desk",
      },
      loan: history.loan,
      summary: history.summary,
      repayments: history.repayments,
    });

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="loan-repayment-history-${safeRef}.pdf"`,
    );
    return res.status(200).send(pdfBuffer);
  },
);

export const listAdminLoanApplications = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const { filterBase } = await buildAdminLoanBaseFilter(req);
  const filter = { ...filterBase };

  if (typeof req.query?.status === "string" && req.query.status.trim()) {
    const status = String(req.query.status).trim();
    if (LoanApplicationStatuses.includes(status)) filter.status = status;
  }
  if (!filter.status) {
    filter.status = { $ne: "draft" };
  }

  const page = Math.max(1, parseInt(String(req.query?.page ?? "1"), 10) || 1);
  const limit = Math.min(
    200,
    Math.max(1, parseInt(String(req.query?.limit ?? "50"), 10) || 50),
  );
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
          status: req.query?.status === "draft" ? "draft" : { $ne: "draft" },
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
  const nextPaymentMap = await buildLoanNextPaymentMap(applications);

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
      nextPaymentDueDate: nextPaymentMap.get(String(a._id))?.dueDate ?? null,
      nextPaymentAmount: nextPaymentMap.get(String(a._id))?.amountDue ?? null,
      nextPaymentStatus: nextPaymentMap.get(String(a._id))?.status ?? null,
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

export const listAdminLoanTracker = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const { filterBase } = await buildAdminLoanBaseFilter(req);
  const filter = {
    ...filterBase,
    status: { $in: ["disbursed", "defaulted", "completed"] },
  };

  const loanType =
    typeof req.query?.loanType === "string" ? req.query.loanType.trim() : "";
  if (loanType) {
    filter.loanType = loanType;
  }

  const applications = await LoanApplicationModel.find(filter)
    .sort({ disbursedAt: -1, createdAt: -1 })
    .lean();

  if (applications.length === 0) {
    return sendSuccess(res, {
      statusCode: 200,
      results: 0,
      data: {
        loans: [],
        summary: {
          activeLoans: 0,
          completedLoans: 0,
          overdueLoans: 0,
          defaultedLoans: 0,
          totalOutstanding: 0,
          totalRepaid: 0,
          totalNextDue: 0,
        },
      },
    });
  }

  const profileIds = applications.map((application) => application.userId).filter(Boolean);
  const [profiles, nextPaymentMap, scheduleItems] = await Promise.all([
    ProfileModel.find(
      { _id: { $in: profileIds } },
      { fullName: 1, email: 1, phone: 1 },
    ).lean(),
    buildLoanNextPaymentMap(applications),
    LoanRepaymentScheduleItemModel.find({
      loanApplicationId: { $in: applications.map((application) => application._id) },
    })
      .sort({ installmentNumber: 1 })
      .lean(),
  ]);

  const profileById = new Map(profiles.map((profile) => [String(profile._id), profile]));
  const scheduleStatsByLoan = new Map();
  const now = new Date();

  for (const item of scheduleItems) {
    const loanId = String(item.loanApplicationId);
    const dueAmount = getLoanScheduleOutstandingAmount(item);
    const current = scheduleStatsByLoan.get(loanId) || {
      totalInstallments: 0,
      paidInstallments: 0,
      overdueInstallments: 0,
      outstandingScheduleBalance: 0,
      lastPaidAt: null,
    };

    current.totalInstallments += 1;
    current.outstandingScheduleBalance += dueAmount;

    if (dueAmount <= 0) {
      current.paidInstallments += 1;
      if (item.paidAt) {
        const paidAtIso = new Date(item.paidAt).toISOString();
        if (!current.lastPaidAt || paidAtIso > current.lastPaidAt) {
          current.lastPaidAt = paidAtIso;
        }
      }
    } else {
      const dueDate = item.dueDate ? new Date(item.dueDate) : null;
      if (dueDate && dueDate.getTime() < now.getTime()) {
        current.overdueInstallments += 1;
      }
    }

    scheduleStatsByLoan.set(loanId, current);
  }

  const statusFilter = String(req.query?.status || "active").trim().toLowerCase();
  const rows = applications
    .map((application) => {
      const loanId = String(application._id);
      const applicant = profileById.get(String(application.userId)) || null;
      const nextPayment = nextPaymentMap.get(loanId) || null;
      const scheduleStats = scheduleStatsByLoan.get(loanId) || {
        totalInstallments: 0,
        paidInstallments: 0,
        overdueInstallments: 0,
        outstandingScheduleBalance: 0,
        lastPaidAt: null,
      };

      const totalRepayable = Number(
        application.totalRepayable ?? application.approvedAmount ?? application.loanAmount ?? 0,
      );
      const remainingBalance = Number(application.remainingBalance ?? 0);
      const repaidSoFar = Math.max(0, totalRepayable - remainingBalance);
      const trackerStatus =
        remainingBalance <= 0 || application.status === "completed"
          ? "completed"
          : scheduleStats.overdueInstallments > 0
            ? "overdue"
            : "active";

      return {
        _id: application._id,
        borrowerId: application.userId,
        borrowerName: applicant?.fullName || "Member",
        borrowerEmail: applicant?.email || null,
        borrowerPhone: applicant?.phone || null,
        groupId: application.groupId || null,
        groupName: application.groupName || null,
        loanCode: application.loanCode || null,
        loanType: application.loanType || null,
        loanStatus: application.status,
        trackerStatus,
        approvedAmount: Number(application.approvedAmount ?? application.loanAmount ?? 0),
        totalRepayable,
        remainingBalance,
        repaidSoFar,
        interestRate: Number(
          application.approvedInterestRate ?? application.interestRate ?? 0,
        ),
        interestRateType: application.interestRateType || null,
        repaymentPeriod: Number(application.repaymentPeriod ?? 0),
        monthlyPayment: Number(application.monthlyPayment ?? 0),
        disbursedAt: application.disbursedAt || null,
        repaymentStartDate: application.repaymentStartDate || null,
        nextPaymentDueDate: nextPayment?.dueDate ?? null,
        nextPaymentAmount: Number(nextPayment?.amountDue ?? 0),
        nextPaymentStatus: nextPayment?.status ?? null,
        nextInstallmentNumber: nextPayment?.installmentNumber ?? null,
        overdueInstallments: scheduleStats.overdueInstallments,
        paidInstallments: scheduleStats.paidInstallments,
        totalInstallments: scheduleStats.totalInstallments,
        lastPaidAt: scheduleStats.lastPaidAt,
      };
    })
    .filter((row) => {
      if (statusFilter === "all") return true;
      if (statusFilter === "completed") return row.trackerStatus === "completed";
      if (statusFilter === "overdue") return row.trackerStatus === "overdue";
      if (statusFilter === "active") return row.trackerStatus !== "completed";
      return row.loanStatus === statusFilter;
    });

  const summary = rows.reduce(
    (acc, row) => {
      if (row.trackerStatus === "completed") acc.completedLoans += 1;
      else acc.activeLoans += 1;
      if (row.trackerStatus === "overdue") acc.overdueLoans += 1;
      if (row.loanStatus === "defaulted") acc.defaultedLoans += 1;
      acc.totalOutstanding += Number(row.remainingBalance || 0);
      acc.totalRepaid += Number(row.repaidSoFar || 0);
      acc.totalNextDue += Number(row.nextPaymentAmount || 0);
      return acc;
    },
    {
      activeLoans: 0,
      completedLoans: 0,
      overdueLoans: 0,
      defaultedLoans: 0,
      totalOutstanding: 0,
      totalRepaid: 0,
      totalNextDue: 0,
    },
  );

  return sendSuccess(res, {
    statusCode: 200,
    results: rows.length,
    data: { loans: rows, summary },
  });
});

export const recordAdminLoanRepayment = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));
  if (!req.loanApplication) return next(new AppError("Missing loan context", 500));

  const amount = Number(req.body?.amount);
  const paymentMethod = String(
    req.body?.paymentMethod || "bank_transfer",
  ).trim().toLowerCase();
  const manualPaymentReference = req.body?.paymentReference
    ? String(req.body.paymentReference).trim()
    : null;
  const notes = req.body?.notes ? String(req.body.notes).trim() : "";
  const receivedAt = req.body?.receivedAt ? new Date(req.body.receivedAt) : new Date();
  const hasReceiptInput =
    req.body &&
    Object.prototype.hasOwnProperty.call(req.body, "receipt");
  const receipt = hasReceiptInput
    ? normalizeManualRepaymentReceipt(req.body?.receipt)
    : null;

  if (!Number.isFinite(amount) || amount <= 0) {
    return next(new AppError("amount must be greater than 0", 400));
  }
  if (!ManualLoanPaymentMethods.has(paymentMethod)) {
    return next(new AppError("Invalid paymentMethod", 400));
  }
  if (Number.isNaN(receivedAt.getTime())) {
    return next(new AppError("Invalid receivedAt date", 400));
  }
  if (hasReceiptInput && !receipt) {
    return next(new AppError("Invalid repayment receipt", 400));
  }

  const reference = generateReference();
  const result = await applyLoanRepayment({
    application: req.loanApplication,
    amount,
    reference,
    channel: paymentMethod,
    description:
      notes || `Manual loan repayment for ${req.loanApplication.loanCode || "loan"}`,
    gateway: "manual",
    metadata: {
      manual: true,
      recordedBy: req.user.profileId,
      paymentMethod,
      manualPaymentReference: manualPaymentReference || null,
      notes: notes || null,
      receipt,
    },
    paidAt: receivedAt,
  });

  createNotification({
    userId: req.loanApplication.userId,
    title: "Loan repayment recorded",
    message: `A manual repayment has been recorded for ${req.loanApplication.loanCode || "your loan"}.`,
    type: "payment_received",
    metadata: {
      loanId: req.loanApplication._id,
      loanCode: req.loanApplication.loanCode,
      amount,
      reference,
      manual: true,
      paymentMethod,
      manualPaymentReference,
      receiptUrl: receipt?.url || null,
      allocations: result.allocations.map((allocation) => ({
        installmentNumber: allocation.installmentNumber,
        appliedAmount: allocation.appliedAmount,
      })),
    },
  }).catch((error) => {
    console.error("Failed to create admin repayment notification", error);
  });

  return sendSuccess(res, {
    statusCode: 200,
    message: "Loan repayment recorded",
    data: {
      transaction: result.transaction,
      application: result.application,
      allocations: result.allocations,
      nextPayment: result.nextPayment,
    },
  });
});

export const reviewAdminLoanApplication = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const { applicationId } = req.params;
  const app = await LoanApplicationModel.findById(applicationId);
  if (!app) return next(new AppError("Loan application not found", 404));

  if (!hasUserRole(req.user, "admin")) {
    if (!app.groupId)
      return next(new AppError("Only admins can review this loan", 403));

    const manageableGroupIds = await getManageableGroupIds(req);
    if (
      !manageableGroupIds ||
      !manageableGroupIds.includes(String(app.groupId))
    ) {
      return next(new AppError("You cannot review loans for this group", 403));
    }
  }

  const { status, reviewNotes, approvedAmount, approvedInterestRate } =
    req.body || {};

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
    if (
      typeof approvedInterestRate !== "undefined" &&
      approvedInterestRate !== null
    ) {
      if (
        !isInterestRateAllowed(
          app.loanType || "revolving",
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

export const reconcileAdminLoanApplication = catchAsync(
  async (req, res, next) => {
    if (!req.user) return next(new AppError("Not authenticated", 401));
    if (!req.user.profileId)
      return next(new AppError("User profile not found", 400));
    if (!req.loanApplication)
      return next(new AppError("Missing loan context", 500));

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
  },
);

export const reviewLoanEditRequest = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));
  if (!req.loanApplication)
    return next(new AppError("Missing loan context", 500));

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
    if (
      Object.prototype.hasOwnProperty.call(updatePayload, "repaymentPeriod")
    ) {
      const period = Number(updatePayload.repaymentPeriod);
      if (!period || period <= 0) {
        return next(
          new AppError("repaymentPeriod must be greater than 0", 400),
        );
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

export const exportAdminLoanApplications = catchAsync(
  async (req, res, next) => {
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
        return next(
          new AppError("You cannot manage loans for this group", 403),
        );
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
      const safeYear = Number.isFinite(year) && year ? year : now.getFullYear();
      const safeMonth =
        Number.isFinite(month) && month
          ? Math.min(12, Math.max(1, month))
          : null;
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
      const regex = new RegExp(escapeRegex(search), "i");
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
  },
);

export const downloadAdminLoanApplicationPdf = catchAsync(
  async (req, res, next) => {
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

    const loan =
      typeof loanDoc.toObject === "function" ? loanDoc.toObject() : loanDoc;
    const pdfBuffer = await generateLoanApplicationPdfBuffer({
      loan,
      applicant,
      organization: {
        name: "Champions Revolving Contributions",
        subtitle: "Loan Processing Desk",
      },
    });

    const reference = loan.loanCode || loan.loanNumber || loan._id || "loan";
    const safeRef = String(reference)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-");

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename=\"loan-application-${safeRef}.pdf\"`,
    );
    res.status(200).send(pdfBuffer);
  },
);

export const emailAdminLoanApplicationPdf = catchAsync(
  async (req, res, next) => {
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
    const extraEmails = parseEmailList(
      req.body?.extraEmails || req.body?.emails,
    );

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
      return next(
        new AppError("Too many email recipients. Maximum is 10.", 400),
      );
    }

    const loan =
      typeof loanDoc.toObject === "function" ? loanDoc.toObject() : loanDoc;
    const pdfBuffer = await generateLoanApplicationPdfBuffer({
      loan,
      applicant,
      organization: {
        name: "Champions Revolving Contributions",
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
  },
);
