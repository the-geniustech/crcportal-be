import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import {
  FormPaymentModel,
  FormPaymentStatuses,
  FormPaymentTypes,
} from "../models/FormPayment.js";
import { hasUserRole } from "../utils/roles.js";
import {
  BSS_FORM_PAYMENT_TYPES,
  resolveFormPaymentDisplayLabel,
} from "../services/formPaymentService.js";
import { generateGroupFormPaymentLedgerWorkbookBuffer } from "../services/groupFormPaymentLedgerWorkbook.js";
import { generateGroupFormPaymentLedgerPdfBuffer } from "../services/pdf/groupFormPaymentLedgerPdf.js";

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 200;
const ELEVATED_GROUP_ROLES = new Set([
  "coordinator",
  "treasurer",
  "secretary",
  "admin",
]);

const SORT_MAP = {
  submitted_desc: { submittedAt: -1, createdAt: -1 },
  submitted_asc: { submittedAt: 1, createdAt: 1 },
  reviewed_desc: { reviewedAt: -1, updatedAt: -1 },
  reviewed_asc: { reviewedAt: 1, updatedAt: 1 },
  amount_desc: { amount: -1, submittedAt: -1 },
  amount_asc: { amount: 1, submittedAt: -1 },
  member_asc: { memberName: 1, submittedAt: -1 },
  member_desc: { memberName: -1, submittedAt: -1 },
  form_type_asc: { formLabel: 1, submittedAt: -1 },
  status_asc: { paymentStatus: 1, submittedAt: -1 },
};

const FORM_TYPE_LABELS = {
  membership_registration: "Membership Registration",
  revolving_loan: "Revolving Loan",
  bridging_loan: "BSS Loan Form",
  soft_loan: "BSS Loan Form",
  special_loan: "BSS Loan Form",
};
const VIRTUAL_FORM_TYPES = ["bss_loan"];

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function escapeRegex(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function csvEscape(value) {
  const raw = String(value ?? "");
  if (/[",\n]/.test(raw)) return `"${raw.replace(/"/g, '""')}"`;
  return raw;
}

function sanitizeFilenameSegment(value) {
  return String(value || "group")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || "group";
}

function formatCurrency(amount) {
  const value = Number(amount || 0);
  if (!Number.isFinite(value)) return "NGN 0";
  return `NGN ${value.toLocaleString("en-NG", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  })}`;
}

function formatDate(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return new Intl.DateTimeFormat("en-NG", {
    day: "numeric",
    month: "short",
    year: "numeric",
  }).format(date);
}

function resolveFormLabel(payment) {
  return resolveFormPaymentDisplayLabel(
    payment,
    FORM_TYPE_LABELS[payment.formType],
  );
}

function buildExportRows(payments) {
  return payments.map((payment) => ({
    memberName: payment.memberName || "Unknown member",
    memberEmail: payment.memberEmail || "-",
    memberPhone: payment.memberPhone || "-",
    formLabel: resolveFormLabel(payment),
    amount: Number(payment.amount || 0),
    amountFormatted: formatCurrency(payment.amount),
    paymentStatus: payment.paymentStatus || "-",
    submittedAt: formatDate(payment.submittedAt),
    reviewedAt: formatDate(payment.reviewedAt),
    sourceReference: payment.sourceReference || "-",
    transactionReference: payment.transactionReference || "-",
  }));
}

function parseDateParam(value, label, endOfDay = false) {
  if (!value) return null;
  const parsed = new Date(String(value));
  if (Number.isNaN(parsed.getTime())) {
    throw new AppError(`Invalid ${label} date`, 400);
  }
  if (endOfDay) parsed.setHours(23, 59, 59, 999);
  return parsed;
}

function canViewAllGroupPayments(req) {
  if (hasUserRole(req.user, "admin", "groupCoordinator")) return true;
  const membership = req.groupMembership;
  if (!membership || membership.status !== "active") return false;
  return ELEVATED_GROUP_ROLES.has(membership.role);
}

function resolveSort(req) {
  const sort = String(req.query?.sort || "submitted_desc").trim();
  return SORT_MAP[sort] || SORT_MAP.submitted_desc;
}

function serializePayment(payment) {
  const plain =
    payment && typeof payment.toObject === "function"
      ? payment.toObject({ versionKey: false })
      : payment;

  if (!plain) return null;

  return {
    ...plain,
    id: String(plain._id),
    _id: String(plain._id),
    formLabel: resolveFormLabel(plain),
    userId: plain.userId ? String(plain.userId) : null,
    userAccountId: plain.userAccountId ? String(plain.userAccountId) : null,
    groupId: plain.groupId ? String(plain.groupId) : null,
    sourceId: plain.sourceId ? String(plain.sourceId) : null,
    transactionId: plain.transactionId ? String(plain.transactionId) : null,
    reviewedBy: plain.reviewedBy ? String(plain.reviewedBy) : null,
  };
}

function buildFilter(req) {
  if (!req.group?._id) {
    throw new AppError("Missing group context", 500);
  }

  const filter = { groupId: req.group._id };

  if (!canViewAllGroupPayments(req)) {
    filter.userId = req.user.profileId;
  }

  const formType = String(req.query?.formType || "all").trim();
  if (formType && formType !== "all") {
    if (formType === "bss_loan") {
      filter.formType = { $in: BSS_FORM_PAYMENT_TYPES };
    } else if (FormPaymentTypes.includes(formType)) {
      filter.formType = formType;
    } else if (!VIRTUAL_FORM_TYPES.includes(formType)) {
      throw new AppError("Invalid form type", 400);
    }
  }

  const paymentStatus = String(req.query?.paymentStatus || "all").trim();
  if (paymentStatus && paymentStatus !== "all") {
    if (!FormPaymentStatuses.includes(paymentStatus)) {
      throw new AppError("Invalid payment status", 400);
    }
    filter.paymentStatus = paymentStatus;
  }

  const fromDate = parseDateParam(req.query?.from, "from");
  const toDate = parseDateParam(req.query?.to, "to", true);
  if (fromDate || toDate) {
    filter.submittedAt = {};
    if (fromDate) filter.submittedAt.$gte = fromDate;
    if (toDate) filter.submittedAt.$lte = toDate;
  }

  if (fromDate && toDate && fromDate.getTime() > toDate.getTime()) {
    throw new AppError("From date cannot be after to date", 400);
  }

  const search = String(req.query?.search || "").trim();
  if (search) {
    const regex = new RegExp(escapeRegex(search), "i");
    filter.$or = [
      { memberName: regex },
      { memberEmail: regex },
      { memberPhone: regex },
      { formLabel: regex },
      { sourceReference: regex },
      { transactionReference: regex },
    ];
  }

  return filter;
}

async function getSummary(filter) {
  const [summary] = await FormPaymentModel.aggregate([
    { $match: filter },
    {
      $group: {
        _id: null,
        totalRecords: { $sum: 1 },
        totalAmount: { $sum: "$amount" },
        pendingCount: {
          $sum: { $cond: [{ $eq: ["$paymentStatus", "pending"] }, 1, 0] },
        },
        paidCount: {
          $sum: { $cond: [{ $eq: ["$paymentStatus", "paid"] }, 1, 0] },
        },
        defaultedCount: {
          $sum: { $cond: [{ $eq: ["$paymentStatus", "defaulted"] }, 1, 0] },
        },
        pendingAmount: {
          $sum: {
            $cond: [{ $eq: ["$paymentStatus", "pending"] }, "$amount", 0],
          },
        },
        paidAmount: {
          $sum: {
            $cond: [{ $eq: ["$paymentStatus", "paid"] }, "$amount", 0],
          },
        },
        defaultedAmount: {
          $sum: {
            $cond: [{ $eq: ["$paymentStatus", "defaulted"] }, "$amount", 0],
          },
        },
      },
    },
  ]);

  return {
    totalRecords: summary?.totalRecords ?? 0,
    totalAmount: summary?.totalAmount ?? 0,
    pendingCount: summary?.pendingCount ?? 0,
    paidCount: summary?.paidCount ?? 0,
    defaultedCount: summary?.defaultedCount ?? 0,
    pendingAmount: summary?.pendingAmount ?? 0,
    paidAmount: summary?.paidAmount ?? 0,
    defaultedAmount: summary?.defaultedAmount ?? 0,
  };
}

export const listGroupFormPayments = catchAsync(async (req, res) => {
  const page = parsePositiveInt(req.query?.page, 1);
  const limit = clamp(
    parsePositiveInt(req.query?.limit, DEFAULT_LIMIT),
    1,
    MAX_LIMIT,
  );
  const skip = (page - 1) * limit;
  const filter = buildFilter(req);
  const sort = resolveSort(req);

  const [payments, total, summary] = await Promise.all([
    FormPaymentModel.find(filter)
      .sort(sort)
      .skip(skip)
      .limit(limit)
      .lean(),
    FormPaymentModel.countDocuments(filter),
    getSummary(filter),
  ]);

  return sendSuccess(res, {
    statusCode: 200,
    results: payments.length,
    total,
    page,
    limit,
    data: {
      payments: payments.map(serializePayment),
      summary,
      scope: canViewAllGroupPayments(req) ? "group" : "member",
    },
  });
});

export const exportGroupFormPayments = catchAsync(async (req, res, next) => {
  const group = req.group;
  if (!group) return next(new AppError("Group not found", 404));

  const format = String(req.query?.format || "pdf")
    .trim()
    .toLowerCase();
  if (!["pdf", "csv", "xlsx"].includes(format)) {
    return next(new AppError("Invalid export format", 400));
  }

  const filter = buildFilter(req);
  const sort = resolveSort(req);
  const now = new Date();
  const scopeLabel = canViewAllGroupPayments(req)
    ? "Group-wide form payment records"
    : "My form payment records";

  const [payments, summary] = await Promise.all([
    FormPaymentModel.find(filter).sort(sort).lean(),
    getSummary(filter),
  ]);
  const rows = buildExportRows(payments);
  const filenameBase = `form-payments-${sanitizeFilenameSegment(
    group.groupName || "group",
  )}`;

  if (format === "csv") {
    const headers = [
      "Member Name",
      "Email",
      "Phone",
      "Form Type",
      "Amount",
      "Status",
      "Submitted",
      "Reviewed",
      "Source Reference",
      "Transaction Reference",
    ];
    const body = rows.map((row) => [
      row.memberName,
      row.memberEmail,
      row.memberPhone,
      row.formLabel,
      row.amount,
      row.paymentStatus,
      row.submittedAt,
      row.reviewedAt,
      row.sourceReference,
      row.transactionReference,
    ]);
    const totalsRow = [
      "Totals",
      "",
      "",
      `${rows.length} records`,
      summary.totalAmount,
      `${summary.paidCount} paid`,
      `${summary.pendingCount} pending`,
      `${summary.defaultedCount} defaulted`,
      "",
      "",
    ];
    const csv = `\uFEFFsep=,\n${[headers, ...body, totalsRow]
      .map((row) => row.map(csvEscape).join(","))
      .join("\n")}`;

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${filenameBase}.csv"`,
    );
    return res.status(200).send(csv);
  }

  if (format === "xlsx") {
    const workbookBuffer = await generateGroupFormPaymentLedgerWorkbookBuffer({
      groupName: group.groupName || "Group",
      scopeLabel,
      generatedAt: now,
      rows,
      summary,
    });

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    );
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${filenameBase}.xlsx"`,
    );
    return res.status(200).send(workbookBuffer);
  }

  const pdfBuffer = await generateGroupFormPaymentLedgerPdfBuffer({
    groupName: group.groupName || "Group",
    scopeLabel,
    generatedAt: now,
    rows,
    summary,
  });

  res.setHeader("Content-Type", "application/pdf");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="${filenameBase}.pdf"`,
  );
  return res.status(200).send(pdfBuffer);
});
