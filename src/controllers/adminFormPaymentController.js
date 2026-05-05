import mongoose from "mongoose";

import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import {
  FormPaymentModel,
  FormPaymentStatuses,
  FormPaymentTypes,
} from "../models/FormPayment.js";
import {
  AuditActions,
  AuditEntityTypes,
  createAuditLog,
} from "../services/auditLog.js";
import { syncFormPaymentTransaction } from "../services/formPaymentService.js";
import { generateGroupFormPaymentLedgerWorkbookBuffer } from "../services/groupFormPaymentLedgerWorkbook.js";
import { generateGroupFormPaymentLedgerPdfBuffer } from "../services/pdf/groupFormPaymentLedgerPdf.js";

const DEFAULT_LIMIT = 20;
const MAX_LIMIT = 100;
const SORT_MAP = {
  submitted_desc: { submittedAt: -1, createdAt: -1 },
  submitted_asc: { submittedAt: 1, createdAt: 1 },
  reviewed_desc: { reviewedAt: -1, updatedAt: -1 },
  reviewed_asc: { reviewedAt: 1, updatedAt: 1 },
  amount_desc: { amount: -1, submittedAt: -1 },
  amount_asc: { amount: 1, submittedAt: -1 },
  member_asc: { memberName: 1, submittedAt: -1 },
  member_desc: { memberName: -1, submittedAt: -1 },
  group_asc: { groupName: 1, submittedAt: -1 },
  group_desc: { groupName: -1, submittedAt: -1 },
  form_type_asc: { formLabel: 1, submittedAt: -1 },
  status_asc: { paymentStatus: 1, submittedAt: -1 },
};
const FORM_TYPE_LABELS = {
  membership_registration: "Membership Registration",
  revolving_loan: "Revolving Loan",
  bridging_loan: "BSS Bridging Loan",
  soft_loan: "BSS Soft Loan",
  special_loan: "BSS Special Loan",
};

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
  return (
    String(value || "form-payments")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "") || "form-payments"
  );
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
  return (
    payment.formLabel ||
    FORM_TYPE_LABELS[payment.formType] ||
    "Form Payment"
  );
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
    userId: plain.userId ? String(plain.userId) : null,
    userAccountId: plain.userAccountId ? String(plain.userAccountId) : null,
    groupId: plain.groupId ? String(plain.groupId) : null,
    sourceId: plain.sourceId ? String(plain.sourceId) : null,
    transactionId: plain.transactionId ? String(plain.transactionId) : null,
    reviewedBy: plain.reviewedBy ? String(plain.reviewedBy) : null,
  };
}

function buildFilter(req) {
  const filter = {};

  const formType = String(req.query?.formType || "all").trim();
  if (formType && formType !== "all") {
    if (!FormPaymentTypes.includes(formType)) {
      throw new AppError("Invalid form type", 400);
    }
    filter.formType = formType;
  }

  const paymentStatus = String(req.query?.paymentStatus || "all").trim();
  if (paymentStatus && paymentStatus !== "all") {
    if (!FormPaymentStatuses.includes(paymentStatus)) {
      throw new AppError("Invalid payment status", 400);
    }
    filter.paymentStatus = paymentStatus;
  }

  const groupId = String(req.query?.groupId || "all").trim();
  if (groupId && groupId !== "all") {
    if (!mongoose.isValidObjectId(groupId)) {
      throw new AppError("Invalid groupId", 400);
    }
    filter.groupId = new mongoose.Types.ObjectId(groupId);
  }

  const fromDate = parseDateParam(req.query?.from, "from");
  const toDate = parseDateParam(req.query?.to, "to", true);
  if (fromDate || toDate) {
    filter.submittedAt = {};
    if (fromDate) filter.submittedAt.$gte = fromDate;
    if (toDate) filter.submittedAt.$lte = toDate;
  }

  if (
    fromDate &&
    toDate &&
    fromDate.getTime() > toDate.getTime()
  ) {
    throw new AppError("From date cannot be after to date", 400);
  }

  const search = String(req.query?.search || "").trim();
  if (search) {
    const regex = new RegExp(escapeRegex(search), "i");
    filter.$or = [
      { memberName: regex },
      { memberEmail: regex },
      { memberPhone: regex },
      { groupName: regex },
      { formLabel: regex },
      { sourceReference: regex },
      { transactionReference: regex },
    ];
  }

  return filter;
}

function buildExportRows(payments) {
  return payments.map((payment) => ({
    memberName: payment.memberName || "Unknown member",
    groupName: payment.groupName || "No group",
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

function resolveSort(req) {
  const sort = String(req.query?.sort || "submitted_desc").trim();
  return SORT_MAP[sort] || SORT_MAP.submitted_desc;
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

export const listAdminFormPayments = catchAsync(async (req, res) => {
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
    },
  });
});

export const exportAdminFormPayments = catchAsync(async (req, res, next) => {
  const format = String(req.query?.format || "pdf")
    .trim()
    .toLowerCase();
  if (!["pdf", "csv", "xlsx"].includes(format)) {
    return next(new AppError("Invalid export format", 400));
  }

  const filter = buildFilter(req);
  const sort = resolveSort(req);
  const now = new Date();

  const [payments, summary] = await Promise.all([
    FormPaymentModel.find(filter).sort(sort).lean(),
    getSummary(filter),
  ]);
  const rows = buildExportRows(payments);
  const filenameBase = sanitizeFilenameSegment(
    `admin-form-payments-${now.toISOString().slice(0, 10)}`,
  );

  if (format === "csv") {
    const headers = [
      "Member Name",
      "Group Name",
      "Email",
      "Phone",
      "Form Type",
      "Amount (NGN)",
      "Status",
      "Submitted",
      "Reviewed",
      "Source Reference",
      "Transaction Reference",
    ];
    const body = rows.map((row) => [
      row.memberName,
      row.groupName,
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
      "All groups",
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
      groupName: "All Groups",
      scopeLabel: "Admin form payment records",
      generatedAt: now,
      rows,
      summary,
      includeGroupName: true,
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
    groupName: "All Groups",
    scopeLabel: "Admin form payment records",
    generatedAt: now,
    rows,
    summary,
    includeGroupName: true,
  });

  res.setHeader("Content-Type", "application/pdf");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="${filenameBase}.pdf"`,
  );
  return res.status(200).send(pdfBuffer);
});

export const getAdminFormPaymentDetails = catchAsync(async (req, res, next) => {
  const { paymentId } = req.params;
  if (!mongoose.isValidObjectId(paymentId)) {
    return next(new AppError("Invalid paymentId", 400));
  }

  const payment = await FormPaymentModel.findById(paymentId).lean();
  if (!payment) return next(new AppError("Form payment not found", 404));

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      payment: serializePayment(payment),
    },
  });
});

export const updateAdminFormPayment = catchAsync(async (req, res, next) => {
  const { paymentId } = req.params;
  if (!mongoose.isValidObjectId(paymentId)) {
    return next(new AppError("Invalid paymentId", 400));
  }

  const patch = {};
  if (Object.prototype.hasOwnProperty.call(req.body || {}, "paymentStatus")) {
    const paymentStatus = String(req.body.paymentStatus || "").trim();
    if (!FormPaymentStatuses.includes(paymentStatus)) {
      return next(new AppError("Invalid payment status", 400));
    }
    patch.paymentStatus = paymentStatus;
    patch.reviewedAt = new Date();
    patch.reviewedBy = req.user?.profileId ?? null;
  }

  if (Object.prototype.hasOwnProperty.call(req.body || {}, "notes")) {
    patch.notes = String(req.body.notes ?? "").trim() || null;
    patch.reviewedAt = patch.reviewedAt || new Date();
    patch.reviewedBy = req.user?.profileId ?? null;
  }

  if (Object.keys(patch).length === 0) {
    return next(
      new AppError("Provide a paymentStatus or notes value to update", 400),
    );
  }

  const existing = await FormPaymentModel.findById(paymentId).lean();
  if (!existing) return next(new AppError("Form payment not found", 404));

  const payment = await FormPaymentModel.findByIdAndUpdate(paymentId, patch, {
    new: true,
    runValidators: true,
  });

  await syncFormPaymentTransaction(payment, {
    actorProfileId: req.user?.profileId ?? null,
    channel: "admin_review",
  });

  const syncedPayment = await FormPaymentModel.findById(payment._id).lean();

  await createAuditLog({
    req,
    action: AuditActions.ADMIN_FORM_PAYMENT_UPDATE,
    entityType: AuditEntityTypes.FORM_PAYMENT,
    entityId: syncedPayment._id,
    targetProfileId: syncedPayment.userId,
    groupId: syncedPayment.groupId,
    summary: `Updated ${syncedPayment.formLabel} payment for ${syncedPayment.memberName || "member"}`,
    metadata: {
      previousStatus: existing.paymentStatus,
      nextStatus: syncedPayment.paymentStatus,
      transactionId: syncedPayment.transactionId || null,
      transactionReference: syncedPayment.transactionReference || null,
      notesUpdated: Object.prototype.hasOwnProperty.call(req.body || {}, "notes"),
    },
  });

  return sendSuccess(res, {
    statusCode: 200,
    message: "Form payment updated",
    data: {
      payment: serializePayment(syncedPayment),
    },
  });
});
