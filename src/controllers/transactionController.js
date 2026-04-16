import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import {
  TransactionModel,
  TransactionStatuses,
  TransactionTypes,
} from "../models/Transaction.js";
import { ProfileModel } from "../models/Profile.js";
import { sendEmail } from "../services/mail/resendClient.js";
import { generateReceiptPdfBuffer } from "../services/pdf/receiptPdf.js";
import {
  buildReceiptEmailHtml,
  buildReceiptEmailText,
  buildReceiptPayload,
  receiptTypeLabels,
} from "../services/receiptService.js";
import {
  generateStatementPdfBuffer,
  formatCurrency,
  formatDate,
} from "../services/pdf/statementPdf.js";

function formatStatementAmount(amount) {
  return formatCurrency(amount);
}

function parseEmailList(input) {
  if (Array.isArray(input)) {
    return input.map((item) => String(item).trim()).filter(Boolean);
  }
  if (typeof input === "string") {
    return input
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
  }
  return [];
}

function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value).trim());
}

export const listMyTransactions = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const filter = { userId: req.user.profileId };

  if (typeof req.query?.type === "string" && req.query.type.trim()) {
    const t = req.query.type.trim();
    if (TransactionTypes.includes(t)) filter.type = t;
  }

  if (typeof req.query?.status === "string" && req.query.status.trim()) {
    const s = req.query.status.trim();
    if (TransactionStatuses.includes(s)) filter.status = s;
  }

  const page = Math.max(1, parseInt(String(req.query?.page ?? "1"), 10) || 1);
  const limit = Math.min(
    200,
    Math.max(1, parseInt(String(req.query?.limit ?? "50"), 10) || 50),
  );
  const skip = (page - 1) * limit;

  const [transactions, total] = await Promise.all([
    TransactionModel.find(filter).sort({ date: -1 }).skip(skip).limit(limit),
    TransactionModel.countDocuments(filter),
  ]);

  return sendSuccess(res, {
    statusCode: 200,
    results: transactions.length,
    total,
    page,
    limit,
    data: { transactions },
  });
});

function formatStatementDate(value) {
  return formatDate(value);
}

function buildPeriodLabel({ startDate, endDate }) {
  if (startDate && endDate) {
    return `${formatStatementDate(startDate)} to ${formatStatementDate(endDate)}`;
  }
  if (startDate) {
    return `From ${formatStatementDate(startDate)}`;
  }
  if (endDate) {
    return `Up to ${formatStatementDate(endDate)}`;
  }
  return "All Time";
}

function toCsvValue(value) {
  if (value === null || value === undefined) return "";
  const text = String(value);
  if (/[",\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function buildStatementCsv({
  memberName,
  periodLabel,
  generatedAt,
  summary,
  rows,
}) {
  const lines = [];
  lines.push(`Member,${toCsvValue(memberName)}`);
  lines.push(`Generated,${toCsvValue(formatStatementDate(generatedAt))}`);
  lines.push(`Period,${toCsvValue(periodLabel)}`);
  lines.push("");
  lines.push("Date,Type,Description,Reference,Status,Amount");
  rows.forEach((row) => {
    lines.push(
      [
        toCsvValue(row.date),
        toCsvValue(row.type),
        toCsvValue(row.description),
        toCsvValue(row.reference),
        toCsvValue(row.status),
        toCsvValue(row.amount),
      ].join(","),
    );
  });
  lines.push("");
  lines.push(
    `Total Credits,${toCsvValue(formatStatementAmount(summary.totalCredits))}`,
  );
  lines.push(
    `Total Debits,${toCsvValue(formatStatementAmount(summary.totalDebits))}`,
  );
  lines.push(
    `Net Position,${toCsvValue(formatStatementAmount(summary.netPosition))}`,
  );
  lines.push(`Transactions,${toCsvValue(summary.transactionCount)}`);
  return lines.join("\n");
}

export const downloadMyStatement = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const format = String(req.query?.format || "pdf").toLowerCase();
  if (!["pdf", "csv"].includes(format)) {
    return next(new AppError("Invalid format. Use pdf or csv.", 400));
  }

  const filter = { userId: req.user.profileId };
  const rawType =
    typeof req.query?.type === "string" ? req.query.type.trim() : "";
  const normalizedType =
    rawType === "contribution" ? "group_contribution" : rawType;
  if (
    normalizedType &&
    Object.keys(receiptTypeLabels).includes(normalizedType)
  ) {
    filter.type = normalizedType;
  }

  const startDate = req.query?.startDate
    ? new Date(String(req.query.startDate))
    : null;
  const endDate = req.query?.endDate
    ? new Date(String(req.query.endDate))
    : null;
  if (startDate && Number.isNaN(startDate.getTime())) {
    return next(new AppError("Invalid start date", 400));
  }
  if (endDate && Number.isNaN(endDate.getTime())) {
    return next(new AppError("Invalid end date", 400));
  }

  if (startDate || endDate) {
    const start = startDate ? new Date(startDate) : new Date(0);
    const end = endDate ? new Date(endDate) : new Date();
    if (endDate) {
      end.setHours(23, 59, 59, 999);
    }
    filter.date = { $gte: start, $lte: end };
  }

  const transactions = await TransactionModel.find(filter)
    .sort({ date: -1 })
    .lean();

  const creditTypes = new Set(["deposit", "group_contribution", "interest"]);
  const debitTypes = new Set(["withdrawal", "loan_repayment"]);

  const summary = transactions.reduce(
    (acc, t) => {
      const amount = Number(t.amount || 0);
      if (creditTypes.has(t.type)) acc.totalCredits += amount;
      if (debitTypes.has(t.type)) acc.totalDebits += amount;
      acc.transactionCount += 1;
      return acc;
    },
    { totalCredits: 0, totalDebits: 0, transactionCount: 0 },
  );
  summary.netPosition = summary.totalCredits - summary.totalDebits;

  const rows = transactions.map((t) => ({
    date: formatStatementDate(t.date),
    type: receiptTypeLabels[t.type] || t.type,
    description: t.description || "-",
    reference: t.reference || "-",
    status: String(t.status || "pending").toUpperCase(),
    amount: formatStatementAmount(t.amount),
  }));

  const profile = await ProfileModel.findById(req.user.profileId).lean();
  const periodLabel = buildPeriodLabel({
    startDate: startDate || null,
    endDate: endDate || null,
  });
  const generatedAt = new Date();
  const memberName = profile?.fullName || "Member";
  const memberEmail = profile?.email || null;

  const safeLabel = periodLabel
    .replace(/\s+/g, "-")
    .replace(/[^a-zA-Z0-9\-]/g, "");
  const filename = `statement-${safeLabel || "all-time"}.${format}`;

  if (format === "csv") {
    const csv = buildStatementCsv({
      memberName,
      periodLabel,
      generatedAt,
      summary,
      rows,
    });
    res.setHeader("Content-Type", "text/csv");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    return res.status(200).send(csv);
  }

  const pdfBuffer = await generateStatementPdfBuffer({
    memberName,
    memberEmail,
    periodLabel,
    generatedAt,
    summary,
    rows,
  });

  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
  return res.status(200).send(pdfBuffer);
});

export const getMyTransaction = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const id = req.params.id;
  const transaction = await TransactionModel.findOne({
    _id: id,
    userId: req.user.profileId,
  });
  if (!transaction) return next(new AppError("Transaction not found", 404));

  return sendSuccess(res, { statusCode: 200, data: { transaction } });
});

export const emailMyTransactionReceipt = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const id = req.params.id;
  const emails = parseEmailList(req.body?.emails || req.body?.email)
    .map((value) => value.toLowerCase())
    .filter(Boolean);
  if (emails.length === 0)
    return next(new AppError("Valid email is required", 400));

  const invalid = emails.filter((value) => !isValidEmail(value));
  if (invalid.length > 0) {
    return next(new AppError(`Invalid email(s): ${invalid.join(", ")}`, 400));
  }

  const uniqueEmails = Array.from(new Set(emails));
  if (uniqueEmails.length > 10) {
    return next(new AppError("Too many email recipients. Maximum is 10.", 400));
  }

  const tx = await TransactionModel.findOne({
    _id: id,
    userId: req.user.profileId,
  });
  if (!tx) return next(new AppError("Transaction not found", 404));

  const profile = await ProfileModel.findById(req.user.profileId).lean();
  const payload = buildReceiptPayload({ tx, profile });
  const pdfBuffer = await generateReceiptPdfBuffer(payload);

  await sendEmail({
    to: uniqueEmails,
    subject: `CRC Receipt - ${tx.reference}`,
    html: buildReceiptEmailHtml(payload),
    text: buildReceiptEmailText(payload),
    attachments: [
      {
        filename: `Champions-Revolving-Contributions-Receipt-${tx.reference}.pdf`,
        content: pdfBuffer.toString("base64"),
        contentType: "application/pdf",
      },
    ],
  });

  return sendSuccess(res, {
    statusCode: 200,
    message: "Receipt email queued",
    data: { ok: true, recipients: uniqueEmails },
  });
});

export const downloadMyTransactionReceiptPdf = catchAsync(
  async (req, res, next) => {
    if (!req.user) return next(new AppError("Not authenticated", 401));
    if (!req.user.profileId)
      return next(new AppError("User profile not found", 400));

    const id = req.params.id;
    const tx = await TransactionModel.findOne({
      _id: id,
      userId: req.user.profileId,
    });
    if (!tx) return next(new AppError("Transaction not found", 404));

    const profile = await ProfileModel.findById(req.user.profileId).lean();
    const payload = buildReceiptPayload({ tx, profile });
    const pdfBuffer = await generateReceiptPdfBuffer(payload);

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="Champions-Revolving-Contributions-Receipt-${tx.reference}.pdf"`,
    );
    res.status(200).send(pdfBuffer);
  },
);

export const listTransactions = catchAsync(async (req, res) => {
  const filter = {};

  if (typeof req.query?.type === "string" && req.query.type.trim()) {
    const t = req.query.type.trim();
    if (TransactionTypes.includes(t)) filter.type = t;
  }

  if (typeof req.query?.status === "string" && req.query.status.trim()) {
    const s = req.query.status.trim();
    if (TransactionStatuses.includes(s)) filter.status = s;
  }

  if (typeof req.query?.userId === "string" && req.query.userId.trim()) {
    filter.userId = req.query.userId.trim();
  }

  const page = Math.max(1, parseInt(String(req.query?.page ?? "1"), 10) || 1);
  const limit = Math.min(
    200,
    Math.max(1, parseInt(String(req.query?.limit ?? "50"), 10) || 50),
  );
  const skip = (page - 1) * limit;

  const [transactions, total] = await Promise.all([
    TransactionModel.find(filter).sort({ date: -1 }).skip(skip).limit(limit),
    TransactionModel.countDocuments(filter),
  ]);

  return sendSuccess(res, {
    statusCode: 200,
    results: transactions.length,
    total,
    page,
    limit,
    data: { transactions },
  });
});
