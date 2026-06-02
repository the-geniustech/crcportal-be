import dotenv from "dotenv";

dotenv.config();

import mongoose from "mongoose";
import { connectMongo } from "../db.js";
import { FormPaymentModel, FormPaymentStatuses } from "../models/FormPayment.js";
import {
  GroupMembershipModel,
  GroupMembershipStatuses,
} from "../models/GroupMembership.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import { TransactionModel } from "../models/Transaction.js";
import {
  resolveLoanFormPaymentConfig,
  syncFormPaymentTransaction,
  upsertLoanFormPayment,
  upsertMembershipFormPayment,
} from "../services/formPaymentService.js";

const PRIMARY_CONFIRMATION_TOKEN = "SEED_HISTORICAL_FORM_PAYMENTS";
const LEGACY_CONFIRMATION_TOKEN = "SEED_UNPAID_FORM_PAYMENTS";
const CONFIRMATION_TOKENS = new Set([
  PRIMARY_CONFIRMATION_TOKEN,
  LEGACY_CONFIRMATION_TOKEN,
]);
const DEFAULT_BATCH_SIZE = 100;
const DEFAULT_MEMBERSHIP_STATUSES = [
  "pending",
  "active",
  "inactive",
  "suspended",
];
const DEFAULT_LOAN_EXCLUDED_STATUSES = ["draft"];
const DEFAULT_PAYMENT_STATUS = "paid";

function parseArgs(args) {
  const output = {};
  for (let index = 0; index < args.length; index += 1) {
    const current = args[index];
    if (!current.startsWith("--")) continue;
    const key = current.replace(/^--/, "");
    const next = args[index + 1];
    if (!next || next.startsWith("--")) {
      output[key] = true;
    } else {
      output[key] = next;
      index += 1;
    }
  }
  return output;
}

function parseCsv(value) {
  return String(value || "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function parseDateArg(value, label, { endOfDay = false } = {}) {
  const raw = String(value || "").trim();
  if (!raw) return null;

  const parsed = /^\d{4}-\d{2}-\d{2}$/.test(raw)
    ? new Date(`${raw}T${endOfDay ? "23:59:59.999" : "00:00:00.000"}Z`)
    : new Date(raw);

  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid --${label} date`);
  }
  if (endOfDay && !/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    parsed.setHours(23, 59, 59, 999);
  }
  return parsed;
}

function parseOptionalEndDateArg(args) {
  if (!Object.prototype.hasOwnProperty.call(args, "to")) return null;
  return parseDateArg(args.to, "to", { endOfDay: true });
}

function resolvePaymentStatus(value) {
  const status = String(value || DEFAULT_PAYMENT_STATUS)
    .trim()
    .toLowerCase();
  if (!FormPaymentStatuses.includes(status)) {
    throw new Error(
      `Invalid --payment-status. Use one of: ${FormPaymentStatuses.join(", ")}.`,
    );
  }
  return status;
}

function resolveMembershipStatuses(value) {
  const statuses = parseCsv(value);
  const resolved =
    statuses.length > 0 ? statuses : [...DEFAULT_MEMBERSHIP_STATUSES];
  const invalid = resolved.filter(
    (status) => !GroupMembershipStatuses.includes(status),
  );
  if (invalid.length > 0) {
    throw new Error(`Invalid membership status: ${invalid.join(", ")}`);
  }
  return resolved;
}

function resolveBatchSize(value) {
  if (value === undefined || value === null || value === true) {
    return DEFAULT_BATCH_SIZE;
  }
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed < 1 || parsed > 1000) {
    throw new Error("Invalid --batch-size. Use a value from 1 to 1000.");
  }
  return parsed;
}

function resolveHistoricalDate(value, fallback = new Date()) {
  const parsed = value ? new Date(value) : fallback;
  if (Number.isNaN(parsed.getTime())) return fallback;
  return parsed;
}

function resolveMembershipPaymentDate(membership) {
  return resolveHistoricalDate(
    membership?.joinedAt || membership?.requestedAt || membership?.createdAt,
  );
}

function resolveLoanPaymentDate(loan) {
  return resolveHistoricalDate(loan?.createdAt || loan?.updatedAt);
}

function buildDateWindow(fields, startDate, endDate) {
  if (!startDate && !endDate) return null;

  const window = {};
  if (startDate) window.$gte = startDate;
  if (endDate) window.$lte = endDate;

  return {
    $or: fields.map((field) => ({
      [field]: window,
    })),
  };
}

function buildMembershipQuery({ startDate, endDate, statuses }) {
  const query = {
    userId: { $ne: null },
    groupId: { $ne: null },
    status: { $in: statuses },
  };

  const dateWindow = buildDateWindow(
    ["joinedAt", "requestedAt", "createdAt"],
    startDate,
    endDate,
  );
  if (dateWindow) Object.assign(query, dateWindow);

  return query;
}

function buildLoanQuery({ startDate, endDate, excludedStatuses }) {
  const query = {
    userId: { $ne: null },
    status: { $nin: excludedStatuses },
  };

  const dateWindow = buildDateWindow(["createdAt"], startDate, endDate);
  if (dateWindow) Object.assign(query, dateWindow);

  return query;
}

function buildHistoricalPatch({
  payment,
  paymentStatus,
  historicalDate,
  note,
}) {
  const patch = {
    paymentStatus,
    submittedAt: historicalDate,
    createdAt: historicalDate,
    updatedAt: historicalDate,
  };

  if (paymentStatus === "paid" || paymentStatus === "defaulted") {
    patch.reviewedAt = historicalDate;
  } else {
    patch.reviewedAt = null;
  }

  if (!payment?.notes && note) {
    patch.notes = note;
  }

  return patch;
}

async function applyHistoricalPaymentState({
  payment,
  paymentStatus,
  historicalDate,
  note,
  channel,
}) {
  if (!payment?._id) return null;

  const updatedPayment = await FormPaymentModel.findByIdAndUpdate(
    payment._id,
    {
      $set: buildHistoricalPatch({
        payment,
        paymentStatus,
        historicalDate,
        note,
      }),
    },
    {
      new: true,
      runValidators: true,
      timestamps: false,
    },
  );

  if (!updatedPayment) return null;

  const transaction = await syncFormPaymentTransaction(updatedPayment, {
    actorProfileId: null,
    channel,
  });

  if (transaction?._id) {
    await TransactionModel.collection.updateOne(
      { _id: transaction._id },
      {
        $set: {
          date: historicalDate,
          updatedAt: historicalDate,
        },
      },
    );
  }

  await FormPaymentModel.collection.updateOne(
    { _id: updatedPayment._id },
    {
      $set: {
        submittedAt: historicalDate,
        reviewedAt:
          paymentStatus === "paid" || paymentStatus === "defaulted"
            ? historicalDate
            : null,
        createdAt: historicalDate,
        updatedAt: historicalDate,
      },
    },
  );

  return FormPaymentModel.findById(updatedPayment._id);
}

async function processMembershipBatch({
  batch,
  dryRun,
  paymentStatus,
  stats,
}) {
  if (batch.length === 0) return;

  const existingPayments = await FormPaymentModel.find({
    sourceModel: "GroupMembership",
    formType: "membership_registration",
    sourceId: { $in: batch.map((membership) => membership._id) },
  }).lean();
  const paymentBySourceId = new Map(
    existingPayments.map((payment) => [String(payment.sourceId), payment]),
  );
  const note = "Historical backfill: membership registration form payment already paid by existing member.";

  for (const membership of batch) {
    stats.membershipScanned += 1;

    try {
      const historicalDate = resolveMembershipPaymentDate(membership);
      const existingPayment = paymentBySourceId.get(String(membership._id));

      if (dryRun) {
        if (!existingPayment) stats.wouldCreate += 1;
        else stats.wouldUpdateExisting += 1;
        stats.wouldSyncTransaction += 1;
        continue;
      }

      const payment =
        existingPayment ||
        (await upsertMembershipFormPayment({
          membership,
          syncTransaction: false,
        }));

      if (!existingPayment && payment) stats.created += 1;
      if (existingPayment) stats.updatedExisting += 1;

      const updatedPayment = await applyHistoricalPaymentState({
        payment,
        paymentStatus,
        historicalDate,
        note,
        channel: "historical_membership_form_payment_backfill",
      });

      if (updatedPayment) stats.syncedTransactions += 1;
    } catch (error) {
      stats.failed += 1;
      if (stats.errors.length < 25) {
        stats.errors.push({
          source: "membership",
          sourceId: String(membership._id),
          userId: membership.userId ? String(membership.userId) : null,
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }
  }
}

async function processLoanBatch({ batch, dryRun, paymentStatus, stats }) {
  if (batch.length === 0) return;

  const existingPayments = await FormPaymentModel.find({
    sourceModel: "LoanApplication",
    sourceId: { $in: batch.map((loan) => loan._id) },
  }).lean();
  const paymentBySourceAndType = new Map(
    existingPayments.map((payment) => [
      `${String(payment.sourceId)}:${payment.formType}`,
      payment,
    ]),
  );
  const note = "Historical backfill: loan form payment already paid for existing loan application.";

  for (const loan of batch) {
    stats.loanScanned += 1;

    try {
      const config = resolveLoanFormPaymentConfig(loan.loanType);
      if (!config) {
        stats.skippedUnsupportedLoanType += 1;
        continue;
      }

      const historicalDate = resolveLoanPaymentDate(loan);
      const existingPayment = paymentBySourceAndType.get(
        `${String(loan._id)}:${config.formType}`,
      );

      if (dryRun) {
        if (!existingPayment) stats.wouldCreate += 1;
        else stats.wouldUpdateExisting += 1;
        stats.wouldSyncTransaction += 1;
        continue;
      }

      const payment =
        existingPayment ||
        (await upsertLoanFormPayment({
          application: loan,
          syncTransaction: false,
        }));

      if (!existingPayment && payment) stats.created += 1;
      if (existingPayment) stats.updatedExisting += 1;

      const updatedPayment = await applyHistoricalPaymentState({
        payment,
        paymentStatus,
        historicalDate,
        note,
        channel: "historical_loan_form_payment_backfill",
      });

      if (updatedPayment) stats.syncedTransactions += 1;
    } catch (error) {
      stats.failed += 1;
      if (stats.errors.length < 25) {
        stats.errors.push({
          source: "loan",
          sourceId: String(loan._id),
          userId: loan.userId ? String(loan.userId) : null,
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }
  }
}

async function processCursor({ cursor, batchSize, processBatch }) {
  let batch = [];
  for await (const item of cursor) {
    batch.push(item);
    if (batch.length >= batchSize) {
      await processBatch(batch);
      batch = [];
    }
  }
  await processBatch(batch);
}

const args = parseArgs(process.argv.slice(2));
const hasConfirmArg = Object.prototype.hasOwnProperty.call(args, "confirm");
const dryRun = args["dry-run"] || !CONFIRMATION_TOKENS.has(args.confirm);
const paymentStatus = resolvePaymentStatus(args["payment-status"]);
const startDate = parseDateArg(args.from, "from");
const endDate = parseOptionalEndDateArg(args);
const batchSize = resolveBatchSize(args["batch-size"]);
const membershipStatuses = resolveMembershipStatuses(args["membership-statuses"]);
const excludedLoanStatuses = parseCsv(args["exclude-loan-statuses"]);
const loanExcludedStatuses =
  excludedLoanStatuses.length > 0
    ? excludedLoanStatuses
    : DEFAULT_LOAN_EXCLUDED_STATUSES;
const mongoUri = process.env.MONGO_URI;

if (!mongoUri) {
  // eslint-disable-next-line no-console
  console.error("Missing MONGO_URI");
  process.exit(1);
}

if (hasConfirmArg && !CONFIRMATION_TOKENS.has(args.confirm)) {
  // eslint-disable-next-line no-console
  console.error(
    `Invalid --confirm value. Use --confirm ${PRIMARY_CONFIRMATION_TOKEN} to write records.`,
  );
  process.exit(1);
}

if (startDate && endDate && startDate.getTime() > endDate.getTime()) {
  // eslint-disable-next-line no-console
  console.error("The --from date cannot be after --to date");
  process.exit(1);
}

const stats = {
  dryRun,
  confirmationRequired: dryRun ? PRIMARY_CONFIRMATION_TOKEN : null,
  from: startDate ? startDate.toISOString() : null,
  to: endDate ? endDate.toISOString() : null,
  paymentStatus,
  membershipStatuses,
  loanExcludedStatuses,
  membershipScanned: 0,
  loanScanned: 0,
  wouldCreate: 0,
  wouldUpdateExisting: 0,
  wouldSyncTransaction: 0,
  created: 0,
  updatedExisting: 0,
  syncedTransactions: 0,
  skippedUnsupportedLoanType: 0,
  failed: 0,
  errors: [],
};

try {
  await connectMongo({ mongoUri });

  const membershipCursor = GroupMembershipModel.find(
    buildMembershipQuery({
      startDate,
      endDate,
      statuses: membershipStatuses,
    }),
  )
    .sort({ joinedAt: 1, requestedAt: 1, createdAt: 1, _id: 1 })
    .batchSize(batchSize)
    .lean()
    .cursor();

  await processCursor({
    cursor: membershipCursor,
    batchSize,
    processBatch: (batch) =>
      processMembershipBatch({
        batch,
        dryRun,
        paymentStatus,
        stats,
      }),
  });

  const loanCursor = LoanApplicationModel.find(
    buildLoanQuery({
      startDate,
      endDate,
      excludedStatuses: loanExcludedStatuses,
    }),
  )
    .sort({ createdAt: 1, _id: 1 })
    .batchSize(batchSize)
    .lean()
    .cursor();

  await processCursor({
    cursor: loanCursor,
    batchSize,
    processBatch: (batch) =>
      processLoanBatch({
        batch,
        dryRun,
        paymentStatus,
        stats,
      }),
  });

  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        ok: stats.failed === 0 ? 1 : 0,
        message: dryRun
          ? `Dry run only. Re-run with --confirm ${PRIMARY_CONFIRMATION_TOKEN} to write records.`
          : "Historical paid form payments backfilled successfully.",
        stats,
      },
      null,
      2,
    ),
  );
} catch (error) {
  // eslint-disable-next-line no-console
  console.error(
    JSON.stringify(
      {
        ok: 0,
        error: error instanceof Error ? error.message : String(error),
        stats,
      },
      null,
      2,
    ),
  );
  process.exitCode = 1;
} finally {
  await mongoose.disconnect();
}

if (stats.failed > 0) {
  process.exitCode = 1;
}
