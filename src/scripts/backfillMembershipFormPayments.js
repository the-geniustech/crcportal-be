import dotenv from "dotenv";

dotenv.config();

import mongoose from "mongoose";
import { connectMongo } from "../db.js";
import { FormPaymentModel } from "../models/FormPayment.js";
import { TransactionModel } from "../models/Transaction.js";
import {
  GroupMembershipModel,
  GroupMembershipStatuses,
} from "../models/GroupMembership.js";
import {
  syncFormPaymentTransaction,
  upsertMembershipFormPayment,
} from "../services/formPaymentService.js";

const DEFAULT_STATUSES = ["active", "inactive", "suspended"];
const DEFAULT_BATCH_SIZE = 100;
const DEFAULT_NOTE =
  "Historical backfill: membership registration form payment was already paid before the form-payment tracker was introduced.";

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

function resolveStatuses(args) {
  const explicit = parseCsv(args.statuses);
  const statuses = explicit.length > 0 ? explicit : [...DEFAULT_STATUSES];

  if (args["include-pending"] && !statuses.includes("pending")) {
    statuses.push("pending");
  }

  const invalid = statuses.filter(
    (status) => !GroupMembershipStatuses.includes(status),
  );
  if (invalid.length > 0) {
    throw new Error(`Invalid membership status: ${invalid.join(", ")}`);
  }

  return statuses;
}

function resolveHistoricalSubmittedAt(membership) {
  return (
    membership.requestedAt ||
    membership.joinedAt ||
    membership.createdAt ||
    new Date()
  );
}

function resolveHistoricalPaidAt(membership) {
  return (
    membership.joinedAt ||
    membership.reviewedAt ||
    membership.requestedAt ||
    membership.createdAt ||
    new Date()
  );
}

function buildQuery({ statuses, args }) {
  const query = {
    status: { $in: statuses },
    userId: { $ne: null },
  };

  if (args["group-id"]) {
    if (!mongoose.isValidObjectId(args["group-id"])) {
      throw new Error("Invalid --group-id value");
    }
    query.groupId = new mongoose.Types.ObjectId(args["group-id"]);
  }

  if (args["profile-id"]) {
    if (!mongoose.isValidObjectId(args["profile-id"])) {
      throw new Error("Invalid --profile-id value");
    }
    query.userId = new mongoose.Types.ObjectId(args["profile-id"]);
  }

  return query;
}

function parseLimit(value) {
  if (value === undefined || value === null || value === true) return null;
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    throw new Error("Invalid --limit value");
  }
  return parsed;
}

const args = parseArgs(process.argv.slice(2));
const isDryRun = Boolean(args["dry-run"]);
const refreshPaidSnapshots = Boolean(args.refresh);
const overwriteNotes = Boolean(args["overwrite-notes"]);
const statuses = resolveStatuses(args);
const limit = parseLimit(args.limit);
const mongoUri = process.env.MONGO_URI;

if (!mongoUri) {
  // eslint-disable-next-line no-console
  console.error("Missing MONGO_URI");
  process.exit(1);
}

const stats = {
  dryRun: isDryRun,
  statuses,
  limit,
  scanned: 0,
  missingPaymentRecords: 0,
  createdPaymentRecords: 0,
  markedPaid: 0,
  refreshedPaidRecords: 0,
  missingTransactions: 0,
  syncedTransactions: 0,
  alreadyPaid: 0,
  skippedPaid: 0,
  failed: 0,
  errors: [],
};

try {
  await connectMongo({ mongoUri });

  const query = buildQuery({ statuses, args });
  let membershipQuery = GroupMembershipModel.find(query)
    .sort({ joinedAt: 1, requestedAt: 1, createdAt: 1, _id: 1 })
    .batchSize(100);

  if (limit) {
    membershipQuery = membershipQuery.limit(limit);
  }

  const cursor = membershipQuery.cursor();

  async function processBatch(memberships) {
    if (memberships.length === 0) return;

    const existingPayments = await FormPaymentModel.find(
      {
        sourceModel: "GroupMembership",
        formType: "membership_registration",
        sourceId: { $in: memberships.map((membership) => membership._id) },
      },
      { sourceId: 1, paymentStatus: 1, notes: 1 },
    ).lean();
    const existingByMembershipId = new Map(
      existingPayments.map((payment) => [String(payment.sourceId), payment]),
    );
    const existingPaymentIds = existingPayments.map((payment) => payment._id);
    const existingTransactions =
      existingPaymentIds.length > 0
        ? await TransactionModel.find(
            {
              type: "form_payment",
              "metadata.formPaymentId": { $in: existingPaymentIds },
            },
            { "metadata.formPaymentId": 1 },
          ).lean()
        : [];
    const transactionByPaymentId = new Set(
      existingTransactions
        .map((transaction) => transaction?.metadata?.formPaymentId)
        .filter(Boolean)
        .map(String),
    );

    for (const membership of memberships) {
      stats.scanned += 1;

      try {
        const existing = existingByMembershipId.get(String(membership._id));
        const hasPaidRecord = existing?.paymentStatus === "paid";
        const hasTransaction =
          existing && transactionByPaymentId.has(String(existing._id));

        if (!existing) {
          stats.missingPaymentRecords += 1;
        }

        if (existing && !hasTransaction) {
          stats.missingTransactions += 1;
        }

        if (
          hasPaidRecord &&
          hasTransaction &&
          !refreshPaidSnapshots &&
          !overwriteNotes
        ) {
          stats.alreadyPaid += 1;
          stats.skippedPaid += 1;
          continue;
        }

        if (isDryRun) {
          if (!hasPaidRecord) stats.markedPaid += 1;
          if (hasPaidRecord && refreshPaidSnapshots) {
            stats.refreshedPaidRecords += 1;
          }
          if (!hasTransaction) stats.syncedTransactions += 1;
          continue;
        }

        const payment = await upsertMembershipFormPayment({ membership });
        if (!existing) {
          stats.createdPaymentRecords += 1;
        }

        const submittedAt = resolveHistoricalSubmittedAt(membership);
        const paidAt = resolveHistoricalPaidAt(membership);
        const update = {
          paymentStatus: "paid",
          submittedAt,
          reviewedAt: paidAt,
        };

        if (membership.reviewedBy) {
          update.reviewedBy = membership.reviewedBy;
        }

        if (overwriteNotes || !payment.notes) {
          update.notes = DEFAULT_NOTE;
        }

        await FormPaymentModel.updateOne({ _id: payment._id }, { $set: update });
        await syncFormPaymentTransaction(payment._id, {
          actorProfileId: membership.reviewedBy || null,
          channel: "historical_backfill",
        });
        stats.syncedTransactions += 1;

        if (payment.paymentStatus === "paid") {
          stats.refreshedPaidRecords += 1;
        } else {
          stats.markedPaid += 1;
        }
      } catch (error) {
        stats.failed += 1;
        if (stats.errors.length < 20) {
          stats.errors.push({
            membershipId: String(membership._id),
            userId: membership.userId ? String(membership.userId) : null,
            groupId: membership.groupId ? String(membership.groupId) : null,
            message: error instanceof Error ? error.message : String(error),
          });
        }
      }
    }
  }

  let batch = [];
  for await (const membership of cursor) {
    batch.push(membership);
    if (batch.length >= DEFAULT_BATCH_SIZE) {
      await processBatch(batch);
      batch = [];
    }
  }
  await processBatch(batch);

  // eslint-disable-next-line no-console
  console.log(JSON.stringify({ ok: stats.failed === 0 ? 1 : 0, stats }, null, 2));
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
