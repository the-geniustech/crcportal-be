import dotenv from "dotenv";

dotenv.config();

import mongoose from "mongoose";
import { connectMongo } from "../db.js";
import { FormPaymentModel, FormPaymentStatuses } from "../models/FormPayment.js";
import { TransactionModel } from "../models/Transaction.js";
import { syncFormPaymentTransaction } from "../services/formPaymentService.js";

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

function parseLimit(value) {
  if (value === undefined || value === null || value === true) return null;
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    throw new Error("Invalid --limit value");
  }
  return parsed;
}

function buildQuery(args) {
  const query = {};

  const status = String(args.status || "all").trim();
  if (status && status !== "all") {
    if (!FormPaymentStatuses.includes(status)) {
      throw new Error("Invalid --status value");
    }
    query.paymentStatus = status;
  }

  if (args["missing-only"]) {
    query.$or = [
      { transactionId: null },
      { transactionId: { $exists: false } },
      { transactionReference: null },
      { transactionReference: { $exists: false } },
      { transactionReference: "" },
    ];
  }

  return query;
}

const args = parseArgs(process.argv.slice(2));
const isDryRun = Boolean(args["dry-run"]);
const missingOnly = Boolean(args["missing-only"]);
const limit = parseLimit(args.limit);
const mongoUri = process.env.MONGO_URI;

if (!mongoUri) {
  // eslint-disable-next-line no-console
  console.error("Missing MONGO_URI");
  process.exit(1);
}

const stats = {
  dryRun: isDryRun,
  missingOnly,
  status: args.status || "all",
  limit,
  scanned: 0,
  missingTransactions: 0,
  synced: 0,
  failed: 0,
  errors: [],
};

try {
  await connectMongo({ mongoUri });

  const query = buildQuery(args);
  let paymentQuery = FormPaymentModel.find(query)
    .sort({ submittedAt: 1, createdAt: 1, _id: 1 })
    .batchSize(100);

  if (limit) paymentQuery = paymentQuery.limit(limit);

  const cursor = paymentQuery.cursor();

  for await (const payment of cursor) {
    stats.scanned += 1;

    try {
      const existingTx = await TransactionModel.exists({
        type: "form_payment",
        "metadata.formPaymentId": payment._id,
      });

      if (!existingTx) stats.missingTransactions += 1;

      if (isDryRun) {
        stats.synced += 1;
        continue;
      }

      await syncFormPaymentTransaction(payment, {
        actorProfileId: payment.reviewedBy || null,
        channel: "historical_sync",
      });
      stats.synced += 1;
    } catch (error) {
      stats.failed += 1;
      if (stats.errors.length < 20) {
        stats.errors.push({
          paymentId: String(payment._id),
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }
  }

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
