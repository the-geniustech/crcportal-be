import dotenv from "dotenv";

dotenv.config();

import mongoose from "mongoose";
import { AuditLogModel } from "../models/AuditLog.js";
import { FormPaymentModel } from "../models/FormPayment.js";
import { TransactionModel } from "../models/Transaction.js";
import {
  formatScriptError,
  mongoOptions,
  parseArgs,
  runWithOptionalTransaction,
  withSession,
} from "./utils/userDataCleanup.js";

const CONFIRMATION_TOKEN = "CLEAR_FORM_PAYMENTS";

async function collectFormPaymentTransactionLinks(session) {
  const [transactionIds, transactionReferences] = await Promise.all([
    withSession(
      FormPaymentModel.distinct("transactionId", {
        transactionId: { $ne: null },
      }),
      session,
    ),
    withSession(
      FormPaymentModel.distinct("transactionReference", {
        transactionReference: { $nin: [null, ""] },
      }),
      session,
    ),
  ]);

  return {
    transactionIds: transactionIds.filter(Boolean),
    transactionReferences: transactionReferences.filter(Boolean),
  };
}

function buildFormPaymentTransactionFilter({
  transactionIds = [],
  transactionReferences = [],
} = {}) {
  const or = [
    { type: "form_payment" },
    { reference: /^CRC-FORM-/ },
    { "metadata.paymentType": "form_payment" },
    { "metadata.formPaymentId": { $exists: true } },
  ];

  if (transactionIds.length > 0) {
    or.push({ _id: { $in: transactionIds } });
  }

  if (transactionReferences.length > 0) {
    or.push({ reference: { $in: transactionReferences } });
  }

  return { $or: or };
}

function buildFormPaymentAuditLogFilter() {
  return {
    $or: [
      { entityType: "formPayment" },
      { action: "admin.form_payment.update" },
    ],
  };
}

async function countCleanupTargets(session, { includeAuditLogs = false } = {}) {
  const links = await collectFormPaymentTransactionLinks(session);
  const transactionFilter = buildFormPaymentTransactionFilter(links);
  const auditLogFilter = buildFormPaymentAuditLogFilter();
  const [formPayments, transactions, auditLogs] = await Promise.all([
    withSession(FormPaymentModel.countDocuments({}), session),
    withSession(TransactionModel.countDocuments(transactionFilter), session),
    includeAuditLogs
      ? withSession(AuditLogModel.countDocuments(auditLogFilter), session)
      : Promise.resolve(0),
  ]);

  return {
    formPayments,
    transactions,
    auditLogs,
    linkedTransactionIds: links.transactionIds.length,
    linkedTransactionReferences: links.transactionReferences.length,
  };
}

async function executeCleanup({ dryRun, includeAuditLogs, useTransaction }) {
  return runWithOptionalTransaction({
    useTransaction,
    work: async (session) => {
      const before = await countCleanupTargets(session, { includeAuditLogs });

      if (dryRun) {
        return {
          dryRun: true,
          confirmationRequired: CONFIRMATION_TOKEN,
          includeAuditLogs,
          before,
          deleted: {
            formPayments: 0,
            transactions: 0,
            auditLogs: 0,
          },
          after: before,
        };
      }

      const links = await collectFormPaymentTransactionLinks(session);
      const transactionFilter = buildFormPaymentTransactionFilter(links);
      const auditLogFilter = buildFormPaymentAuditLogFilter();

      const deletedTransactions = await withSession(
        TransactionModel.deleteMany(transactionFilter),
        session,
      );
      const deletedFormPayments = await withSession(
        FormPaymentModel.deleteMany({}, mongoOptions(session)),
        session,
      );
      const deletedAuditLogs = includeAuditLogs
        ? await withSession(AuditLogModel.deleteMany(auditLogFilter), session)
        : { deletedCount: 0 };
      const after = await countCleanupTargets(session, { includeAuditLogs });

      return {
        dryRun: false,
        includeAuditLogs,
        before,
        deleted: {
          formPayments: deletedFormPayments.deletedCount ?? 0,
          transactions: deletedTransactions.deletedCount ?? 0,
          auditLogs: deletedAuditLogs.deletedCount ?? 0,
        },
        after,
      };
    },
  });
}

const args = parseArgs(process.argv.slice(2));
const useTransaction = !args["no-transaction"];
const includeAuditLogs = Boolean(args["include-audit-logs"]);
const hasConfirmArg = Object.prototype.hasOwnProperty.call(args, "confirm");
const isConfirmed = args.confirm === CONFIRMATION_TOKEN;
const dryRun = Boolean(args["dry-run"]) || !isConfirmed;

if (hasConfirmArg && !isConfirmed) {
  // eslint-disable-next-line no-console
  console.error(
    JSON.stringify(
      {
        ok: 0,
        error: `Invalid --confirm value. Use --confirm ${CONFIRMATION_TOKEN} to permanently delete all form payment records.`,
      },
      null,
      2,
    ),
  );
  process.exit(1);
}

try {
  const result = await executeCleanup({
    dryRun,
    includeAuditLogs,
    useTransaction,
  });
  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        ok: 1,
        message: dryRun
          ? `Dry run only. Re-run with --confirm ${CONFIRMATION_TOKEN} to delete these records.`
          : includeAuditLogs
            ? "All form payments, matching form-payment transactions, and form-payment audit logs were deleted."
            : "All form payments and matching form-payment transactions were deleted. Audit logs were left untouched.",
        result,
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
        error: formatScriptError(error),
      },
      null,
      2,
    ),
  );
  process.exitCode = 1;
} finally {
  if (mongoose.connection.readyState !== 0) {
    await mongoose.disconnect();
  }
}
