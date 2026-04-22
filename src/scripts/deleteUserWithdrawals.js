import dotenv from "dotenv";

dotenv.config();

import { TransactionModel } from "../models/Transaction.js";
import { WithdrawalRequestModel } from "../models/WithdrawalRequest.js";
import {
  asObjectId,
  buildMixedIdValues,
  formatScriptError,
  mongoOptions,
  parseArgs,
  resolveUserContext,
  runWithOptionalTransaction,
  toIdStrings,
  withSession,
} from "./utils/userDataCleanup.js";

async function loadWithdrawalTransactions({
  profileId,
  withdrawalIds,
  payoutReferences,
  deleteAll,
  session,
}) {
  if (deleteAll) {
    return withSession(
      TransactionModel.find({
        userId: profileId,
        type: "withdrawal",
      }),
      session,
    ).lean();
  }

  const idValues = buildMixedIdValues(withdrawalIds);
  const references = [...new Set((payoutReferences || []).filter(Boolean))];
  const or = [];
  if (idValues.length > 0) {
    or.push({ "metadata.withdrawalRequestId": { $in: idValues } });
  }
  if (references.length > 0) {
    or.push({ reference: { $in: references } });
  }
  if (or.length === 0) return [];

  return withSession(
    TransactionModel.find({
      userId: profileId,
      type: "withdrawal",
      $or: or,
    }),
    session,
  ).lean();
}

async function executeWithdrawalCleanup({
  userId,
  profileId,
  withdrawalId = null,
  dryRun = false,
  useTransaction = true,
}) {
  return runWithOptionalTransaction({
    useTransaction,
    work: async (session) => {
      const { user, profile } = await resolveUserContext({
        userId,
        profileId,
        session,
      });

      const deleteAll = !withdrawalId;
      const withdrawalFilter = {
        userId: profile._id,
        ...(deleteAll ? {} : { _id: withdrawalId }),
      };

      const withdrawals = await withSession(
        WithdrawalRequestModel.find(withdrawalFilter).lean(),
        session,
      );

      if (withdrawals.length === 0) {
        throw new Error(
          deleteAll
            ? "No withdrawals found for this user"
            : `Withdrawal ${withdrawalId} not found for this user`,
        );
      }

      const withdrawalIds = withdrawals.map((withdrawal) => withdrawal._id);
      const payoutReferences = [...new Set(
        withdrawals
          .map((withdrawal) => String(withdrawal.payoutReference || "").trim())
          .filter(Boolean),
      )];

      const transactions = await loadWithdrawalTransactions({
        profileId: profile._id,
        withdrawalIds,
        payoutReferences,
        deleteAll,
        session,
      });

      const summary = {
        user: {
          userId: String(user._id),
          profileId: String(profile._id),
          email: user.email ?? null,
          fullName: profile.fullName ?? null,
        },
        mode: deleteAll ? "all" : "single",
        targetWithdrawalId: withdrawalId ? String(withdrawalId) : null,
        withdrawals: {
          matched: withdrawals.length,
          amountRemoved: withdrawals.reduce(
            (sum, withdrawal) => sum + Number(withdrawal.amount ?? 0),
            0,
          ),
        },
        transactions: {
          matched: transactions.length,
        },
        dryRun,
      };

      if (dryRun) {
        return summary;
      }

      const [transactionDeleteResult, withdrawalDeleteResult] = await Promise.all([
        transactions.length > 0
          ? TransactionModel.deleteMany(
              { _id: { $in: transactions.map((transaction) => transaction._id) } },
              mongoOptions(session),
            )
          : Promise.resolve({ deletedCount: 0 }),
        WithdrawalRequestModel.deleteMany(
          { _id: { $in: withdrawalIds } },
          mongoOptions(session),
        ),
      ]);

      return {
        ...summary,
        transactions: {
          ...summary.transactions,
          deleted: Number(transactionDeleteResult?.deletedCount ?? 0),
        },
        withdrawals: {
          ...summary.withdrawals,
          deleted: Number(withdrawalDeleteResult?.deletedCount ?? 0),
        },
      };
    },
  });
}

const args = parseArgs(process.argv.slice(2));

const runCli = async () => {
  if (args.help || args.h) {
    // eslint-disable-next-line no-console
    console.log(
      [
        "Usage:",
        "  node src/scripts/deleteUserWithdrawals.js --userId <userId> --profileId <profileId> [--withdrawalId <withdrawalId>] [--dry-run] [--no-transaction]",
        "",
        "Notes:",
        "  Without --withdrawalId, the script deletes the user's entire withdrawal history and corresponding withdrawal transactions.",
        "  Single-record delete targets one withdrawal request and its matching payout transaction records.",
      ].join("\n"),
    );
    process.exit(0);
  }

  const userId = asObjectId(args.userId, "userId");
  const profileId = asObjectId(args.profileId, "profileId");
  const withdrawalId = args.withdrawalId
    ? asObjectId(args.withdrawalId, "withdrawalId")
    : null;
  const dryRun = Boolean(args["dry-run"]);
  const useTransaction = !Boolean(args["no-transaction"]);

  const result = await executeWithdrawalCleanup({
    userId,
    profileId,
    withdrawalId,
    dryRun,
    useTransaction,
  });

  // eslint-disable-next-line no-console
  console.log(JSON.stringify({ ok: 1, result }, null, 2));
};

runCli().catch((error) => {
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
  process.exit(1);
});
