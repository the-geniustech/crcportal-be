import dotenv from "dotenv";

dotenv.config();

import { GuarantorNotificationModel } from "../models/GuarantorNotification.js";
import { LoanApplicationEditRequestModel } from "../models/LoanApplicationEditRequest.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import { LoanGuarantorModel } from "../models/LoanGuarantor.js";
import { LoanRepaymentScheduleItemModel } from "../models/LoanRepaymentScheduleItem.js";
import { NotificationModel } from "../models/Notification.js";
import { RecurringPaymentModel } from "../models/RecurringPayment.js";
import { TransactionModel } from "../models/Transaction.js";
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

const LOAN_NOTIFICATION_TYPES = [
  "loan_application",
  "loan_disbursed",
  "loan_edit_request",
  "loan_status",
];

async function collectLoanIds({ profileId, specificLoanId, session }) {
  if (specificLoanId) {
    return [specificLoanId];
  }

  const [applications, transactions, recurringPayments, editRequests, notifications] =
    await Promise.all([
      withSession(
        LoanApplicationModel.find({ userId: profileId }, { _id: 1 }).lean(),
        session,
      ),
      withSession(
        TransactionModel.find(
          {
            userId: profileId,
            $or: [
              { type: "loan_disbursement" },
              { type: "loan_repayment" },
              { loanId: { $type: "objectId" } },
            ],
          },
          { loanId: 1 },
        ).lean(),
        session,
      ),
      withSession(
        RecurringPaymentModel.find(
          {
            userId: profileId,
            paymentType: "loan_repayment",
          },
          { loanId: 1 },
        ).lean(),
        session,
      ),
      withSession(
        LoanApplicationEditRequestModel.find(
          { userId: profileId },
          { loanApplicationId: 1 },
        ).lean(),
        session,
      ),
      withSession(
        NotificationModel.find(
          {
            userId: profileId,
            $or: [
              { type: { $in: LOAN_NOTIFICATION_TYPES } },
              { "metadata.loanId": { $exists: true } },
            ],
          },
          { metadata: 1 },
        ).lean(),
        session,
      ),
    ]);

  return toIdStrings([
    ...applications.map((application) => application._id),
    ...transactions.map((transaction) => transaction.loanId),
    ...recurringPayments.map((payment) => payment.loanId),
    ...editRequests.map((request) => request.loanApplicationId),
    ...notifications.map((notification) => notification?.metadata?.loanId),
  ]);
}

async function executeLoanCleanup({
  userId,
  profileId,
  loanApplicationId = null,
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

      const specificLoanId = loanApplicationId ? String(loanApplicationId) : null;
      let loanApplications = [];

      if (specificLoanId) {
        loanApplications = await withSession(
          LoanApplicationModel.find({
            _id: specificLoanId,
            userId: profile._id,
          }).lean(),
          session,
        );

        if (loanApplications.length === 0) {
          throw new Error(
            `Loan application ${loanApplicationId} not found for this user`,
          );
        }
      } else {
        loanApplications = await withSession(
          LoanApplicationModel.find({ userId: profile._id }).lean(),
          session,
        );
      }

      const loanIds = await collectLoanIds({
        profileId: profile._id,
        specificLoanId,
        session,
      });

      const mixedLoanIds = buildMixedIdValues(loanIds);
      const loanTransactionsFilter = specificLoanId
        ? {
            userId: profile._id,
            loanId: specificLoanId,
          }
        : {
            userId: profile._id,
            $or: [
              { loanId: { $in: loanIds } },
              { type: "loan_disbursement" },
              { type: "loan_repayment" },
            ],
          };
      const recurringPaymentsFilter = specificLoanId
        ? {
            userId: profile._id,
            loanId: specificLoanId,
          }
        : {
            userId: profile._id,
            paymentType: "loan_repayment",
          };
      const editRequestsFilter = specificLoanId
        ? {
            userId: profile._id,
            loanApplicationId: specificLoanId,
          }
        : {
            userId: profile._id,
          };
      const notificationsFilter = specificLoanId
        ? {
            userId: profile._id,
            "metadata.loanId": { $in: mixedLoanIds },
          }
        : {
            userId: profile._id,
            $or: [
              { "metadata.loanId": { $in: mixedLoanIds } },
              { type: { $in: LOAN_NOTIFICATION_TYPES } },
            ],
          };

      const [
        transactions,
        recurringPayments,
        editRequests,
        notifications,
        repaymentScheduleItems,
        guarantors,
      ] = await Promise.all([
        withSession(TransactionModel.find(loanTransactionsFilter).lean(), session),
        withSession(RecurringPaymentModel.find(recurringPaymentsFilter).lean(), session),
        withSession(
          LoanApplicationEditRequestModel.find(editRequestsFilter).lean(),
          session,
        ),
        withSession(NotificationModel.find(notificationsFilter).lean(), session),
        loanIds.length > 0
          ? withSession(
              LoanRepaymentScheduleItemModel.find({
                loanApplicationId: { $in: loanIds },
              }).lean(),
              session,
            )
          : Promise.resolve([]),
        loanIds.length > 0
          ? withSession(
              LoanGuarantorModel.find({
                loanApplicationId: { $in: loanIds },
              }).lean(),
              session,
            )
          : Promise.resolve([]),
      ]);

      const guarantorIds = guarantors.map((guarantor) => guarantor._id);
      const guarantorNotifications = guarantorIds.length > 0
        ? await withSession(
            GuarantorNotificationModel.find({
              guarantorId: { $in: guarantorIds },
            }).lean(),
            session,
          )
        : [];

      const summary = {
        user: {
          userId: String(user._id),
          profileId: String(profile._id),
          email: user.email ?? null,
          fullName: profile.fullName ?? null,
        },
        mode: specificLoanId ? "single" : "all",
        targetLoanApplicationId: specificLoanId,
        loans: {
          matched: loanApplications.length,
          loanIds,
        },
        repaymentScheduleItems: {
          matched: repaymentScheduleItems.length,
        },
        guarantors: {
          matched: guarantors.length,
        },
        guarantorNotifications: {
          matched: guarantorNotifications.length,
        },
        editRequests: {
          matched: editRequests.length,
        },
        recurringPayments: {
          matched: recurringPayments.length,
        },
        transactions: {
          matched: transactions.length,
        },
        notifications: {
          matched: notifications.length,
        },
        dryRun,
      };

      if (
        !specificLoanId &&
        loanApplications.length === 0 &&
        repaymentScheduleItems.length === 0 &&
        guarantors.length === 0 &&
        guarantorNotifications.length === 0 &&
        editRequests.length === 0 &&
        recurringPayments.length === 0 &&
        transactions.length === 0 &&
        notifications.length === 0
      ) {
        throw new Error("No loan records found for this user");
      }

      if (dryRun) {
        return summary;
      }

      const [
        guarantorNotificationDeleteResult,
        guarantorDeleteResult,
        scheduleDeleteResult,
        editRequestDeleteResult,
        recurringDeleteResult,
        transactionDeleteResult,
        notificationDeleteResult,
        loanDeleteResult,
      ] = await Promise.all([
        guarantorNotifications.length > 0
          ? GuarantorNotificationModel.deleteMany(
              { _id: { $in: guarantorNotifications.map((item) => item._id) } },
              mongoOptions(session),
            )
          : Promise.resolve({ deletedCount: 0 }),
        guarantors.length > 0
          ? LoanGuarantorModel.deleteMany(
              { _id: { $in: guarantors.map((item) => item._id) } },
              mongoOptions(session),
            )
          : Promise.resolve({ deletedCount: 0 }),
        repaymentScheduleItems.length > 0
          ? LoanRepaymentScheduleItemModel.deleteMany(
              {
                _id: { $in: repaymentScheduleItems.map((item) => item._id) },
              },
              mongoOptions(session),
            )
          : Promise.resolve({ deletedCount: 0 }),
        editRequests.length > 0
          ? LoanApplicationEditRequestModel.deleteMany(
              { _id: { $in: editRequests.map((item) => item._id) } },
              mongoOptions(session),
            )
          : Promise.resolve({ deletedCount: 0 }),
        recurringPayments.length > 0
          ? RecurringPaymentModel.deleteMany(
              { _id: { $in: recurringPayments.map((item) => item._id) } },
              mongoOptions(session),
            )
          : Promise.resolve({ deletedCount: 0 }),
        transactions.length > 0
          ? TransactionModel.deleteMany(
              { _id: { $in: transactions.map((item) => item._id) } },
              mongoOptions(session),
            )
          : Promise.resolve({ deletedCount: 0 }),
        notifications.length > 0
          ? NotificationModel.deleteMany(
              { _id: { $in: notifications.map((item) => item._id) } },
              mongoOptions(session),
            )
          : Promise.resolve({ deletedCount: 0 }),
        loanApplications.length > 0
          ? LoanApplicationModel.deleteMany(
              { _id: { $in: loanApplications.map((item) => item._id) } },
              mongoOptions(session),
            )
          : Promise.resolve({ deletedCount: 0 }),
      ]);

      return {
        ...summary,
        loans: {
          ...summary.loans,
          deleted: Number(loanDeleteResult?.deletedCount ?? 0),
        },
        repaymentScheduleItems: {
          ...summary.repaymentScheduleItems,
          deleted: Number(scheduleDeleteResult?.deletedCount ?? 0),
        },
        guarantors: {
          ...summary.guarantors,
          deleted: Number(guarantorDeleteResult?.deletedCount ?? 0),
        },
        guarantorNotifications: {
          ...summary.guarantorNotifications,
          deleted: Number(guarantorNotificationDeleteResult?.deletedCount ?? 0),
        },
        editRequests: {
          ...summary.editRequests,
          deleted: Number(editRequestDeleteResult?.deletedCount ?? 0),
        },
        recurringPayments: {
          ...summary.recurringPayments,
          deleted: Number(recurringDeleteResult?.deletedCount ?? 0),
        },
        transactions: {
          ...summary.transactions,
          deleted: Number(transactionDeleteResult?.deletedCount ?? 0),
        },
        notifications: {
          ...summary.notifications,
          deleted: Number(notificationDeleteResult?.deletedCount ?? 0),
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
        "  node src/scripts/deleteUserLoans.js --userId <userId> --profileId <profileId> [--loanApplicationId <loanApplicationId>] [--dry-run] [--no-transaction]",
        "",
        "Notes:",
        "  Without --loanApplicationId, the script deletes the user's full loan domain data.",
        "  Cleanup includes loan applications, repayment schedules, guarantors, guarantor notifications, edit requests, borrower notifications, recurring loan payments, and loan transactions.",
        "  Single-record mode targets one loan application and its related records.",
      ].join("\n"),
    );
    process.exit(0);
  }

  const userId = asObjectId(args.userId, "userId");
  const profileId = asObjectId(args.profileId, "profileId");
  const loanApplicationId = args.loanApplicationId
    ? asObjectId(args.loanApplicationId, "loanApplicationId")
    : null;
  const dryRun = Boolean(args["dry-run"]);
  const useTransaction = !Boolean(args["no-transaction"]);

  const result = await executeLoanCleanup({
    userId,
    profileId,
    loanApplicationId,
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
