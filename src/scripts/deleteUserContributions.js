import dotenv from "dotenv";

dotenv.config();

import { ContributionModel } from "../models/Contribution.js";
import { ContributionSettingModel } from "../models/ContributionSetting.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { GroupModel } from "../models/Group.js";
import { ProfileModel } from "../models/Profile.js";
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

const COUNTED_CONTRIBUTION_STATUSES = ["completed", "verified"];

function sumAmounts(items, field = "amount") {
  return (Array.isArray(items) ? items : []).reduce(
    (sum, item) => sum + Number(item?.[field] ?? 0),
    0,
  );
}

async function recomputeGroupSnapshot(groupId, session) {
  const [memberCount, savingsAgg] = await Promise.all([
    withSession(
      GroupMembershipModel.countDocuments({
        groupId,
        status: "active",
      }),
      session,
    ),
    withSession(
      ContributionModel.aggregate([
        {
          $match: {
            groupId,
            status: { $in: COUNTED_CONTRIBUTION_STATUSES },
          },
        },
        {
          $group: {
            _id: null,
            totalSavings: { $sum: "$amount" },
          },
        },
      ]),
      session,
    ),
  ]);

  const totalSavings = Number(savingsAgg?.[0]?.totalSavings ?? 0);
  await GroupModel.updateOne(
    { _id: groupId },
    {
      $set: {
        memberCount,
        totalSavings,
      },
    },
    mongoOptions(session),
  );

  return { groupId: String(groupId), memberCount, totalSavings };
}

async function recomputeMembershipContributionTotal(profileId, groupId, session) {
  const agg = await withSession(
    ContributionModel.aggregate([
      {
        $match: {
          userId: profileId,
          groupId,
          status: { $in: COUNTED_CONTRIBUTION_STATUSES },
        },
      },
      {
        $group: {
          _id: null,
          totalContributed: { $sum: "$amount" },
        },
      },
    ]),
    session,
  );

  const totalContributed = Number(agg?.[0]?.totalContributed ?? 0);
  await GroupMembershipModel.updateOne(
    { userId: profileId, groupId },
    { $set: { totalContributed } },
    mongoOptions(session),
  );

  return { groupId: String(groupId), totalContributed };
}

async function reconcileContributionSettings({ profileId, groupIds, session }) {
  const normalizedGroupIds = toIdStrings(groupIds);
  if (normalizedGroupIds.length === 0) {
    return { matched: 0, updated: 0 };
  }

  const settings = await withSession(
    ContributionSettingModel.find({
      userId: profileId,
      groupId: { $in: normalizedGroupIds },
    }).lean(),
    session,
  );

  if (settings.length === 0) {
    return { matched: 0, updated: 0 };
  }

  const actualsAgg = await withSession(
    ContributionModel.aggregate([
      {
        $match: {
          userId: profileId,
          groupId: { $in: normalizedGroupIds },
          status: { $in: COUNTED_CONTRIBUTION_STATUSES },
        },
      },
      {
        $group: {
          _id: {
            groupId: "$groupId",
            year: "$year",
            contributionType: "$contributionType",
          },
          totalActual: { $sum: "$amount" },
        },
      },
    ]),
    session,
  );

  const actualByKey = new Map(
    actualsAgg.map((entry) => [
      `${String(entry?._id?.groupId)}:${Number(entry?._id?.year ?? 0)}:${String(
        entry?._id?.contributionType || "revolving",
      )}`,
      Number(entry?.totalActual ?? 0),
    ]),
  );

  const ops = settings.map((setting) => {
    const key = `${String(setting.groupId)}:${Number(setting.year ?? 0)}:${String(
      setting.contributionType || "revolving",
    )}`;
    const totalActual = Number(actualByKey.get(key) ?? 0);
    const totalExpected = Number(setting.totalExpected ?? 0);
    const outstandingBalance = Math.max(totalExpected - totalActual, 0);

    return {
      updateOne: {
        filter: { _id: setting._id },
        update: {
          $set: {
            totalActual,
            outstandingBalance,
          },
        },
      },
    };
  });

  if (ops.length > 0) {
    await ContributionSettingModel.bulkWrite(ops, {
      ordered: false,
      ...mongoOptions(session),
    });
  }

  return { matched: settings.length, updated: ops.length };
}

async function loadContributionTransactions({
  profileId,
  contributionIds,
  paymentReferences,
  deleteAll,
  session,
}) {
  if (deleteAll) {
    return withSession(
      TransactionModel.find({
        userId: profileId,
        type: "group_contribution",
      }),
      session,
    ).lean();
  }

  const idValues = buildMixedIdValues(contributionIds);
  const references = [...new Set((paymentReferences || []).filter(Boolean))];
  const or = [];
  if (idValues.length > 0) {
    or.push({ "metadata.contributionId": { $in: idValues } });
    or.push({ "metadata.bulkContributionIds": { $in: idValues } });
  }
  if (references.length > 0) {
    or.push({ reference: { $in: references } });
  }
  if (or.length === 0) return [];

  return withSession(
    TransactionModel.find({
      userId: profileId,
      type: "group_contribution",
      $or: or,
    }),
    session,
  ).lean();
}

async function reconcileContributionTransactions({
  profileId,
  deletedContributionIds,
  deletedContributionObjectIds,
  candidateTransactions,
  session,
  dryRun = false,
}) {
  const deletedIdSet = new Set(toIdStrings(deletedContributionIds));
  const deletedObjectIds = Array.isArray(deletedContributionObjectIds)
    ? deletedContributionObjectIds
    : [];
  const stats = {
    matched: candidateTransactions.length,
    deleted: 0,
    updated: 0,
  };

  for (const transaction of candidateTransactions) {
    const metadata =
      transaction?.metadata && typeof transaction.metadata === "object"
        ? { ...transaction.metadata }
        : {};
    const referencedIds = toIdStrings([
      metadata.contributionId,
      ...(Array.isArray(metadata.bulkContributionIds)
        ? metadata.bulkContributionIds
        : []),
    ]);

    let remainingContributions = [];
    if (referencedIds.length > 0) {
      const remainingIds = referencedIds.filter((id) => !deletedIdSet.has(id));
      if (remainingIds.length > 0) {
        remainingContributions = await withSession(
          ContributionModel.find({
            _id: { $in: remainingIds },
            userId: profileId,
          }).lean(),
          session,
        );
      }
    } else if (transaction.reference) {
      remainingContributions = await withSession(
        ContributionModel.find({
          userId: profileId,
          paymentReference: transaction.reference,
          _id: { $nin: deletedObjectIds },
        }).lean(),
        session,
      );
    }

    if (remainingContributions.length === 0) {
      stats.deleted += 1;
      if (!dryRun) {
        await TransactionModel.deleteOne(
          { _id: transaction._id },
          mongoOptions(session),
        );
      }
      continue;
    }

    const nextContributionIds = remainingContributions.map(
      (contribution) => contribution._id,
    );
    const uniqueGroupIds = toIdStrings(
      remainingContributions.map((contribution) => contribution.groupId),
    );
    let groupId = null;
    let groupName = null;

    if (uniqueGroupIds.length === 1) {
      groupId = remainingContributions[0].groupId;
      const group = await withSession(
        GroupModel.findById(groupId, { groupName: 1 }).lean(),
        session,
      );
      groupName = group?.groupName ?? null;
    }

    if (nextContributionIds.length === 1) {
      metadata.contributionId = nextContributionIds[0];
    } else {
      delete metadata.contributionId;
    }

    if (
      nextContributionIds.length > 1 ||
      Array.isArray(transaction?.metadata?.bulkContributionIds)
    ) {
      metadata.bulkContributionIds = nextContributionIds;
    } else {
      delete metadata.bulkContributionIds;
    }

    stats.updated += 1;
    if (!dryRun) {
      await TransactionModel.updateOne(
        { _id: transaction._id },
        {
          $set: {
            amount: sumAmounts(remainingContributions),
            groupId,
            groupName,
            metadata,
          },
        },
        mongoOptions(session),
      );
    }
  }

  return stats;
}

async function executeContributionCleanup({
  userId,
  profileId,
  contributionId = null,
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

      const deleteAll = !contributionId;
      const contributionFilter = {
        userId: profile._id,
        ...(deleteAll ? {} : { _id: contributionId }),
      };

      const targetContributions = await withSession(
        ContributionModel.find(contributionFilter).lean(),
        session,
      );

      if (targetContributions.length === 0) {
        throw new Error(
          deleteAll
            ? "No contributions found for this user"
            : `Contribution ${contributionId} not found for this user`,
        );
      }

      const deletedContributionIds = targetContributions.map(
        (contribution) => contribution._id,
      );
      const deletedContributionIdStrings = toIdStrings(deletedContributionIds);
      const affectedGroupIds = toIdStrings(
        targetContributions.map((contribution) => contribution.groupId),
      );
      const paymentReferences = [...new Set(
        targetContributions
          .map((contribution) => String(contribution.paymentReference || "").trim())
          .filter(Boolean),
      )];
      const countedAmountRemoved = sumAmounts(
        targetContributions.filter((contribution) =>
          COUNTED_CONTRIBUTION_STATUSES.includes(contribution.status),
        ),
      );

      const contributionTransactions = await loadContributionTransactions({
        profileId: profile._id,
        contributionIds: deletedContributionIdStrings,
        paymentReferences,
        deleteAll,
        session,
      });

      const recurringPayments = deleteAll
        ? await withSession(
            RecurringPaymentModel.find({
              userId: profile._id,
              paymentType: "group_contribution",
            }).lean(),
            session,
          )
        : [];
      const contributionSettings = deleteAll
        ? await withSession(
            ContributionSettingModel.find({ userId: profile._id }).lean(),
            session,
          )
        : [];

      const transactionPlan = await reconcileContributionTransactions({
        profileId: profile._id,
        deletedContributionIds: deletedContributionIdStrings,
        deletedContributionObjectIds: deletedContributionIds,
        candidateTransactions: contributionTransactions,
        session,
        dryRun: true,
      });

      const summary = {
        user: {
          userId: String(user._id),
          profileId: String(profile._id),
          email: user.email ?? null,
          fullName: profile.fullName ?? null,
        },
        mode: deleteAll ? "all" : "single",
        targetContributionId: contributionId ? String(contributionId) : null,
        contributions: {
          matched: targetContributions.length,
          amountRemoved: sumAmounts(targetContributions),
          countedAmountRemoved,
        },
        transactions: transactionPlan,
        contributionSettings: {
          matched: contributionSettings.length,
          deleteAll,
        },
        recurringPayments: {
          matched: recurringPayments.length,
          deleteAll,
        },
        affectedGroups: affectedGroupIds,
        dryRun,
      };

      if (dryRun) {
        return summary;
      }

      const transactionResult = deleteAll
        ? {
            matched: contributionTransactions.length,
            deleted: contributionTransactions.length,
            updated: 0,
          }
        : await reconcileContributionTransactions({
            profileId: profile._id,
            deletedContributionIds: deletedContributionIdStrings,
            deletedContributionObjectIds: deletedContributionIds,
            candidateTransactions: contributionTransactions,
            session,
            dryRun: false,
          });

      if (deleteAll && contributionTransactions.length > 0) {
        await TransactionModel.deleteMany(
          {
            _id: { $in: contributionTransactions.map((transaction) => transaction._id) },
          },
          mongoOptions(session),
        );
      }

      const contributionDeleteResult = await ContributionModel.deleteMany(
        { _id: { $in: deletedContributionIds } },
        mongoOptions(session),
      );

      let settingsResult = { deleted: 0, updated: 0 };
      if (deleteAll) {
        const deleteResult = await ContributionSettingModel.deleteMany(
          { userId: profile._id },
          mongoOptions(session),
        );
        settingsResult = {
          deleted: Number(deleteResult?.deletedCount ?? 0),
          updated: 0,
        };
      } else {
        settingsResult = await reconcileContributionSettings({
          profileId: profile._id,
          groupIds: affectedGroupIds,
          session,
        });
      }

      let recurringResult = { deleted: 0 };
      if (deleteAll) {
        const deleteResult = await RecurringPaymentModel.deleteMany(
          {
            userId: profile._id,
            paymentType: "group_contribution",
          },
          mongoOptions(session),
        );
        recurringResult.deleted = Number(deleteResult?.deletedCount ?? 0);

        await ProfileModel.updateOne(
          { _id: profile._id },
          { $set: { contributionSettings: null } },
          mongoOptions(session),
        );
      }

      const groupSnapshots = [];
      const membershipSnapshots = [];
      for (const groupId of affectedGroupIds) {
        membershipSnapshots.push(
          await recomputeMembershipContributionTotal(profile._id, groupId, session),
        );
        groupSnapshots.push(await recomputeGroupSnapshot(groupId, session));
      }

      return {
        ...summary,
        transactions: transactionResult,
        contributions: {
          ...summary.contributions,
          deleted: Number(contributionDeleteResult?.deletedCount ?? 0),
        },
        contributionSettings: {
          ...summary.contributionSettings,
          ...settingsResult,
        },
        recurringPayments: {
          ...summary.recurringPayments,
          ...recurringResult,
          profileContributionSettingsCleared: deleteAll,
        },
        groups: groupSnapshots,
        memberships: membershipSnapshots,
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
        "  node src/scripts/deleteUserContributions.js --userId <userId> --profileId <profileId> [--contributionId <contributionId>] [--dry-run] [--no-transaction]",
        "",
        "Notes:",
        "  Without --contributionId, the script deletes the user's entire contribution domain data.",
        "  Full delete also removes group-contribution transactions, contribution settings, recurring contribution payments, and clears Profile.contributionSettings.",
        "  Single-record delete removes the specified contribution, reconciles shared transactions when needed, and recomputes affected totals.",
      ].join("\n"),
    );
    process.exit(0);
  }

  const userId = asObjectId(args.userId, "userId");
  const profileId = asObjectId(args.profileId, "profileId");
  const contributionId = args.contributionId
    ? asObjectId(args.contributionId, "contributionId")
    : null;
  const dryRun = Boolean(args["dry-run"]);
  const useTransaction = !Boolean(args["no-transaction"]);

  const result = await executeContributionCleanup({
    userId,
    profileId,
    contributionId,
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
