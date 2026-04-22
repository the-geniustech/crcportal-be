import dotenv from "dotenv";

dotenv.config();

import mongoose from "mongoose";
import { connectMongo } from "../db.js";
import { UserModel } from "../models/User.js";
import { ProfileModel } from "../models/Profile.js";
import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { ContributionModel } from "../models/Contribution.js";
import { TransactionModel } from "../models/Transaction.js";
import { ContributionSettingModel } from "../models/ContributionSetting.js";
import { WithdrawalRequestModel } from "../models/WithdrawalRequest.js";
import { RecurringPaymentModel } from "../models/RecurringPayment.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import {
  BLOCKING_MEMBERSHIP_STATUSES,
} from "../utils/groupMembershipPolicy.js";
import { assignGroupMemberSerial } from "../utils/groupMemberSerial.js";
import {
  normalizeContributionType,
  resolveExpectedContributionAmount,
} from "../utils/contributionPolicy.js";
import { normalizeUserRoles, pickPrimaryRole } from "../utils/roles.js";

const COUNTED_CONTRIBUTION_STATUSES = ["completed", "verified"];
const DEFAULT_SOURCE_STATUSES = ["active", "pending", "suspended"];
const TARGET_MEMBERSHIP_REVIEW_NOTE = "Transferred by migration script";
const SOURCE_MEMBERSHIP_REVIEW_NOTE = "Transferred out by migration script";

const parseArgs = (args) => {
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
};

const asObjectId = (value, fieldName) => {
  if (!value || !mongoose.Types.ObjectId.isValid(value)) {
    throw new Error(`Invalid ${fieldName}`);
  }
  return new mongoose.Types.ObjectId(String(value));
};

const normalizeNumber = (value) => {
  const num =
    typeof value === "number"
      ? value
      : typeof value === "string"
        ? Number(value)
        : NaN;
  return Number.isFinite(num) ? Number(num) : null;
};

const normalizePositiveNumber = (value) => {
  const num = normalizeNumber(value);
  return num !== null && num > 0 ? num : null;
};

const normalizeContributionKey = (year, type) => {
  const safeYear = Number(year ?? 0);
  const safeType = normalizeContributionType(type) || "revolving";
  return `${safeYear}:${safeType}`;
};

const buildUnitsPayload = (type, units) => {
  const value = normalizePositiveNumber(units);
  return {
    revolving: type === "revolving" ? value : null,
    special: type === "special" ? value : null,
    endwell: type === "endwell" ? value : null,
    festive: type === "festive" ? value : null,
  };
};

const appendReviewNote = (existing, addition) => {
  const base = String(existing || "").trim();
  const next = String(addition || "").trim();
  if (!base) return next || null;
  if (!next) return base;
  if (base.includes(next)) return base;
  return `${base} | ${next}`;
};

const withSession = (queryOrAggregate, session) =>
  session ? queryOrAggregate.session(session) : queryOrAggregate;

async function syncUserCoordinatorRole(profileId, session) {
  if (!profileId) return;

  const [user, hasCoordinatorMembership] = await Promise.all([
    withSession(
      UserModel.findOne({ profileId }).select("roles role").lean(),
      session,
    ),
    withSession(
      GroupMembershipModel.exists({
        userId: profileId,
        role: "coordinator",
        status: "active",
      }),
      session,
    ),
  ]);

  if (!user) return;

  const currentRoles = normalizeUserRoles(user);
  const nextRoles = new Set(currentRoles);

  if (hasCoordinatorMembership) {
    nextRoles.add("groupCoordinator");
    nextRoles.add("member");
  } else {
    nextRoles.delete("groupCoordinator");
    if (!nextRoles.has("member")) {
      nextRoles.add("member");
    }
  }

  const resolvedRoles = Array.from(nextRoles);
  await UserModel.updateOne(
    { profileId },
    {
      $set: {
        roles: resolvedRoles,
        role: pickPrimaryRole(resolvedRoles),
      },
    },
    session ? { session } : {},
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
    session ? { session } : {},
  );

  return { memberCount, totalSavings };
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
          total: { $sum: "$amount" },
        },
      },
    ]),
    session,
  );

  return Number(agg?.[0]?.total ?? 0);
}

async function syncContributionSettingsForProfileGroup({
  profileId,
  sourceGroupId,
  targetGroup,
  sourceSettings,
  targetSettings,
  session,
}) {
  const actualsAgg = await withSession(
    ContributionModel.aggregate([
      {
        $match: {
          userId: profileId,
          groupId: targetGroup._id,
          status: { $in: COUNTED_CONTRIBUTION_STATUSES },
        },
      },
      {
        $group: {
          _id: {
            year: "$year",
            contributionType: "$contributionType",
          },
          totalActual: { $sum: "$amount" },
        },
      },
    ]),
    session,
  );

  const blueprintByKey = new Map();
  const registerBlueprint = (setting) => {
    if (!setting) return;
    const year = normalizeNumber(setting.year);
    if (!year) return;
    const contributionType =
      normalizeContributionType(setting.contributionType) || "revolving";
    const key = normalizeContributionKey(year, contributionType);
    const existing = blueprintByKey.get(key) || {
      year,
      contributionType,
      units: null,
      expectedMonthlyAmount: null,
      totalExpected: null,
    };

    if (existing.units === null) {
      existing.units = normalizePositiveNumber(setting.units);
    }
    if (existing.expectedMonthlyAmount === null) {
      existing.expectedMonthlyAmount = normalizePositiveNumber(
        setting.expectedMonthlyAmount,
      );
    }
    if (existing.totalExpected === null) {
      existing.totalExpected = normalizePositiveNumber(setting.totalExpected);
    }

    blueprintByKey.set(key, existing);
  };

  targetSettings.forEach(registerBlueprint);
  sourceSettings.forEach(registerBlueprint);

  const actualsByKey = new Map();
  actualsAgg.forEach((entry) => {
    const year = normalizeNumber(entry?._id?.year);
    if (!year) return;
    const contributionType =
      normalizeContributionType(entry?._id?.contributionType) || "revolving";
    const key = normalizeContributionKey(year, contributionType);
    actualsByKey.set(key, Number(entry?.totalActual ?? 0));

    if (!blueprintByKey.has(key)) {
      blueprintByKey.set(key, {
        year,
        contributionType,
        units: null,
        expectedMonthlyAmount: null,
        totalExpected: null,
      });
    }
  });

  const ops = [];
  for (const blueprint of blueprintByKey.values()) {
    const settingsKey = normalizeContributionKey(
      blueprint.year,
      blueprint.contributionType,
    );
    const settingsStub = {
      year: blueprint.year,
      units: buildUnitsPayload(blueprint.contributionType, blueprint.units),
    };
    const derivedExpected = resolveExpectedContributionAmount({
      settings: settingsStub,
      year: blueprint.year,
      groupMonthlyContribution: targetGroup.monthlyContribution,
      type: blueprint.contributionType,
    });
    const expectedMonthlyAmount =
      normalizePositiveNumber(derivedExpected) ||
      normalizePositiveNumber(blueprint.expectedMonthlyAmount) ||
      normalizePositiveNumber(
        normalizeNumber(blueprint.totalExpected) !== null
          ? Number(blueprint.totalExpected) / 12
          : null,
      ) ||
      0;
    const totalExpected = Number(expectedMonthlyAmount) * 12;
    const totalActual = Number(actualsByKey.get(settingsKey) ?? 0);
    const outstandingBalance = Math.max(totalExpected - totalActual, 0);

    ops.push({
      updateOne: {
        filter: {
          userId: profileId,
          groupId: targetGroup._id,
          year: blueprint.year,
          contributionType: blueprint.contributionType,
        },
        update: {
          $set: {
            userId: profileId,
            groupId: targetGroup._id,
            year: blueprint.year,
            contributionType: blueprint.contributionType,
            expectedMonthlyAmount,
            totalExpected,
            totalActual,
            outstandingBalance,
            units: blueprint.units,
          },
        },
        upsert: true,
      },
    });
  }

  if (ops.length > 0) {
    await ContributionSettingModel.bulkWrite(ops, {
      ordered: false,
      ...(session ? { session } : {}),
    });
  }

  const deleteResult = await ContributionSettingModel.deleteMany(
    { userId: profileId, groupId: sourceGroupId },
    session ? { session } : {},
  );

  return {
    upsertedKeys: ops.length,
    deletedSourceSettings: Number(deleteResult?.deletedCount ?? 0),
  };
}

async function resolveSourceMembership({
  profileId,
  targetGroupId,
  fromGroupNumber = null,
  session = null,
}) {
  const statusFilter = fromGroupNumber !== null
    ? { status: { $in: [...new Set([...DEFAULT_SOURCE_STATUSES, "inactive"])] } }
    : { status: { $in: DEFAULT_SOURCE_STATUSES } };

  const memberships = await withSession(
    GroupMembershipModel.find(
      {
        userId: profileId,
        groupId: { $ne: targetGroupId },
        ...statusFilter,
      },
      {
        userId: 1,
        groupId: 1,
        role: 1,
        status: 1,
        joinedAt: 1,
        requestedAt: 1,
        reviewedAt: 1,
        reviewedBy: 1,
        reviewNotes: 1,
        memberNumber: 1,
        memberSerial: 1,
        totalContributed: 1,
      },
    ).lean(),
    session,
  );

  if (memberships.length === 0) {
    return { sourceMembership: null, candidates: [] };
  }

  const groupIds = [...new Set(memberships.map((membership) => String(membership.groupId)))];
  const groups = await withSession(
    GroupModel.find(
      { _id: { $in: groupIds } },
      { groupNumber: 1, groupName: 1, coordinatorId: 1 },
    ).lean(),
    session,
  );
  const groupById = new Map(groups.map((group) => [String(group._id), group]));

  const candidates = memberships
    .map((membership) => ({
      ...membership,
      group: groupById.get(String(membership.groupId)) || null,
    }))
    .filter((candidate) => candidate.group);

  if (fromGroupNumber !== null) {
    const matched = candidates.filter(
      (candidate) => Number(candidate.group?.groupNumber) === Number(fromGroupNumber),
    );
    if (matched.length === 1) {
      return { sourceMembership: matched[0], candidates };
    }
    if (matched.length === 0) {
      throw new Error(`No source membership found for group ${fromGroupNumber}`);
    }
    throw new Error(`Multiple source memberships found for group ${fromGroupNumber}`);
  }

  const blocking = candidates.filter((candidate) =>
    BLOCKING_MEMBERSHIP_STATUSES.includes(candidate.status),
  );
  const nonZeroBlocking = blocking.filter(
    (candidate) => Number(candidate.group?.groupNumber ?? 0) !== 0,
  );
  const activeBlocking = blocking.filter((candidate) => candidate.status === "active");

  if (nonZeroBlocking.length === 1) {
    return { sourceMembership: nonZeroBlocking[0], candidates };
  }
  if (blocking.length === 1) {
    return { sourceMembership: blocking[0], candidates };
  }
  if (activeBlocking.length === 1) {
    return { sourceMembership: activeBlocking[0], candidates };
  }

  return { sourceMembership: null, candidates };
}

async function gatherTransferScope({
  profileId,
  sourceGroupId,
  session,
}) {
  const [
    contributions,
    contributionSettings,
    withdrawals,
    recurringPayments,
    transactions,
    loanApplications,
    loanTransactions,
    recurringLoanPayments,
  ] = await Promise.all([
    withSession(
      ContributionModel.countDocuments({ userId: profileId, groupId: sourceGroupId }),
      session,
    ),
    withSession(
      ContributionSettingModel.countDocuments({
        userId: profileId,
        groupId: sourceGroupId,
      }),
      session,
    ),
    withSession(
      WithdrawalRequestModel.countDocuments({ userId: profileId, groupId: sourceGroupId }),
      session,
    ),
    withSession(
      RecurringPaymentModel.countDocuments({ userId: profileId, groupId: sourceGroupId }),
      session,
    ),
    withSession(
      TransactionModel.countDocuments({
        userId: profileId,
        groupId: sourceGroupId,
        $or: [{ loanId: null }, { loanId: { $exists: false } }],
      }),
      session,
    ),
    withSession(
      LoanApplicationModel.countDocuments({ userId: profileId, groupId: sourceGroupId }),
      session,
    ),
    withSession(
      TransactionModel.countDocuments({
        userId: profileId,
        groupId: sourceGroupId,
        loanId: { $type: "objectId" },
      }),
      session,
    ),
    withSession(
      RecurringPaymentModel.countDocuments({
        userId: profileId,
        groupId: sourceGroupId,
        loanId: { $type: "objectId" },
      }),
      session,
    ),
  ]);

  return {
    contributions,
    transactions,
    contributionSettings,
    withdrawals,
    recurringPayments,
    loanApplications,
    loanTransactions,
    recurringLoanPayments,
  };
}

async function executeTransfer({
  userId,
  profileId,
  targetGroupNumber,
  fromGroupNumber = null,
  dryRun = false,
  moveLoans = false,
  useTransaction = true,
}) {
  const mongoUri = process.env.MONGO_URI;
  if (!mongoUri) {
    throw new Error("Missing MONGO_URI");
  }

  await connectMongo({ mongoUri });

  const session = useTransaction ? await mongoose.startSession() : null;
  let transferResult = null;

  const run = async (activeSession) => {
    const [user, profile, targetGroup] = await Promise.all([
      withSession(
        UserModel.findById(userId).select("+active profileId roles role").lean(),
        activeSession,
      ),
      withSession(ProfileModel.findById(profileId).lean(), activeSession),
      withSession(
        GroupModel.findOne(
          { groupNumber: Number(targetGroupNumber) },
          {
            groupNumber: 1,
            groupName: 1,
            monthlyContribution: 1,
            memberCount: 1,
            maxMembers: 1,
            coordinatorId: 1,
          },
        ).lean(),
        activeSession,
      ),
    ]);

    if (!user) {
      throw new Error(`User ${userId} not found`);
    }
    if (!profile) {
      throw new Error(`Profile ${profileId} not found`);
    }
    if (!user.profileId || String(user.profileId) !== String(profile._id)) {
      throw new Error("User profileId does not match the provided profileId");
    }
    if (!targetGroup) {
      throw new Error(`Target group ${targetGroupNumber} not found`);
    }

    const { sourceMembership, candidates } = await resolveSourceMembership({
      profileId: profile._id,
      targetGroupId: targetGroup._id,
      fromGroupNumber,
      session: activeSession,
    });

    if (!sourceMembership) {
      throw new Error(
        candidates.length > 1
          ? `Source group is ambiguous. Candidates: ${candidates
              .map(
                (candidate) =>
                  `${candidate.group.groupNumber} (${candidate.group.groupName}) [${candidate.status}]`,
              )
              .join(", ")}. Re-run with --fromGroupNumber.`
          : "No source membership found to transfer",
      );
    }

    const sourceGroup = sourceMembership.group;
    if (String(sourceGroup._id) === String(targetGroup._id)) {
      throw new Error("Source group and target group cannot be the same");
    }

    if (
      String(sourceGroup.coordinatorId || "") === String(profile._id) ||
      sourceMembership.role === "coordinator"
    ) {
      throw new Error(
        `Cannot transfer the current coordinator of group ${sourceGroup.groupNumber}. Reassign the coordinator first.`,
      );
    }

    const [targetMembership, conflictingMemberships, transferScope] = await Promise.all([
      withSession(
        GroupMembershipModel.findOne({
          userId: profile._id,
          groupId: targetGroup._id,
        }),
        activeSession,
      ),
      withSession(
        GroupMembershipModel.find({
          userId: profile._id,
          groupId: { $nin: [sourceGroup._id, targetGroup._id] },
          status: { $in: BLOCKING_MEMBERSHIP_STATUSES },
        }).lean(),
        activeSession,
      ),
      gatherTransferScope({
        profileId: profile._id,
        sourceGroupId: sourceGroup._id,
        session: activeSession,
      }),
    ]);

    if (!moveLoans) {
      const blockedLoanArtifacts =
        Number(transferScope.loanApplications ?? 0) +
        Number(transferScope.loanTransactions ?? 0) +
        Number(transferScope.recurringLoanPayments ?? 0);
      if (blockedLoanArtifacts > 0) {
        throw new Error(
          "Loan-linked records exist for the source group. Re-run with --move-loans if you intentionally want those moved too.",
        );
      }
    }

    if (conflictingMemberships.length > 0) {
      const conflictGroupIds = [
        ...new Set(conflictingMemberships.map((membership) => String(membership.groupId))),
      ];
      const conflictGroups = await withSession(
        GroupModel.find(
          { _id: { $in: conflictGroupIds } },
          { groupNumber: 1, groupName: 1 },
        ).lean(),
        activeSession,
      );
      const nonZeroConflicts = conflictGroups.filter(
        (group) => Number(group.groupNumber ?? 0) !== 0,
      );
      if (nonZeroConflicts.length > 0) {
        throw new Error(
          `User has other blocking memberships outside the move scope: ${nonZeroConflicts
            .map((group) => `${group.groupNumber} (${group.groupName})`)
            .join(", ")}`,
        );
      }
    }

    const targetActiveCount = await withSession(
      GroupMembershipModel.countDocuments({
        groupId: targetGroup._id,
        status: "active",
      }),
      activeSession,
    );
    const targetWouldAddMember = !targetMembership || targetMembership.status !== "active";
    if (
      targetWouldAddMember &&
      Number(targetGroup.maxMembers ?? 0) > 0 &&
      targetActiveCount >= Number(targetGroup.maxMembers)
    ) {
      throw new Error(
        `Target group ${targetGroup.groupNumber} is full (${targetActiveCount}/${targetGroup.maxMembers})`,
      );
    }

    const sourceSettings = await withSession(
      ContributionSettingModel.find({
        userId: profile._id,
        groupId: sourceGroup._id,
      }).lean(),
      activeSession,
    );
    const targetSettings = await withSession(
      ContributionSettingModel.find({
        userId: profile._id,
        groupId: targetGroup._id,
      }).lean(),
      activeSession,
    );

    const beforeSnapshots = {
      sourceGroup: {
        groupId: String(sourceGroup._id),
        groupNumber: Number(sourceGroup.groupNumber),
        groupName: sourceGroup.groupName,
        memberCount: Number(sourceGroup.memberCount ?? 0),
      },
      targetGroup: {
        groupId: String(targetGroup._id),
        groupNumber: Number(targetGroup.groupNumber),
        groupName: targetGroup.groupName,
        memberCount: Number(targetGroup.memberCount ?? 0),
      },
      sourceMembership: {
        groupId: String(sourceGroup._id),
        status: sourceMembership.status,
        role: sourceMembership.role,
        totalContributed: Number(sourceMembership.totalContributed ?? 0),
      },
      targetMembership: targetMembership
        ? {
            id: String(targetMembership._id),
            status: targetMembership.status,
            role: targetMembership.role,
            totalContributed: Number(targetMembership.totalContributed ?? 0),
          }
        : null,
    };

    const summary = {
      user: {
        userId: String(user._id),
        profileId: String(profile._id),
        email: user.email ?? null,
        fullName: profile.fullName ?? null,
      },
      sourceGroup: {
        groupId: String(sourceGroup._id),
        groupNumber: Number(sourceGroup.groupNumber),
        groupName: sourceGroup.groupName,
      },
      targetGroup: {
        groupId: String(targetGroup._id),
        groupNumber: Number(targetGroup.groupNumber),
        groupName: targetGroup.groupName,
      },
      counts: transferScope,
      before: beforeSnapshots,
      dryRun,
      moveLoans,
      usedTransaction: Boolean(activeSession),
    };

    if (dryRun) {
      return summary;
    }

    const now = new Date();
    const targetRole = (() => {
      if (String(targetGroup.coordinatorId || "") === String(profile._id)) {
        return "coordinator";
      }
      if (targetMembership?.role) {
        return targetMembership.role;
      }
      return "member";
    })();

    const targetMembershipDoc =
      targetMembership ||
      new GroupMembershipModel({
        userId: profile._id,
        groupId: targetGroup._id,
      });

    targetMembershipDoc.groupId = targetGroup._id;
    targetMembershipDoc.userId = profile._id;
    targetMembershipDoc.status = "active";
    targetMembershipDoc.role = targetRole;
    targetMembershipDoc.joinedAt =
      targetMembershipDoc.joinedAt ||
      sourceMembership.joinedAt ||
      sourceMembership.requestedAt ||
      now;
    targetMembershipDoc.requestedAt =
      targetMembershipDoc.requestedAt ||
      sourceMembership.requestedAt ||
      now;
    targetMembershipDoc.reviewedAt = now;
    targetMembershipDoc.reviewedBy = null;
    targetMembershipDoc.reviewNotes = appendReviewNote(
      targetMembershipDoc.reviewNotes,
      `${TARGET_MEMBERSHIP_REVIEW_NOTE}: from group ${sourceGroup.groupNumber} on ${now.toISOString()}`,
    );
    await targetMembershipDoc.save({
      validateBeforeSave: true,
      ...(activeSession ? { session: activeSession } : {}),
    });

    sourceMembership.reviewedAt = now;
    sourceMembership.reviewedBy = null;
    sourceMembership.reviewNotes = appendReviewNote(
      sourceMembership.reviewNotes,
      `${SOURCE_MEMBERSHIP_REVIEW_NOTE}: to group ${targetGroup.groupNumber} on ${now.toISOString()}`,
    );

    await GroupMembershipModel.updateOne(
      { _id: sourceMembership._id },
      {
        $set: {
          status: "inactive",
          reviewedAt: sourceMembership.reviewedAt,
          reviewedBy: sourceMembership.reviewedBy,
          reviewNotes: sourceMembership.reviewNotes,
        },
      },
      activeSession ? { session: activeSession } : {},
    );

    const contributionResult = await ContributionModel.updateMany(
      { userId: profile._id, groupId: sourceGroup._id },
      { $set: { groupId: targetGroup._id } },
      activeSession ? { session: activeSession } : {},
    );

    const transactionResult = await TransactionModel.updateMany(
      {
        userId: profile._id,
        groupId: sourceGroup._id,
        $or: [{ loanId: null }, { loanId: { $exists: false } }],
      },
      {
        $set: {
          groupId: targetGroup._id,
          groupName: targetGroup.groupName,
        },
      },
      activeSession ? { session: activeSession } : {},
    );

    await TransactionModel.updateMany(
      {
        userId: profile._id,
        groupId: targetGroup._id,
        type: "group_contribution",
        $or: [{ loanId: null }, { loanId: { $exists: false } }],
      },
      {
        $set: {
          description: `Group contribution - ${targetGroup.groupName}`,
        },
      },
      activeSession ? { session: activeSession } : {},
    );

    const withdrawalResult = await WithdrawalRequestModel.updateMany(
      { userId: profile._id, groupId: sourceGroup._id },
      {
        $set: {
          groupId: targetGroup._id,
          groupName: targetGroup.groupName,
        },
      },
      activeSession ? { session: activeSession } : {},
    );

    const recurringContributionResult = await RecurringPaymentModel.updateMany(
      {
        userId: profile._id,
        groupId: sourceGroup._id,
        $or: [{ loanId: null }, { loanId: { $exists: false } }],
      },
      {
        $set: {
          groupId: targetGroup._id,
          groupName: targetGroup.groupName,
        },
      },
      activeSession ? { session: activeSession } : {},
    );

    await RecurringPaymentModel.updateMany(
      {
        userId: profile._id,
        groupId: targetGroup._id,
        paymentType: "group_contribution",
        $or: [{ loanId: null }, { loanId: { $exists: false } }],
      },
      {
        $set: {
          description: `Group contribution - ${targetGroup.groupName}`,
        },
      },
      activeSession ? { session: activeSession } : {},
    );

    let loanApplicationResult = { modifiedCount: 0 };
    let loanTransactionResult = { modifiedCount: 0 };
    let recurringLoanResult = { modifiedCount: 0 };
    if (moveLoans) {
      loanApplicationResult = await LoanApplicationModel.updateMany(
        { userId: profile._id, groupId: sourceGroup._id },
        {
          $set: {
            groupId: targetGroup._id,
            groupName: targetGroup.groupName,
          },
        },
        activeSession ? { session: activeSession } : {},
      );

      loanTransactionResult = await TransactionModel.updateMany(
        {
          userId: profile._id,
          groupId: sourceGroup._id,
          loanId: { $type: "objectId" },
        },
        {
          $set: {
            groupId: targetGroup._id,
            groupName: targetGroup.groupName,
          },
        },
        activeSession ? { session: activeSession } : {},
      );

      recurringLoanResult = await RecurringPaymentModel.updateMany(
        {
          userId: profile._id,
          groupId: sourceGroup._id,
          loanId: { $type: "objectId" },
        },
        {
          $set: {
            groupId: targetGroup._id,
            groupName: targetGroup.groupName,
          },
        },
        activeSession ? { session: activeSession } : {},
      );
    }

    const contributionSettingsResult = await syncContributionSettingsForProfileGroup({
      profileId: profile._id,
      sourceGroupId: sourceGroup._id,
      targetGroup,
      sourceSettings,
      targetSettings,
      session: activeSession,
    });

    const targetTotalContributed = await recomputeMembershipContributionTotal(
      profile._id,
      targetGroup._id,
      activeSession,
    );
    const sourceTotalContributed = await recomputeMembershipContributionTotal(
      profile._id,
      sourceGroup._id,
      activeSession,
    );

    targetMembershipDoc.totalContributed = targetTotalContributed;
    await assignGroupMemberSerial({
      membership: targetMembershipDoc,
      group: targetGroup,
      session: activeSession,
    });

    await GroupMembershipModel.updateOne(
      { _id: sourceMembership._id },
      {
        $set: {
          totalContributed: sourceTotalContributed,
        },
      },
      activeSession ? { session: activeSession } : {},
    );

    const [sourceGroupSnapshot, targetGroupSnapshot] = await Promise.all([
      recomputeGroupSnapshot(sourceGroup._id, activeSession),
      recomputeGroupSnapshot(targetGroup._id, activeSession),
    ]);

    await syncUserCoordinatorRole(profile._id, activeSession);

    const refreshedTargetMembership = await withSession(
      GroupMembershipModel.findById(targetMembershipDoc._id).lean(),
      activeSession,
    );
    const refreshedSourceMembership = await withSession(
      GroupMembershipModel.findById(sourceMembership._id).lean(),
      activeSession,
    );

    return {
      ...summary,
      updates: {
        contributionsMoved: Number(contributionResult?.modifiedCount ?? 0),
        transactionsMoved: Number(transactionResult?.modifiedCount ?? 0),
        withdrawalsMoved: Number(withdrawalResult?.modifiedCount ?? 0),
        recurringPaymentsMoved: Number(
          recurringContributionResult?.modifiedCount ?? 0,
        ),
        contributionSettingsUpserted: Number(
          contributionSettingsResult?.upsertedKeys ?? 0,
        ),
        sourceContributionSettingsDeleted: Number(
          contributionSettingsResult?.deletedSourceSettings ?? 0,
        ),
        loanApplicationsMoved: Number(
          loanApplicationResult?.modifiedCount ?? 0,
        ),
        loanTransactionsMoved: Number(
          loanTransactionResult?.modifiedCount ?? 0,
        ),
        recurringLoanPaymentsMoved: Number(
          recurringLoanResult?.modifiedCount ?? 0,
        ),
      },
      after: {
        sourceGroup: sourceGroupSnapshot,
        targetGroup: targetGroupSnapshot,
        sourceMembership: refreshedSourceMembership
          ? {
              id: String(refreshedSourceMembership._id),
              status: refreshedSourceMembership.status,
              role: refreshedSourceMembership.role,
              totalContributed: Number(
                refreshedSourceMembership.totalContributed ?? 0,
              ),
            }
          : null,
        targetMembership: refreshedTargetMembership
          ? {
              id: String(refreshedTargetMembership._id),
              status: refreshedTargetMembership.status,
              role: refreshedTargetMembership.role,
              totalContributed: Number(
                refreshedTargetMembership.totalContributed ?? 0,
              ),
              memberNumber: refreshedTargetMembership.memberNumber ?? null,
              memberSerial: refreshedTargetMembership.memberSerial ?? null,
            }
          : null,
      },
    };
  };

  try {
    if (dryRun || !session) {
      transferResult = await run(null);
    } else {
      await session.withTransaction(async () => {
        transferResult = await run(session);
      });
    }
  } finally {
    if (session) {
      await session.endSession();
    }
    await mongoose.disconnect();
  }

  return transferResult;
}

const args = parseArgs(process.argv.slice(2));

const runCli = async () => {
  if (args.help || args.h) {
    // eslint-disable-next-line no-console
    console.log(
      [
        "Usage:",
        "  node src/scripts/moveUserBetweenGroups.js --userId <userId> --profileId <profileId> --toGroupNumber <groupNumber> [--fromGroupNumber <groupNumber>] [--dry-run] [--move-loans] [--no-transaction]",
        "",
        "Notes:",
        "  --toGroupNumber can also be passed as --groupNumber",
        "  --fromGroupNumber is optional and only needed when the source group is ambiguous",
        "  --dry-run validates and reports counts without changing data",
        "  --move-loans also migrates loan applications and loan-linked transactions/recurring payments",
        "  --no-transaction is for standalone MongoDB instances that do not support transactions",
      ].join("\n"),
    );
    return;
  }

  const userId = asObjectId(args.userId, "userId");
  const profileId = asObjectId(args.profileId, "profileId");
  const targetGroupNumber = normalizeNumber(
    args.toGroupNumber ?? args.groupNumber ?? args.toGroup ?? args.group,
  );
  const fromGroupNumber = normalizeNumber(args.fromGroupNumber ?? args.fromGroup);
  const dryRun = Boolean(args["dry-run"]);
  const moveLoans = Boolean(args["move-loans"]);
  const useTransaction = !Boolean(args["no-transaction"]);

  if (!targetGroupNumber || targetGroupNumber < 0) {
    throw new Error("A valid target group number is required");
  }

  const result = await executeTransfer({
    userId,
    profileId,
    targetGroupNumber,
    fromGroupNumber,
    dryRun,
    moveLoans,
    useTransaction,
  });

  // eslint-disable-next-line no-console
  console.log(JSON.stringify({ ok: 1, result }, null, 2));
};

runCli().catch((error) => {
  const message = error?.message ?? String(error);
  const looksLikeTransactionSupportError =
    /Transaction numbers are only allowed on a replica set member or mongos/i.test(
      message,
    ) ||
    /replica set/i.test(message);

  // eslint-disable-next-line no-console
  console.error(
    JSON.stringify(
      {
        ok: 0,
        error: looksLikeTransactionSupportError
          ? `${message}. Re-run with --no-transaction if you are intentionally using a standalone MongoDB instance.`
          : message,
      },
      null,
      2,
    ),
  );
  process.exit(1);
});
