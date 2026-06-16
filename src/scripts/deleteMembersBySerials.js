import dotenv from "dotenv";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";

const currentFilePath = fileURLToPath(import.meta.url);
const currentDir = path.dirname(currentFilePath);

dotenv.config({ path: path.resolve(currentDir, "../../.env") });

import { AuditLogModel } from "../models/AuditLog.js";
import { BankAccountModel } from "../models/BankAccount.js";
import { CommunicationLogModel } from "../models/CommunicationLog.js";
import { ContributionModel } from "../models/Contribution.js";
import { ContributionSettingModel } from "../models/ContributionSetting.js";
import { FormPaymentModel } from "../models/FormPayment.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { GroupModel } from "../models/Group.js";
import { GroupVoteModel } from "../models/GroupVote.js";
import { GroupVoteResponseModel } from "../models/GroupVoteResponse.js";
import { GuarantorNotificationModel } from "../models/GuarantorNotification.js";
import { LoanApplicationEditRequestModel } from "../models/LoanApplicationEditRequest.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import { LoanGuarantorModel } from "../models/LoanGuarantor.js";
import { LoanRepaymentScheduleItemModel } from "../models/LoanRepaymentScheduleItem.js";
import { LoginHistoryModel } from "../models/LoginHistory.js";
import { MeetingAttendanceModel } from "../models/MeetingAttendance.js";
import { MeetingRsvpModel } from "../models/MeetingRsvp.js";
import { NotificationModel } from "../models/Notification.js";
import { NotificationPreferenceModel } from "../models/NotificationPreference.js";
import { PhoneOtpSessionModel } from "../models/PhoneOtpSession.js";
import { ProfileModel } from "../models/Profile.js";
import { RecurringPaymentModel } from "../models/RecurringPayment.js";
import { RefreshTokenModel } from "../models/RefreshToken.js";
import { TransactionModel } from "../models/Transaction.js";
import { UserModel } from "../models/User.js";
import { WithdrawalRequestModel } from "../models/WithdrawalRequest.js";
import {
  buildMixedIdValues,
  formatScriptError,
  mongoOptions,
  runWithOptionalTransaction,
  toIdStrings,
  withSession,
} from "./utils/userDataCleanup.js";

const CONFIRMATION_TOKEN = "DELETE_MEMBERS_BY_SERIAL";
const COUNTED_CONTRIBUTION_STATUSES = ["completed", "verified"];
const DEFAULT_LOG_DIR = path.resolve(
  currentDir,
  "../../reports/member-serial-deletions",
);

function parseCliArgs(argv) {
  const args = {};
  const serials = [];

  for (let index = 0; index < argv.length; index += 1) {
    const current = argv[index];
    if (!current.startsWith("--")) {
      serials.push(current);
      continue;
    }

    const key = current.replace(/^--/, "");
    const next = argv[index + 1];
    if (!next || next.startsWith("--")) {
      args[key] = true;
      continue;
    }

    args[key] = next;
    index += 1;
  }

  const serialFlagValues = [
    args.serial,
    args.serials,
    args.memberSerial,
    args.memberSerials,
  ]
    .filter(Boolean)
    .flatMap((value) => String(value).split(","));

  return {
    args,
    serials: [...serials, ...serialFlagValues],
  };
}

function normalizeSerial(value) {
  const raw = String(value || "").trim().toUpperCase();
  if (!raw) return "";

  const cleaned = raw
    .replace(/[_\s-]+/g, "")
    .replace(/\\/g, "/")
    .replace(/\/+/g, "/");

  const match = cleaned.match(/^(?:CRC\/)?G?(\d{1,3})\/(\d{1,5})$/i);
  if (!match) return cleaned;

  const [, groupNumber, memberNumber] = match;
  return `CRC/G${Number(groupNumber)}/${String(memberNumber).padStart(4, "0")}`;
}

const unique = (values) =>
  Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => String(value || "").trim())
        .filter(Boolean),
    ),
  );

const toPlainId = (value) => String(value || "").trim();

const isNonEmpty = (value) =>
  value !== null && value !== undefined && String(value).trim() !== "";

const compactFilter = (base, conditions) => {
  const $or = conditions.filter(Boolean);
  if ($or.length === 0) return { ...base, _id: null };
  return { ...base, $or };
};

const deletionCount = (result) => Number(result?.deletedCount ?? 0);
const modifiedCount = (result) => Number(result?.modifiedCount ?? 0);

function dedupeDocs(docs) {
  const byId = new Map();
  (Array.isArray(docs) ? docs : []).forEach((doc) => {
    if (!doc?._id) return;
    byId.set(String(doc._id), doc);
  });
  return [...byId.values()];
}

function buildTimestamp(date = new Date()) {
  return date.toISOString().replace(/[:.]/g, "-");
}

async function writeReport({ outputDir, payload }) {
  await fs.mkdir(outputDir, { recursive: true });
  const mode = payload?.dryRun ? "dry-run" : "delete";
  const reportPath = path.join(
    outputDir,
    `delete-members-by-serial-${mode}-${buildTimestamp()}.json`,
  );
  await fs.writeFile(reportPath, `${JSON.stringify(payload, null, 2)}\n`);
  return reportPath;
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
    { $set: { memberCount, totalSavings } },
    mongoOptions(session),
  );

  return {
    groupId: String(groupId),
    memberCount,
    totalSavings,
  };
}

async function recomputeVoteSnapshot(voteId, session) {
  const counts = await withSession(
    GroupVoteResponseModel.aggregate([
      { $match: { voteId } },
      {
        $group: {
          _id: "$choice",
          count: { $sum: 1 },
        },
      },
    ]),
    session,
  );

  const yesVotes = Number(
    counts.find((item) => item._id === "yes")?.count ?? 0,
  );
  const noVotes = Number(
    counts.find((item) => item._id === "no")?.count ?? 0,
  );

  await GroupVoteModel.updateOne(
    { _id: voteId },
    {
      $set: {
        yesVotes,
        noVotes,
        totalVoters: yesVotes + noVotes,
      },
    },
    mongoOptions(session),
  );

  return {
    voteId: String(voteId),
    yesVotes,
    noVotes,
    totalVoters: yesVotes + noVotes,
  };
}

async function resolveTargets({ serials, session }) {
  const requestedSerials = serials.map((value) => ({
    input: String(value || "").trim(),
    normalized: normalizeSerial(value),
  }));
  const normalizedSerials = unique(
    requestedSerials.map((entry) => entry.normalized),
  );

  if (normalizedSerials.length === 0) {
    throw new Error("At least one member serial number is required.");
  }

  const seedMemberships = await withSession(
    GroupMembershipModel.find({
      memberSerial: { $in: normalizedSerials },
    }).lean(),
    session,
  );

  const membershipBySerial = new Map(
    seedMemberships.map((membership) => [
      String(membership.memberSerial || "").toUpperCase(),
      membership,
    ]),
  );

  const serialResults = requestedSerials.map((entry) => {
    const membership = membershipBySerial.get(entry.normalized);
    if (!membership) {
      return {
        input: entry.input,
        normalized: entry.normalized,
        status: "not_found",
        message: "No GroupMembership matched this memberSerial.",
      };
    }

    return {
      input: entry.input,
      normalized: entry.normalized,
      status: "matched",
      membershipId: String(membership._id),
      profileId: String(membership.userId),
      groupId: String(membership.groupId),
    };
  });

  const profileIds = unique(
    serialResults
      .filter((entry) => entry.status === "matched")
      .map((entry) => entry.profileId),
  );

  const [profiles, users] = await Promise.all([
    profileIds.length
      ? withSession(ProfileModel.find({ _id: { $in: profileIds } }).lean(), session)
      : [],
    profileIds.length
      ? withSession(
          UserModel.find({ profileId: { $in: profileIds } })
            .select("+active profileId email phone roles")
            .lean(),
          session,
        )
      : [],
  ]);

  const profileById = new Map(
    profiles.map((profile) => [String(profile._id), profile]),
  );
  const userByProfileId = new Map(
    users.map((user) => [String(user.profileId), user]),
  );

  return {
    requestedSerials,
    normalizedSerials,
    serialResults: serialResults.map((entry) => {
      const profile = profileById.get(entry.profileId);
      const user = userByProfileId.get(entry.profileId);
      return entry.status === "matched"
        ? {
            ...entry,
            status: profile ? "matched" : "profile_missing",
            userId: user?._id ? String(user._id) : null,
            fullName: profile?.fullName ?? null,
            email: profile?.email ?? user?.email ?? null,
            phone: profile?.phone ?? user?.phone ?? null,
          }
        : entry;
    }),
    profileIds,
    profiles,
    users,
  };
}

async function collectDeletionScope({ profileIds, userIds, phones, session }) {
  const profileIdValues = buildMixedIdValues(profileIds);
  const userIdValues = buildMixedIdValues(userIds);

  const [
    memberships,
    contributions,
    contributionSettings,
    withdrawals,
    bankAccounts,
    recurringPayments,
    notifications,
    notificationPreferences,
    meetingAttendances,
    meetingRsvps,
    voteResponses,
    loanApplications,
    loanEditRequestsByUser,
    loanGuarantorsAsGuarantor,
    formPaymentsByUser,
    communicationLogs,
    refreshTokens,
    loginHistory,
  ] = await Promise.all([
    withSession(
      GroupMembershipModel.find({ userId: { $in: profileIdValues } }).lean(),
      session,
    ),
    withSession(
      ContributionModel.find({ userId: { $in: profileIdValues } }).lean(),
      session,
    ),
    withSession(
      ContributionSettingModel.find({ userId: { $in: profileIdValues } }).lean(),
      session,
    ),
    withSession(
      WithdrawalRequestModel.find({ userId: { $in: profileIdValues } }).lean(),
      session,
    ),
    withSession(
      BankAccountModel.find({ userId: { $in: profileIdValues } }).lean(),
      session,
    ),
    withSession(
      RecurringPaymentModel.find({ userId: { $in: profileIdValues } }).lean(),
      session,
    ),
    withSession(
      NotificationModel.find({ userId: { $in: profileIdValues } }).lean(),
      session,
    ),
    withSession(
      NotificationPreferenceModel.find({
        userId: { $in: profileIdValues },
      }).lean(),
      session,
    ),
    withSession(
      MeetingAttendanceModel.find({ userId: { $in: profileIdValues } }).lean(),
      session,
    ),
    withSession(
      MeetingRsvpModel.find({ userId: { $in: profileIdValues } }).lean(),
      session,
    ),
    withSession(
      GroupVoteResponseModel.find({ userId: { $in: profileIdValues } }).lean(),
      session,
    ),
    withSession(
      LoanApplicationModel.find({
        $or: [
          { userId: { $in: profileIdValues } },
          { profileId: { $in: profileIdValues } },
        ],
      }).lean(),
      session,
    ),
    withSession(
      LoanApplicationEditRequestModel.find({
        userId: { $in: profileIdValues },
      }).lean(),
      session,
    ),
    withSession(
      LoanGuarantorModel.find({
        guarantorUserId: { $in: profileIdValues },
      }).lean(),
      session,
    ),
    withSession(
      FormPaymentModel.find({ userId: { $in: profileIdValues } }).lean(),
      session,
    ),
    withSession(
      CommunicationLogModel.find({ createdBy: { $in: profileIdValues } }).lean(),
      session,
    ),
    userIdValues.length
      ? withSession(
          RefreshTokenModel.find({ userId: { $in: userIdValues } }).lean(),
          session,
        )
      : Promise.resolve([]),
    userIdValues.length
      ? withSession(
          LoginHistoryModel.find({ userId: { $in: userIdValues } }).lean(),
          session,
        )
      : Promise.resolve([]),
  ]);

  const membershipIds = unique(memberships.map((item) => item._id));
  const loanIds = unique([
    ...loanApplications.map((item) => item._id),
    ...loanEditRequestsByUser.map((item) => item.loanApplicationId),
    ...loanGuarantorsAsGuarantor.map((item) => item.loanApplicationId),
    ...recurringPayments.map((item) => item.loanId).filter(Boolean),
  ]);
  const mixedLoanIds = buildMixedIdValues(loanIds);

  const [
    loanScheduleItems,
    loanGuarantorsByLoan,
    loanEditRequestsByLoan,
    loanFormPayments,
  ] = await Promise.all([
    loanIds.length
      ? withSession(
          LoanRepaymentScheduleItemModel.find({
            loanApplicationId: { $in: mixedLoanIds },
          }).lean(),
          session,
        )
      : Promise.resolve([]),
    loanIds.length
      ? withSession(
          LoanGuarantorModel.find({
            loanApplicationId: { $in: mixedLoanIds },
          }).lean(),
          session,
        )
      : Promise.resolve([]),
    loanIds.length
      ? withSession(
          LoanApplicationEditRequestModel.find({
            loanApplicationId: { $in: mixedLoanIds },
          }).lean(),
          session,
        )
      : Promise.resolve([]),
    loanIds.length
      ? withSession(
          FormPaymentModel.find({
            sourceModel: "LoanApplication",
            sourceId: { $in: mixedLoanIds },
          }).lean(),
          session,
        )
      : Promise.resolve([]),
  ]);

  const membershipFormPayments = membershipIds.length
    ? await withSession(
        FormPaymentModel.find({
          sourceModel: "GroupMembership",
          sourceId: { $in: buildMixedIdValues(membershipIds) },
        }).lean(),
        session,
      )
    : [];

  const allLoanGuarantors = dedupeDocs([
    ...loanGuarantorsAsGuarantor,
    ...loanGuarantorsByLoan,
  ]);
  const guarantorIds = unique(allLoanGuarantors.map((item) => item._id));
  const guarantorNotifications = guarantorIds.length
    ? await withSession(
        GuarantorNotificationModel.find({
          guarantorId: { $in: buildMixedIdValues(guarantorIds) },
        }).lean(),
        session,
      )
    : [];

  const allFormPayments = dedupeDocs([
    ...formPaymentsByUser,
    ...loanFormPayments,
    ...membershipFormPayments,
  ]);
  const formPaymentIds = unique(allFormPayments.map((item) => item._id));
  const formPaymentTransactionIds = unique(
    allFormPayments.map((item) => item.transactionId).filter(Boolean),
  );
  const formPaymentTransactionReferences = unique(
    allFormPayments.map((item) => item.transactionReference).filter(Boolean),
  );

  const contributionIds = unique(contributions.map((item) => item._id));
  const withdrawalIds = unique(withdrawals.map((item) => item._id));
  const transactionFilter = compactFilter(
    {},
    [
      { userId: { $in: profileIdValues } },
      contributionIds.length
        ? { "metadata.contributionId": { $in: buildMixedIdValues(contributionIds) } }
        : null,
      contributionIds.length
        ? { "metadata.bulkContributionIds": { $in: buildMixedIdValues(contributionIds) } }
        : null,
      withdrawalIds.length
        ? { "metadata.withdrawalRequestId": { $in: buildMixedIdValues(withdrawalIds) } }
        : null,
      loanIds.length ? { loanId: { $in: mixedLoanIds } } : null,
      loanIds.length ? { "metadata.loanId": { $in: mixedLoanIds } } : null,
      loanIds.length
        ? { "metadata.loanApplicationId": { $in: mixedLoanIds } }
        : null,
      formPaymentIds.length
        ? { "metadata.formPaymentId": { $in: buildMixedIdValues(formPaymentIds) } }
        : null,
      formPaymentTransactionIds.length
        ? { _id: { $in: buildMixedIdValues(formPaymentTransactionIds) } }
        : null,
      formPaymentTransactionReferences.length
        ? { reference: { $in: formPaymentTransactionReferences } }
        : null,
    ],
  );

  const transactions = await withSession(
    TransactionModel.find(transactionFilter).lean(),
    session,
  );

  const auditEntityIds = unique([
    ...profileIds,
    ...userIds,
    ...membershipIds,
    ...contributionIds,
    ...withdrawalIds,
    ...loanIds,
    ...formPaymentIds,
  ]);

  const auditLogs = await withSession(
    AuditLogModel.find(
      compactFilter(
        {},
        [
          userIdValues.length ? { actorUserId: { $in: userIdValues } } : null,
          userIdValues.length ? { targetUserId: { $in: userIdValues } } : null,
          profileIdValues.length
            ? { actorProfileId: { $in: profileIdValues } }
            : null,
          profileIdValues.length
            ? { targetProfileId: { $in: profileIdValues } }
            : null,
          membershipIds.length
            ? { membershipId: { $in: buildMixedIdValues(membershipIds) } }
            : null,
          auditEntityIds.length ? { entityId: { $in: auditEntityIds } } : null,
        ],
      ),
    ).lean(),
    session,
  );

  const phoneValues = unique([
    ...phones,
    ...formPaymentsByUser.map((item) => item.memberPhone).filter(isNonEmpty),
  ]);
  const phoneOtpSessions = phoneValues.length
    ? await withSession(
        PhoneOtpSessionModel.find({ phone: { $in: phoneValues } }).lean(),
        session,
      )
    : [];

  return {
    memberships,
    contributions,
    contributionSettings,
    withdrawals,
    bankAccounts,
    recurringPayments,
    notifications,
    notificationPreferences,
    meetingAttendances,
    meetingRsvps,
    voteResponses,
    loanApplications,
    loanScheduleItems,
    loanGuarantors: allLoanGuarantors,
    guarantorNotifications,
    loanEditRequests: dedupeDocs([...loanEditRequestsByUser, ...loanEditRequestsByLoan]),
    formPayments: allFormPayments,
    transactions,
    communicationLogs,
    refreshTokens,
    loginHistory,
    phoneOtpSessions,
    auditLogs,
    affectedGroupIds: unique([
      ...memberships.map((item) => item.groupId),
      ...contributions.map((item) => item.groupId),
      ...withdrawals.map((item) => item.groupId),
      ...loanApplications.map((item) => item.groupId),
    ]),
    affectedVoteIds: unique(voteResponses.map((item) => item.voteId)),
    profileIdValues,
    userIdValues,
    loanIds,
    membershipIds,
  };
}

function summarizeScope(scope, profiles, users) {
  return {
    profiles: { matched: profiles.length },
    users: { matched: users.length },
    memberships: { matched: scope.memberships.length },
    contributions: { matched: scope.contributions.length },
    contributionSettings: { matched: scope.contributionSettings.length },
    withdrawals: { matched: scope.withdrawals.length },
    bankAccounts: { matched: scope.bankAccounts.length },
    recurringPayments: { matched: scope.recurringPayments.length },
    notifications: { matched: scope.notifications.length },
    notificationPreferences: { matched: scope.notificationPreferences.length },
    meetingAttendances: { matched: scope.meetingAttendances.length },
    meetingRsvps: { matched: scope.meetingRsvps.length },
    voteResponses: { matched: scope.voteResponses.length },
    loanApplications: { matched: scope.loanApplications.length },
    loanScheduleItems: { matched: scope.loanScheduleItems.length },
    loanGuarantors: { matched: scope.loanGuarantors.length },
    guarantorNotifications: { matched: scope.guarantorNotifications.length },
    loanEditRequests: { matched: scope.loanEditRequests.length },
    formPayments: { matched: scope.formPayments.length },
    transactions: { matched: scope.transactions.length },
    communicationLogs: { matched: scope.communicationLogs.length },
    refreshTokens: { matched: scope.refreshTokens.length },
    loginHistory: { matched: scope.loginHistory.length },
    phoneOtpSessions: { matched: scope.phoneOtpSessions.length },
    auditLogs: { matched: scope.auditLogs.length },
    affectedGroups: scope.affectedGroupIds,
    affectedVotes: scope.affectedVoteIds,
  };
}

async function deleteByIds(Model, docs, session) {
  const ids = unique(docs.map((item) => item._id));
  if (ids.length === 0) return { deletedCount: 0 };
  return Model.deleteMany(
    { _id: { $in: buildMixedIdValues(ids) } },
    mongoOptions(session),
  );
}

async function cleanupSharedReferences({ scope, profileIds, userIds, session }) {
  const profileIdValues = buildMixedIdValues(profileIds);
  const userIdValues = buildMixedIdValues(userIds);
  const results = {};

  results.groupCoordinatorRefs = modifiedCount(
    await GroupModel.updateMany(
      { coordinatorId: { $in: profileIdValues } },
      {
        $set: {
          coordinatorId: null,
          coordinatorName: null,
          coordinatorPhone: null,
          coordinatorEmail: null,
        },
      },
      mongoOptions(session),
    ),
  );

  results.membershipReviewRefs = modifiedCount(
    await GroupMembershipModel.updateMany(
      { reviewedBy: { $in: profileIdValues } },
      { $set: { reviewedBy: null } },
      mongoOptions(session),
    ),
  );

  results.contributionVerifierRefs = modifiedCount(
    await ContributionModel.updateMany(
      { verifiedBy: { $in: profileIdValues } },
      { $set: { verifiedBy: null } },
      mongoOptions(session),
    ),
  );

  results.formPaymentReviewRefs = modifiedCount(
    await FormPaymentModel.updateMany(
      { reviewedBy: { $in: profileIdValues } },
      { $set: { reviewedBy: null } },
      mongoOptions(session),
    ),
  );

  results.voteCreatorRefs = modifiedCount(
    await GroupVoteModel.updateMany(
      { createdBy: { $in: profileIdValues } },
      { $set: { createdBy: null } },
      mongoOptions(session),
    ),
  );

  results.embeddedGuarantorRefs = modifiedCount(
    await LoanApplicationModel.updateMany(
      { "guarantors.profileId": { $in: profileIdValues } },
      { $pull: { guarantors: { profileId: { $in: profileIdValues } } } },
      mongoOptions(session),
    ),
  );

  const loanActorUpdates = await Promise.all([
    LoanApplicationModel.updateMany(
      { reviewedBy: { $in: profileIdValues } },
      { $set: { reviewedBy: null } },
      mongoOptions(session),
    ),
    LoanApplicationModel.updateMany(
      { disbursedBy: { $in: profileIdValues } },
      { $set: { disbursedBy: null } },
      mongoOptions(session),
    ),
    LoanApplicationModel.updateMany(
      { "manualDisbursement.initiatedBy": { $in: profileIdValues } },
      { $set: { "manualDisbursement.initiatedBy": null } },
      mongoOptions(session),
    ),
    LoanApplicationModel.updateMany(
      { "manualDisbursement.authorizedBy": { $in: profileIdValues } },
      { $set: { "manualDisbursement.authorizedBy": null } },
      mongoOptions(session),
    ),
    LoanApplicationModel.updateMany(
      { "manualDisbursement.initiatedByUserId": { $in: userIdValues } },
      { $set: { "manualDisbursement.initiatedByUserId": null } },
      mongoOptions(session),
    ),
  ]);
  results.loanActorRefs = loanActorUpdates.reduce(
    (sum, result) => sum + modifiedCount(result),
    0,
  );

  const withdrawalActorUpdates = await Promise.all([
    WithdrawalRequestModel.updateMany(
      { "manualPayout.initiatedBy": { $in: profileIdValues } },
      { $set: { "manualPayout.initiatedBy": null } },
      mongoOptions(session),
    ),
    WithdrawalRequestModel.updateMany(
      { "manualPayout.authorizedBy": { $in: profileIdValues } },
      { $set: { "manualPayout.authorizedBy": null } },
      mongoOptions(session),
    ),
    WithdrawalRequestModel.updateMany(
      { "manualPayout.initiatedByUserId": { $in: userIdValues } },
      { $set: { "manualPayout.initiatedByUserId": null } },
      mongoOptions(session),
    ),
  ]);
  results.withdrawalActorRefs = withdrawalActorUpdates.reduce(
    (sum, result) => sum + modifiedCount(result),
    0,
  );

  if (scope.affectedVoteIds.length > 0) {
    const voteSnapshots = [];
    for (const voteId of scope.affectedVoteIds) {
      voteSnapshots.push(await recomputeVoteSnapshot(voteId, session));
    }
    results.voteSnapshots = voteSnapshots;
  } else {
    results.voteSnapshots = [];
  }

  return results;
}

async function executeMemberDeletion({
  serials,
  dryRun,
  useTransaction,
  confirm,
}) {
  if (!dryRun && confirm !== CONFIRMATION_TOKEN) {
    throw new Error(
      `Refusing to delete without --confirm ${CONFIRMATION_TOKEN}. Run with --dry-run first.`,
    );
  }

  return runWithOptionalTransaction({
    useTransaction,
    work: async (session) => {
      const resolved = await resolveTargets({ serials, session });
      const deletableProfileIds = unique(
        resolved.serialResults
          .filter((entry) => entry.status === "matched")
          .map((entry) => entry.profileId),
      );
      const users = resolved.users.filter((user) =>
        deletableProfileIds.includes(String(user.profileId)),
      );
      const userIds = unique(users.map((user) => user._id));
      const profiles = resolved.profiles.filter((profile) =>
        deletableProfileIds.includes(String(profile._id)),
      );
      const phones = unique([
        ...profiles.map((profile) => profile.phone).filter(isNonEmpty),
        ...users.map((user) => user.phone).filter(isNonEmpty),
        ...users.map((user) => user.pendingPhone).filter(isNonEmpty),
      ]);

      const scope = await collectDeletionScope({
        profileIds: deletableProfileIds,
        userIds,
        phones,
        session,
      });
      const summary = summarizeScope(scope, profiles, users);

      const baseResult = {
        dryRun,
        confirmationRequired: dryRun ? CONFIRMATION_TOKEN : null,
        serials: resolved.serialResults,
        uniqueProfiles: profiles.map((profile) => ({
          profileId: String(profile._id),
          fullName: profile.fullName ?? null,
          email: profile.email ?? null,
          phone: profile.phone ?? null,
          userId: users.find((user) => String(user.profileId) === String(profile._id))
            ? String(users.find((user) => String(user.profileId) === String(profile._id))._id)
            : null,
        })),
        summary,
      };

      if (dryRun || profiles.length === 0) {
        return baseResult;
      }

      const deletionResults = {};

      deletionResults.guarantorNotifications = deletionCount(
        await deleteByIds(
          GuarantorNotificationModel,
          scope.guarantorNotifications,
          session,
        ),
      );
      deletionResults.loanGuarantors = deletionCount(
        await deleteByIds(LoanGuarantorModel, scope.loanGuarantors, session),
      );
      deletionResults.loanScheduleItems = deletionCount(
        await deleteByIds(
          LoanRepaymentScheduleItemModel,
          scope.loanScheduleItems,
          session,
        ),
      );
      deletionResults.loanEditRequests = deletionCount(
        await deleteByIds(
          LoanApplicationEditRequestModel,
          scope.loanEditRequests,
          session,
        ),
      );
      deletionResults.formPayments = deletionCount(
        await deleteByIds(FormPaymentModel, scope.formPayments, session),
      );
      deletionResults.transactions = deletionCount(
        await deleteByIds(TransactionModel, scope.transactions, session),
      );
      deletionResults.withdrawals = deletionCount(
        await deleteByIds(WithdrawalRequestModel, scope.withdrawals, session),
      );
      deletionResults.contributions = deletionCount(
        await deleteByIds(ContributionModel, scope.contributions, session),
      );
      deletionResults.contributionSettings = deletionCount(
        await deleteByIds(
          ContributionSettingModel,
          scope.contributionSettings,
          session,
        ),
      );
      deletionResults.recurringPayments = deletionCount(
        await deleteByIds(RecurringPaymentModel, scope.recurringPayments, session),
      );
      deletionResults.loanApplications = deletionCount(
        await deleteByIds(LoanApplicationModel, scope.loanApplications, session),
      );
      deletionResults.bankAccounts = deletionCount(
        await deleteByIds(BankAccountModel, scope.bankAccounts, session),
      );
      deletionResults.notifications = deletionCount(
        await deleteByIds(NotificationModel, scope.notifications, session),
      );
      deletionResults.notificationPreferences = deletionCount(
        await deleteByIds(
          NotificationPreferenceModel,
          scope.notificationPreferences,
          session,
        ),
      );
      deletionResults.meetingAttendances = deletionCount(
        await deleteByIds(
          MeetingAttendanceModel,
          scope.meetingAttendances,
          session,
        ),
      );
      deletionResults.meetingRsvps = deletionCount(
        await deleteByIds(MeetingRsvpModel, scope.meetingRsvps, session),
      );
      deletionResults.voteResponses = deletionCount(
        await deleteByIds(GroupVoteResponseModel, scope.voteResponses, session),
      );
      deletionResults.communicationLogs = deletionCount(
        await deleteByIds(CommunicationLogModel, scope.communicationLogs, session),
      );
      deletionResults.refreshTokens = deletionCount(
        await deleteByIds(RefreshTokenModel, scope.refreshTokens, session),
      );
      deletionResults.loginHistory = deletionCount(
        await deleteByIds(LoginHistoryModel, scope.loginHistory, session),
      );
      deletionResults.phoneOtpSessions = deletionCount(
        await deleteByIds(PhoneOtpSessionModel, scope.phoneOtpSessions, session),
      );
      deletionResults.auditLogs = deletionCount(
        await deleteByIds(AuditLogModel, scope.auditLogs, session),
      );
      deletionResults.memberships = deletionCount(
        await deleteByIds(GroupMembershipModel, scope.memberships, session),
      );

      const sharedReferenceCleanup = await cleanupSharedReferences({
        scope,
        profileIds: deletableProfileIds,
        userIds,
        session,
      });

      deletionResults.users = deletionCount(
        await deleteByIds(UserModel, users, session),
      );
      deletionResults.profiles = deletionCount(
        await deleteByIds(ProfileModel, profiles, session),
      );

      const groupSnapshots = [];
      for (const groupId of scope.affectedGroupIds) {
        groupSnapshots.push(await recomputeGroupSnapshot(groupId, session));
      }

      return {
        ...baseResult,
        deleted: deletionResults,
        sharedReferenceCleanup,
        groupSnapshots,
      };
    },
  });
}

const { args, serials } = parseCliArgs(process.argv.slice(2));

async function runCli() {
  if (args.help || args.h) {
    // eslint-disable-next-line no-console
    console.log(
      [
        "Usage:",
        "  node src/scripts/deleteMembersBySerials.js <memberSerial...> [--dry-run] [--no-transaction]",
        "  node src/scripts/deleteMembersBySerials.js --serials CRC/G21/0009,CRC/G8/0004 --dry-run",
        "",
        "Real deletion requires:",
        `  --confirm ${CONFIRMATION_TOKEN}`,
        "",
        "Notes:",
        "  The script resolves each memberSerial through GroupMembership.memberSerial.",
        "  It deletes the resolved profile, user account, memberships, contributions, withdrawals, loans, form payments, transactions, notifications, recurring payments, auth sessions, and related owned records.",
        "  Shared records are not deleted; references to the removed profile/user are cleared or recalculated.",
        `  A JSON log file is written to ${DEFAULT_LOG_DIR}.`,
      ].join("\n"),
    );
    process.exit(0);
  }

  const dryRun = Boolean(args["dry-run"]);
  const useTransaction = !Boolean(args["no-transaction"]);
  const outputDir = path.resolve(args.outputDir || args.logDir || DEFAULT_LOG_DIR);
  const startedAt = new Date().toISOString();

  try {
    const result = await executeMemberDeletion({
      serials,
      dryRun,
      useTransaction,
      confirm: args.confirm ? String(args.confirm) : null,
    });
    const payload = {
      ok: 1,
      startedAt,
      completedAt: new Date().toISOString(),
      dryRun,
      useTransaction,
      result,
    };
    const reportPath = await writeReport({ outputDir, payload });

    // eslint-disable-next-line no-console
    console.log(
      JSON.stringify(
        {
          ok: 1,
          dryRun,
          reportPath,
          summary: result.summary,
          deleted: result.deleted || null,
          serials: result.serials,
        },
        null,
        2,
      ),
    );
  } catch (error) {
    const payload = {
      ok: 0,
      startedAt,
      completedAt: new Date().toISOString(),
      dryRun,
      useTransaction,
      error: formatScriptError(error),
      serials,
    };
    const reportPath = await writeReport({ outputDir, payload });

    // eslint-disable-next-line no-console
    console.error(
      JSON.stringify(
        {
          ok: 0,
          error: payload.error,
          reportPath,
        },
        null,
        2,
      ),
    );
    process.exitCode = 1;
  }
}

runCli();
