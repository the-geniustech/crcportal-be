import fs from "fs/promises";
import mongoose from "mongoose";
import { ContributionModel } from "../../../models/Contribution.js";
import { GroupModel } from "../../../models/Group.js";
import { GroupMembershipModel } from "../../../models/GroupMembership.js";
import { ProfileModel } from "../../../models/Profile.js";
import { TransactionModel } from "../../../models/Transaction.js";
import { normalizeContributionType } from "../../../utils/contributionPolicy.js";
import {
  formatScriptError,
  runWithOptionalTransaction,
  withSession,
} from "../../utils/userDataCleanup.js";

const COUNTED_CONTRIBUTION_STATUSES = new Set(["verified", "completed"]);
const PROFILE_SETTING_KEYS = ["revolving", "endwell", "festive"];

const readJson = async (filePath) => {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
};

const fileExists = async (filePath) => {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
};

const asArray = (value) => (Array.isArray(value) ? value : []);

const unique = (values) =>
  Array.from(
    new Set(
      asArray(values)
        .map((value) => String(value || "").trim())
        .filter(Boolean),
    ),
  );

const mapBy = (items, resolver) => {
  const map = new Map();
  asArray(items).forEach((item) => {
    const key = typeof resolver === "function" ? resolver(item) : item?.[resolver];
    if (!key) return;
    map.set(String(key), item);
  });
  return map;
};

const indexByMany = (items, resolver) => {
  const map = new Map();
  asArray(items).forEach((item) => {
    const key = resolver(item);
    if (!key) return;
    const bucket = map.get(key) || [];
    bucket.push(item);
    map.set(key, bucket);
  });
  return map;
};

const toNumber = (value, fallback = null) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const roundMoney = (value) =>
  Math.round((toNumber(value, 0) || 0) * 100) / 100;

const isValidObjectId = (value) =>
  mongoose.Types.ObjectId.isValid(String(value || "").trim());

const toObjectId = (value) => new mongoose.Types.ObjectId(String(value));

const buildContributionNaturalKey = (record) =>
  [
    String(record.userId),
    String(record.groupId),
    String(record.contributionType),
    String(record.year),
    String(record.month),
  ].join("|");

const buildMembershipPairKey = ({ userId, groupId }) =>
  `${String(userId)}|${String(groupId)}`;

const buildProfileSettingGroupKey = ({ profileId, year }) =>
  `${String(profileId)}|${String(year)}`;

const pickRequiredFields = (item, fields) =>
  fields.filter((field) => item[field] === undefined || item[field] === null || item[field] === "");

const normalizeProfileUnits = (rawUnits) => {
  const base = {
    revolving: null,
    endwell: null,
    festive: null,
  };

  if (typeof rawUnits === "number" || typeof rawUnits === "string") {
    const numeric = toNumber(rawUnits);
    if (numeric && numeric > 0) {
      base.revolving = numeric;
    }
    return base;
  }

  if (!rawUnits || typeof rawUnits !== "object") {
    return base;
  }

  PROFILE_SETTING_KEYS.forEach((key) => {
    const numeric = toNumber(rawUnits[key]);
    if (numeric && numeric > 0) {
      base[key] = numeric;
    }
  });

  return base;
};

const normalizeContributionSeed = (item, index, stats) => {
  const normalized = {
    _id: String(item?._id || "").trim(),
    userId: String(item?.profileId || item?.userId || "").trim(),
    profileId: String(item?.profileId || item?.userId || "").trim(),
    groupId: String(item?.groupId || "").trim(),
    membershipId: String(item?.membershipId || "").trim(),
    contributionType: normalizeContributionType(
      item?.contributionType || item?.type,
    ),
    amount: roundMoney(item?.amount),
    month: toNumber(item?.month),
    year: toNumber(item?.year),
    units: toNumber(item?.units, 0) || 0,
    status: String(item?.status || "verified").trim().toLowerCase(),
    paymentReference: item?.paymentReference
      ? String(item.paymentReference).trim()
      : null,
    paymentMethod: item?.paymentMethod
      ? String(item.paymentMethod).trim()
      : "seed_import",
    interestAmount: roundMoney(item?.interestAmount ?? 0),
    verifiedAt: item?.verifiedAt || item?.date || item?.paymentDate || null,
    createdAt: item?.createdAt || item?.date || item?.paymentDate || null,
    seedKey: item?.seedKey ? String(item.seedKey) : null,
    memberSerial: item?.memberSerial ? String(item.memberSerial) : null,
    sourceFile: item?.sourceFile ? String(item.sourceFile) : null,
  };

  const missing = pickRequiredFields(normalized, [
    "_id",
    "userId",
    "groupId",
    "contributionType",
    "amount",
    "month",
    "year",
  ]);

  if (!isValidObjectId(normalized._id)) missing.push("_id:ObjectId");
  if (!isValidObjectId(normalized.userId)) missing.push("userId:ObjectId");
  if (!isValidObjectId(normalized.groupId)) missing.push("groupId:ObjectId");
  if (normalized.membershipId && !isValidObjectId(normalized.membershipId)) {
    missing.push("membershipId:ObjectId");
  }
  if (!Number.isInteger(normalized.month) || normalized.month < 1 || normalized.month > 12) {
    missing.push("month:1-12");
  }
  if (!Number.isInteger(normalized.year) || normalized.year < 2000) {
    missing.push("year:>=2000");
  }
  if (!Number.isFinite(normalized.amount) || normalized.amount < 0) {
    missing.push("amount:>=0");
  }

  if (missing.length) {
    stats.errors.push(
      `contributions[${index}] invalid: ${missing.join(", ")}`,
    );
    stats.contributions.invalid += 1;
    return null;
  }

  return normalized;
};

const normalizeTransactionSeed = (item, index, stats) => {
  const normalized = {
    _id: String(item?._id || "").trim(),
    reference: String(item?.reference || "").trim(),
    userId: String(item?.profileId || item?.userId || "").trim(),
    profileId: String(item?.profileId || item?.userId || "").trim(),
    groupId: item?.groupId ? String(item.groupId).trim() : null,
    amount: roundMoney(item?.amount),
    type: String(item?.type || "").trim(),
    status: String(item?.status || "").trim(),
    description: item?.description ? String(item.description) : "",
    channel: item?.channel ? String(item.channel) : null,
    groupName: item?.groupName ? String(item.groupName) : null,
    gateway: item?.gateway ? String(item.gateway) : "internal",
    date: item?.date || null,
    metadata: item?.metadata && typeof item.metadata === "object" ? item.metadata : {},
    seedKey: item?.seedKey ? String(item.seedKey) : null,
    contributionType: normalizeContributionType(
      item?.contributionType || item?.metadata?.contributionType,
    ),
  };

  const missing = pickRequiredFields(normalized, [
    "_id",
    "reference",
    "userId",
    "amount",
    "type",
    "status",
  ]);

  if (!isValidObjectId(normalized._id)) missing.push("_id:ObjectId");
  if (!isValidObjectId(normalized.userId)) missing.push("userId:ObjectId");
  if (normalized.groupId && !isValidObjectId(normalized.groupId)) {
    missing.push("groupId:ObjectId");
  }
  if (!Number.isFinite(normalized.amount) || normalized.amount < 0) {
    missing.push("amount:>=0");
  }

  const contributionId = normalized.metadata?.contributionId
    ? String(normalized.metadata.contributionId).trim()
    : null;
  if (contributionId && !isValidObjectId(contributionId)) {
    missing.push("metadata.contributionId:ObjectId");
  }
  normalized.contributionId = contributionId;

  if (missing.length) {
    stats.errors.push(
      `transactions[${index}] invalid: ${missing.join(", ")}`,
    );
    stats.transactions.invalid += 1;
    return null;
  }

  return normalized;
};

const normalizeProfileSettingSeed = (item, index, stats) => {
  const normalizedType = normalizeContributionType(
    item?.contributionType || item?.type,
  );

  const normalized = {
    seedKey: item?.seedKey ? String(item.seedKey) : null,
    profileId: String(item?.profileId || "").trim(),
    groupId: String(item?.groupId || "").trim(),
    membershipId: item?.membershipId ? String(item.membershipId).trim() : null,
    contributionType: normalizedType,
    type: item?.type ? String(item.type) : normalizedType,
    year: toNumber(item?.year),
    units: toNumber(item?.units),
    expectedMonthlyAmount: roundMoney(item?.expectedMonthlyAmount ?? 0),
    nextContributionSettings:
      item?.nextContributionSettings && typeof item.nextContributionSettings === "object"
        ? item.nextContributionSettings
        : null,
    currentContributionSettings:
      item?.currentContributionSettings && typeof item.currentContributionSettings === "object"
        ? item.currentContributionSettings
        : null,
    memberSerial: item?.memberSerial ? String(item.memberSerial) : null,
    sourceFiles: asArray(item?.sourceFiles),
  };

  const missing = pickRequiredFields(normalized, [
    "profileId",
    "groupId",
    "contributionType",
    "year",
    "units",
  ]);

  if (!isValidObjectId(normalized.profileId)) missing.push("profileId:ObjectId");
  if (!isValidObjectId(normalized.groupId)) missing.push("groupId:ObjectId");
  if (normalized.membershipId && !isValidObjectId(normalized.membershipId)) {
    missing.push("membershipId:ObjectId");
  }
  if (!Number.isInteger(normalized.year) || normalized.year < 2000) {
    missing.push("year:>=2000");
  }
  if (!Number.isFinite(normalized.units) || normalized.units <= 0) {
    missing.push("units:>0");
  }

  if (missing.length) {
    stats.errors.push(
      `profileContributionSettings[${index}] invalid: ${missing.join(", ")}`,
    );
    stats.profileContributionSettings.invalid += 1;
    return null;
  }

  return normalized;
};

const buildProfileSettingAggregate = (records, stats) => {
  const aggregates = new Map();

  records.forEach((record) => {
    const key = buildProfileSettingGroupKey(record);
    const current = aggregates.get(key) || {
      profileId: record.profileId,
      year: record.year,
      groupIds: new Set(),
      membershipIds: new Set(),
      types: {},
      sourceFiles: new Set(),
      seedKeys: new Set(),
      nextUpdatedAt: null,
    };

    current.groupIds.add(record.groupId);
    if (record.membershipId) current.membershipIds.add(record.membershipId);
    asArray(record.sourceFiles).forEach((fileName) => {
      if (fileName) current.sourceFiles.add(String(fileName));
    });
    if (record.seedKey) current.seedKeys.add(record.seedKey);

    const existingUnits = current.types[record.contributionType];
    if (
      Number.isFinite(existingUnits) &&
      roundMoney(existingUnits) !== roundMoney(record.units)
    ) {
      const resolvedUnits = Math.max(existingUnits, record.units);
      stats.warnings.push(
        `Profile ${record.profileId} has conflicting ${record.contributionType} units for ${record.year}; retained ${resolvedUnits}.`,
      );
      current.types[record.contributionType] = resolvedUnits;
    } else {
      current.types[record.contributionType] = record.units;
    }

    const nextUpdatedAt = record.nextContributionSettings?.updatedAt || null;
    if (nextUpdatedAt) {
      current.nextUpdatedAt = nextUpdatedAt;
    }

    aggregates.set(key, current);
  });

  return [...aggregates.values()];
};

const summarizeBulkWrite = (result) => ({
  upserted: Number(result?.upsertedCount ?? 0),
  modified: Number(result?.modifiedCount ?? 0),
  matched: Number(result?.matchedCount ?? 0),
});

async function loadSpecialContributionSeedFiles({
  inputDir,
  contributionsPath,
  transactionsPath,
  profileContributionSettingsPath,
  warningsPath,
  metaPath,
}) {
  const resolved = {
    contributionsPath,
    transactionsPath,
    profileContributionSettingsPath,
    warningsPath,
    metaPath,
  };

  const [contributions, transactions, profileContributionSettings] =
    await Promise.all([
      readJson(resolved.contributionsPath),
      readJson(resolved.transactionsPath),
      readJson(resolved.profileContributionSettingsPath),
    ]);

  const warnings =
    resolved.warningsPath && (await fileExists(resolved.warningsPath))
      ? await readJson(resolved.warningsPath)
      : [];
  const meta =
    resolved.metaPath && (await fileExists(resolved.metaPath))
      ? await readJson(resolved.metaPath)
      : null;

  return {
    inputDir,
    contributions: asArray(contributions),
    transactions: asArray(transactions),
    profileContributionSettings: asArray(profileContributionSettings),
    warnings: asArray(warnings),
    meta,
    paths: resolved,
  };
}

async function planSeedOperations({
  seed,
  stats,
  session,
  dryRun,
  forceSettingsYear,
}) {
  const normalizedContributions = seed.contributions
    .map((item, index) => normalizeContributionSeed(item, index, stats))
    .filter(Boolean);
  const normalizedTransactions = seed.transactions
    .map((item, index) => normalizeTransactionSeed(item, index, stats))
    .filter(Boolean);
  const normalizedSettings = seed.profileContributionSettings
    .map((item, index) => normalizeProfileSettingSeed(item, index, stats))
    .filter(Boolean);

  stats.contributions.valid = normalizedContributions.length;
  stats.transactions.valid = normalizedTransactions.length;
  stats.profileContributionSettings.valid = normalizedSettings.length;

  const contributionById = mapBy(normalizedContributions, "_id");
  const contributionNaturalKeys = normalizedContributions.map((item) => ({
    userId: item.userId,
    groupId: item.groupId,
    contributionType: item.contributionType,
    month: item.month,
    year: item.year,
  }));

  const profileIds = unique([
    ...normalizedContributions.map((item) => item.userId),
    ...normalizedTransactions.map((item) => item.userId),
    ...normalizedSettings.map((item) => item.profileId),
  ]);
  const groupIds = unique([
    ...normalizedContributions.map((item) => item.groupId),
    ...normalizedTransactions
      .map((item) => item.groupId)
      .filter(Boolean),
    ...normalizedSettings.map((item) => item.groupId),
  ]);
  const membershipIds = unique([
    ...normalizedContributions
      .map((item) => item.membershipId)
      .filter(Boolean),
    ...normalizedSettings
      .map((item) => item.membershipId)
      .filter(Boolean),
  ]);

  const [profiles, groups, memberships, existingContribByIdDocs, existingContribByNaturalKeyDocs] =
    await Promise.all([
      profileIds.length
        ? withSession(
            ProfileModel.find({ _id: { $in: profileIds } }).select(
              "_id contributionSettings fullName",
            ).lean(),
            session,
          )
        : [],
      groupIds.length
        ? withSession(
            GroupModel.find({ _id: { $in: groupIds } }).select(
              "_id groupName groupNumber totalSavings",
            ).lean(),
            session,
          )
        : [],
      membershipIds.length
        ? withSession(
            GroupMembershipModel.find({ _id: { $in: membershipIds } }).select(
              "_id userId groupId totalContributed memberSerial",
            ).lean(),
            session,
          )
        : [],
      normalizedContributions.length
        ? withSession(
            ContributionModel.find({
              _id: { $in: normalizedContributions.map((item) => item._id) },
            })
              .select(
                "_id userId groupId contributionType year month amount status paymentReference paymentMethod",
              )
              .lean(),
            session,
          )
        : [],
      contributionNaturalKeys.length
        ? withSession(
            ContributionModel.find({ $or: contributionNaturalKeys })
              .select(
                "_id userId groupId contributionType year month amount status paymentReference paymentMethod",
              )
              .lean(),
            session,
          )
        : [],
    ]);

  const profilesById = mapBy(profiles, "_id");
  const groupsById = mapBy(groups, "_id");
  const membershipsById = mapBy(memberships, "_id");
  const existingContribById = mapBy(existingContribByIdDocs, "_id");
  const existingContribByNaturalKey = indexByMany(
    existingContribByNaturalKeyDocs,
    buildContributionNaturalKey,
  );

  profileIds.forEach((profileId) => {
    if (!profilesById.has(profileId)) {
      stats.errors.push(`Missing Profile ${profileId}`);
    }
  });
  groupIds.forEach((groupId) => {
    if (!groupsById.has(groupId)) {
      stats.errors.push(`Missing Group ${groupId}`);
    }
  });
  membershipIds.forEach((membershipId) => {
    if (!membershipsById.has(membershipId)) {
      stats.errors.push(`Missing GroupMembership ${membershipId}`);
    }
  });

  const acceptedContributions = [];
  const skippedContributionIds = new Set();

  normalizedContributions.forEach((contribution) => {
    const profile = profilesById.get(contribution.userId);
    const group = groupsById.get(contribution.groupId);
    const membership = contribution.membershipId
      ? membershipsById.get(contribution.membershipId)
      : null;

    if (!profile || !group) {
      stats.contributions.skipped += 1;
      skippedContributionIds.add(contribution._id);
      return;
    }

    if (
      membership &&
      (String(membership.userId) !== contribution.userId ||
        String(membership.groupId) !== contribution.groupId)
    ) {
      stats.errors.push(
        `Contribution ${contribution._id} references membership ${contribution.membershipId} that does not match profile/group.`,
      );
      stats.contributions.skipped += 1;
      skippedContributionIds.add(contribution._id);
      return;
    }

    const existingById = existingContribById.get(contribution._id);
    if (
      existingById &&
      buildContributionNaturalKey(existingById) !==
        buildContributionNaturalKey(contribution)
    ) {
      stats.errors.push(
        `Contribution ${contribution._id} already exists with a different natural key in the database.`,
      );
      stats.contributions.conflicts += 1;
      stats.contributions.skipped += 1;
      skippedContributionIds.add(contribution._id);
      return;
    }

    const naturalKey = buildContributionNaturalKey(contribution);
    const existingWithSameKey =
      existingContribByNaturalKey.get(naturalKey) || [];
    const hasConflictingNaturalKey = existingWithSameKey.some(
      (doc) => String(doc._id) !== contribution._id,
    );

    if (hasConflictingNaturalKey) {
      stats.errors.push(
        `Contribution ${contribution._id} conflicts with an existing contribution that already owns ${naturalKey}.`,
      );
      stats.contributions.conflicts += 1;
      stats.contributions.skipped += 1;
      skippedContributionIds.add(contribution._id);
      return;
    }

    acceptedContributions.push(contribution);
  });

  const existingTxByIdDocs = normalizedTransactions.length
    ? await withSession(
        TransactionModel.find({
          _id: { $in: normalizedTransactions.map((item) => item._id) },
        })
          .select("_id reference userId groupId amount type status metadata")
          .lean(),
        session,
      )
    : [];

  const existingTxByReferenceDocs = normalizedTransactions.length
    ? await withSession(
        TransactionModel.find({
          reference: { $in: normalizedTransactions.map((item) => item.reference) },
        })
          .select("_id reference userId groupId amount type status metadata")
          .lean(),
        session,
      )
    : [];

  const existingTxByContributionDocs = normalizedTransactions
    .map((item) => item.contributionId)
    .filter(Boolean).length
    ? await withSession(
        TransactionModel.find({
          "metadata.contributionId": {
            $in: unique(
              normalizedTransactions
                .map((item) => item.contributionId)
                .filter(Boolean),
            ),
          },
          type: "group_contribution",
        })
          .select("_id reference userId groupId amount type status metadata")
          .lean(),
        session,
      )
    : [];

  const existingTxById = mapBy(existingTxByIdDocs, "_id");
  const existingTxByReference = indexByMany(
    existingTxByReferenceDocs,
    "reference",
  );
  const existingTxByContributionId = indexByMany(
    existingTxByContributionDocs,
    (item) => item?.metadata?.contributionId
      ? String(item.metadata.contributionId)
      : null,
  );

  const acceptedTransactions = [];

  normalizedTransactions.forEach((transaction) => {
    if (!profilesById.has(transaction.userId)) {
      stats.transactions.skipped += 1;
      return;
    }
    if (transaction.groupId && !groupsById.has(transaction.groupId)) {
      stats.transactions.skipped += 1;
      return;
    }

    if (transaction.contributionId) {
      const relatedContribution = contributionById.get(transaction.contributionId);
      if (!relatedContribution) {
        stats.errors.push(
          `Transaction ${transaction.reference} references contribution ${transaction.contributionId} that was not loaded from the seed files.`,
        );
        stats.transactions.skipped += 1;
        return;
      }
      if (skippedContributionIds.has(transaction.contributionId)) {
        stats.transactions.skipped += 1;
        return;
      }
      if (
        transaction.userId !== relatedContribution.userId ||
        String(transaction.groupId || "") !== String(relatedContribution.groupId || "")
      ) {
        stats.errors.push(
          `Transaction ${transaction.reference} does not match its contribution owner/group.`,
        );
        stats.transactions.conflicts += 1;
        stats.transactions.skipped += 1;
        return;
      }
    }

    const existingById = existingTxById.get(transaction._id);
    if (existingById && String(existingById.reference) !== transaction.reference) {
      stats.errors.push(
        `Transaction ${transaction._id} already exists with a different reference in the database.`,
      );
      stats.transactions.conflicts += 1;
      stats.transactions.skipped += 1;
      return;
    }

    const existingByReference =
      existingTxByReference.get(transaction.reference) || [];
    const referenceConflict = existingByReference.some(
      (doc) => String(doc._id) !== transaction._id,
    );
    if (referenceConflict) {
      stats.errors.push(
        `Transaction reference ${transaction.reference} already belongs to another database record.`,
      );
      stats.transactions.conflicts += 1;
      stats.transactions.skipped += 1;
      return;
    }

    if (transaction.contributionId) {
      const sameContributionTransactions =
        existingTxByContributionId.get(transaction.contributionId) || [];
      const contributionConflict = sameContributionTransactions.some(
        (doc) =>
          String(doc.reference) !== transaction.reference &&
          String(doc._id) !== transaction._id,
      );
      if (contributionConflict) {
        stats.errors.push(
          `Transaction ${transaction.reference} conflicts with an existing transaction already linked to contribution ${transaction.contributionId}.`,
        );
        stats.transactions.conflicts += 1;
        stats.transactions.skipped += 1;
        return;
      }
    }

    acceptedTransactions.push(transaction);
  });

  const settingAggregates = buildProfileSettingAggregate(normalizedSettings, stats);
  const acceptedProfileSettings = [];

  settingAggregates.forEach((aggregate) => {
    const profile = profilesById.get(aggregate.profileId);
    if (!profile) {
      stats.profileContributionSettings.skipped += 1;
      return;
    }

    const currentSettings =
      profile?.contributionSettings && typeof profile.contributionSettings === "object"
        ? profile.contributionSettings
        : null;
    const currentYear = toNumber(currentSettings?.year);

    if (
      currentYear &&
      currentYear !== aggregate.year &&
      !forceSettingsYear
    ) {
      stats.profileContributionSettings.yearMismatches += 1;
      stats.profileContributionSettings.skipped += 1;
      stats.warnings.push(
        `Profile ${aggregate.profileId} already has contributionSettings for ${currentYear}; skipped year ${aggregate.year}. Re-run with --force-settings-year to overwrite the year.`,
      );
      return;
    }

    const nextUnits = normalizeProfileUnits(currentSettings?.units);
    Object.entries(aggregate.types).forEach(([type, units]) => {
      nextUnits[type] = units;
    });

    acceptedProfileSettings.push({
      profileId: aggregate.profileId,
      year: aggregate.year,
      contributionSettings: {
        year: aggregate.year,
        units: nextUnits,
        updatedAt: aggregate.nextUpdatedAt || new Date().toISOString(),
      },
    });
  });

  stats.profileContributionSettings.groupedProfiles = acceptedProfileSettings.length;

  const impactedPairs = unique(
    acceptedContributions.map((item) =>
      buildMembershipPairKey({ userId: item.userId, groupId: item.groupId }),
    ),
  ).map((key) => {
    const [userId, groupId] = key.split("|");
    return { userId, groupId };
  });
  const impactedGroupIds = unique(
    acceptedContributions.map((item) => item.groupId),
  );

  const countedStatuses = [...COUNTED_CONTRIBUTION_STATUSES];

  const membershipTotals = impactedPairs.length
    ? await withSession(
        ContributionModel.aggregate([
          {
            $match: {
              status: { $in: countedStatuses },
              $or: impactedPairs.map((pair) => ({
                userId: toObjectId(pair.userId),
                groupId: toObjectId(pair.groupId),
              })),
            },
          },
          {
            $group: {
              _id: {
                userId: "$userId",
                groupId: "$groupId",
              },
              total: { $sum: "$amount" },
            },
          },
        ]),
        session,
      )
    : [];

  const groupTotals = impactedGroupIds.length
    ? await withSession(
        ContributionModel.aggregate([
          {
            $match: {
              status: { $in: countedStatuses },
              groupId: { $in: impactedGroupIds.map(toObjectId) },
            },
          },
          {
            $group: {
              _id: "$groupId",
              total: { $sum: "$amount" },
            },
          },
        ]),
        session,
      )
    : [];

  const existingImpactedMemberships = impactedPairs.length
    ? await withSession(
        GroupMembershipModel.find({
          $or: impactedPairs.map((pair) => ({
            userId: pair.userId,
            groupId: pair.groupId,
          })),
        })
          .select("_id userId groupId totalContributed")
          .lean(),
        session,
      )
    : [];

  const membershipTotalByPair = new Map(
    membershipTotals.map((item) => [
      buildMembershipPairKey({
        userId: item._id.userId,
        groupId: item._id.groupId,
      }),
      roundMoney(item.total),
    ]),
  );
  const groupTotalById = new Map(
    groupTotals.map((item) => [String(item._id), roundMoney(item.total)]),
  );

  const membershipTotalOps = existingImpactedMemberships
    .map((membership) => {
      const key = buildMembershipPairKey(membership);
      const expected = membershipTotalByPair.get(key) || 0;
      if (roundMoney(membership.totalContributed) === expected) return null;
      return {
        updateOne: {
          filter: { _id: membership._id },
          update: {
            $set: {
              totalContributed: expected,
            },
          },
        },
      };
    })
    .filter(Boolean);

  const groupTotalOps = groups
    .filter((group) => impactedGroupIds.includes(String(group._id)))
    .map((group) => {
      const expected = groupTotalById.get(String(group._id)) || 0;
      if (roundMoney(group.totalSavings) === expected) return null;
      return {
        updateOne: {
          filter: { _id: group._id },
          update: {
            $set: {
              totalSavings: expected,
            },
          },
        },
      };
    })
    .filter(Boolean);

  const contributionOps = acceptedContributions.map((contribution) => ({
    updateOne: {
      filter: { _id: contribution._id },
      update: {
        $set: {
          userId: contribution.userId,
          groupId: contribution.groupId,
          month: contribution.month,
          year: contribution.year,
          amount: contribution.amount,
          contributionType: contribution.contributionType,
          units: contribution.units,
          interestAmount: contribution.interestAmount,
          status: contribution.status,
          paymentReference: contribution.paymentReference,
          paymentMethod: contribution.paymentMethod,
          verifiedAt: contribution.verifiedAt,
        },
        $setOnInsert: {
          createdAt: contribution.createdAt || contribution.verifiedAt || new Date().toISOString(),
        },
      },
      upsert: true,
    },
  }));

  const transactionOps = acceptedTransactions.map((transaction) => ({
    updateOne: {
      filter: { reference: transaction.reference },
      update: {
        $set: {
          userId: transaction.userId,
          amount: transaction.amount,
          type: transaction.type,
          status: transaction.status,
          description: transaction.description,
          channel: transaction.channel,
          groupId: transaction.groupId || null,
          groupName: transaction.groupName || null,
          metadata: transaction.metadata,
          gateway: transaction.gateway,
          date: transaction.date || new Date().toISOString(),
        },
        $setOnInsert: {
          _id: transaction._id,
        },
      },
      upsert: true,
    },
  }));

  const profileSettingOps = acceptedProfileSettings.map((record) => ({
    updateOne: {
      filter: { _id: record.profileId },
      update: {
        $set: {
          contributionSettings: record.contributionSettings,
        },
      },
    },
  }));

  const plan = {
    contributions: {
      accepted: acceptedContributions,
      ops: contributionOps,
    },
    transactions: {
      accepted: acceptedTransactions,
      ops: transactionOps,
    },
    profileContributionSettings: {
      accepted: acceptedProfileSettings,
      ops: profileSettingOps,
    },
    membershipTotals: {
      ops: membershipTotalOps,
    },
    groupTotals: {
      ops: groupTotalOps,
    },
  };

  stats.totals.membershipsPlanned = membershipTotalOps.length;
  stats.totals.groupsPlanned = groupTotalOps.length;

  if (dryRun) {
    return plan;
  }

  const bulkOptions = session ? { ordered: false, session } : { ordered: false };

  if (profileSettingOps.length) {
    const result = await ProfileModel.bulkWrite(profileSettingOps, bulkOptions);
    const summary = summarizeBulkWrite(result);
    stats.profileContributionSettings.updated += summary.modified;
    stats.profileContributionSettings.matched += summary.matched;
    stats.profileContributionSettings.modified += summary.modified;
  }

  if (contributionOps.length) {
    const result = await ContributionModel.bulkWrite(contributionOps, bulkOptions);
    const summary = summarizeBulkWrite(result);
    stats.contributions.upserted += summary.upserted;
    stats.contributions.matched += summary.matched;
    stats.contributions.modified += summary.modified;
  }

  if (transactionOps.length) {
    const result = await TransactionModel.bulkWrite(transactionOps, bulkOptions);
    const summary = summarizeBulkWrite(result);
    stats.transactions.upserted += summary.upserted;
    stats.transactions.matched += summary.matched;
    stats.transactions.modified += summary.modified;
  }

  if (membershipTotalOps.length) {
    const result = await GroupMembershipModel.bulkWrite(
      membershipTotalOps,
      bulkOptions,
    );
    stats.totals.membershipsUpdated +=
      Number(result?.modifiedCount ?? 0) + Number(result?.upsertedCount ?? 0);
  }

  if (groupTotalOps.length) {
    const result = await GroupModel.bulkWrite(groupTotalOps, bulkOptions);
    stats.totals.groupsUpdated +=
      Number(result?.modifiedCount ?? 0) + Number(result?.upsertedCount ?? 0);
  }

  return plan;
}

export async function seedSpecialContributionData({
  inputDir,
  contributionsPath,
  transactionsPath,
  profileContributionSettingsPath,
  warningsPath,
  metaPath,
  dryRun = false,
  useTransaction = true,
  failOnWarnings = false,
  forceSettingsYear = false,
}) {
  const seed = await loadSpecialContributionSeedFiles({
    inputDir,
    contributionsPath,
    transactionsPath,
    profileContributionSettingsPath,
    warningsPath,
    metaPath,
  });

  const stats = {
    generator: {
      warnings: seed.warnings.length,
      workbooks: Number(seed?.meta?.totals?.workbooks ?? 0),
    },
    contributions: {
      total: seed.contributions.length,
      valid: 0,
      invalid: 0,
      upserted: 0,
      matched: 0,
      modified: 0,
      skipped: 0,
      conflicts: 0,
    },
    transactions: {
      total: seed.transactions.length,
      valid: 0,
      invalid: 0,
      upserted: 0,
      matched: 0,
      modified: 0,
      skipped: 0,
      conflicts: 0,
    },
    profileContributionSettings: {
      total: seed.profileContributionSettings.length,
      valid: 0,
      invalid: 0,
      groupedProfiles: 0,
      updated: 0,
      matched: 0,
      modified: 0,
      skipped: 0,
      conflicts: 0,
      yearMismatches: 0,
    },
    totals: {
      membershipsPlanned: 0,
      membershipsUpdated: 0,
      groupsPlanned: 0,
      groupsUpdated: 0,
    },
    warnings: [...seed.warnings.map((warning) => warning?.message || JSON.stringify(warning))],
    errors: [],
  };

  if (failOnWarnings && seed.warnings.length > 0) {
    throw new Error(
      `Seed generation warnings are present (${seed.warnings.length}). Re-run without --fail-on-warnings if you want to proceed anyway.`,
    );
  }

  try {
    const result = await runWithOptionalTransaction({
      useTransaction,
      work: async (session) =>
        planSeedOperations({
          seed,
          stats,
          session,
          dryRun,
          forceSettingsYear,
        }),
    });

    return {
      ok: stats.errors.length ? 0 : 1,
      dryRun,
      inputDir,
      paths: seed.paths,
      meta: seed.meta,
      stats,
      plan:
        dryRun && result
          ? {
              contributions: result.contributions.accepted.length,
              transactions: result.transactions.accepted.length,
              profileContributionSettings:
                result.profileContributionSettings.accepted.length,
              membershipTotals: result.membershipTotals.ops.length,
              groupTotals: result.groupTotals.ops.length,
            }
          : undefined,
    };
  } catch (error) {
    throw new Error(formatScriptError(error));
  }
}
