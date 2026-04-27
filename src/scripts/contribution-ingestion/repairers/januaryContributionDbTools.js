import { ContributionModel } from "../../../models/Contribution.js";
import { ContributionSettingModel } from "../../../models/ContributionSetting.js";
import { GroupMembershipModel } from "../../../models/GroupMembership.js";
import { GroupModel } from "../../../models/Group.js";
import { TransactionModel } from "../../../models/Transaction.js";
import { withSession } from "../../utils/userDataCleanup.js";

const COUNTED_STATUSES = new Set(["verified", "completed"]);
const REPAIR_KEY = "januaryContributionHalved";

const asArray = (value) => (Array.isArray(value) ? value : []);
const toId = (value) => String(value ?? "").trim();
const toNumber = (value, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};
const roundMoney = (value) => Math.round(toNumber(value) * 100) / 100;
const nearlyEqual = (left, right, tolerance = 0.01) =>
  Math.abs(roundMoney(left) - roundMoney(right)) <= tolerance;
const buildTupleKey = (...parts) => parts.map((part) => toId(part)).join("|");

const median = (values) => {
  const cleaned = asArray(values)
    .map((value) => roundMoney(value))
    .filter((value) => Number.isFinite(value) && value > 0)
    .sort((a, b) => a - b);
  if (cleaned.length === 0) return null;
  const middle = Math.floor(cleaned.length / 2);
  if (cleaned.length % 2 === 0) {
    return roundMoney((cleaned[middle - 1] + cleaned[middle]) / 2);
  }
  return roundMoney(cleaned[middle]);
};

const uniqueMoneyValues = (values) => {
  const uniques = [];
  asArray(values).forEach((value) => {
    const safeValue = roundMoney(value);
    if (!Number.isFinite(safeValue) || safeValue <= 0) return;
    if (uniques.some((existing) => nearlyEqual(existing, safeValue))) return;
    uniques.push(safeValue);
  });
  return uniques;
};

const isCountedContribution = (contribution) =>
  COUNTED_STATUSES.has(
    String(contribution?.status || "").trim().toLowerCase(),
  );

const getRepairMarker = (transaction) =>
  transaction?.metadata?.repairs?.[REPAIR_KEY] ?? null;

function normalizeGroupNumbers(groupNumbers) {
  const raw = Array.isArray(groupNumbers)
    ? groupNumbers
    : typeof groupNumbers === "string"
      ? groupNumbers.split(",")
      : [];
  return [...new Set(
    raw
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value) && value > 0),
  )];
}

async function resolveGroupIds(groupNumbers, session) {
  const normalized = normalizeGroupNumbers(groupNumbers);
  if (normalized.length === 0) {
    return {
      requestedGroupNumbers: [],
      foundGroups: [],
      foundGroupIds: [],
      missingGroupNumbers: [],
    };
  }

  const groups = await withSession(
    GroupModel.find(
      { groupNumber: { $in: normalized } },
      { _id: 1, groupNumber: 1, groupName: 1 },
    ).lean(),
    session,
  );

  const foundNumbers = new Set(groups.map((group) => Number(group.groupNumber)));
  return {
    requestedGroupNumbers: normalized,
    foundGroups: groups,
    foundGroupIds: groups.map((group) => group._id),
    missingGroupNumbers: normalized.filter((value) => !foundNumbers.has(value)),
  };
}

function buildSeedTransactionQuery({ year, month, groupIds }) {
  const query = {
    type: "group_contribution",
    status: "success",
    channel: "seed",
    gateway: "internal",
    "metadata.year": Number(year),
    "metadata.month": Number(month),
    "metadata.contributionId": { $exists: true, $ne: null },
  };

  if (Array.isArray(groupIds) && groupIds.length > 0) {
    query.groupId = { $in: groupIds };
  }

  return query;
}

function buildSeedNonJanuaryTransactionQuery({ year, month, groupIds }) {
  const query = {
    type: "group_contribution",
    status: "success",
    channel: "seed",
    gateway: "internal",
    "metadata.year": Number(year),
    "metadata.month": { $ne: Number(month) },
    "metadata.contributionId": { $exists: true, $ne: null },
  };

  if (Array.isArray(groupIds) && groupIds.length > 0) {
    query.groupId = { $in: groupIds };
  }

  return query;
}

export async function loadJanuaryContributionDbScope({
  year,
  month = 1,
  groupNumbers = [],
  session = null,
}) {
  const groupScope = await resolveGroupIds(groupNumbers, session);
  const transactionQuery = buildSeedTransactionQuery({
    year,
    month,
    groupIds: groupScope.foundGroupIds,
  });

  const transactions = await withSession(
    TransactionModel.find(transactionQuery, {
      _id: 1,
      amount: 1,
      reference: 1,
      groupId: 1,
      groupName: 1,
      metadata: 1,
    }).lean(),
    session,
  );

  const contributionIds = [...new Set(
    transactions
      .map((transaction) => transaction?.metadata?.contributionId)
      .filter(Boolean)
      .map(toId),
  )];

  const contributions = contributionIds.length
    ? await withSession(
        ContributionModel.find(
          {
            _id: { $in: contributionIds },
            year: Number(year),
            month: Number(month),
          },
          {
            _id: 1,
            userId: 1,
            groupId: 1,
            amount: 1,
            month: 1,
            year: 1,
            contributionType: 1,
            status: 1,
            units: 1,
          },
        ).lean(),
        session,
      )
    : [];

  const contributionById = new Map(
    contributions.map((contribution) => [toId(contribution._id), contribution]),
  );

  const validTransactions = transactions.filter((transaction) =>
    contributionById.has(toId(transaction?.metadata?.contributionId)),
  );

  const tupleMap = new Map();
  contributions.forEach((contribution) => {
    const key = buildTupleKey(
      contribution.userId,
      contribution.groupId,
      contribution.year,
      contribution.contributionType || "revolving",
    );
    if (!tupleMap.has(key)) {
      tupleMap.set(key, {
        userId: contribution.userId,
        groupId: contribution.groupId,
        year: Number(contribution.year),
        contributionType: String(
          contribution.contributionType || "revolving",
        ).trim(),
      });
    }
  });

  const tuples = Array.from(tupleMap.values());
  const settings = tuples.length
    ? await withSession(
        ContributionSettingModel.find(
          {
            $or: tuples.map((tuple) => ({
              userId: tuple.userId,
              groupId: tuple.groupId,
              year: tuple.year,
              contributionType: tuple.contributionType,
            })),
          },
          {
            _id: 1,
            userId: 1,
            groupId: 1,
            year: 1,
            contributionType: 1,
            expectedMonthlyAmount: 1,
            totalExpected: 1,
            totalActual: 1,
            outstandingBalance: 1,
          },
        ).lean(),
        session,
      )
    : [];

  const seedNonJanuaryTransactions = await withSession(
    TransactionModel.find(
      buildSeedNonJanuaryTransactionQuery({
        year,
        month,
        groupIds: groupScope.foundGroupIds,
      }),
      {
        _id: 1,
        metadata: 1,
      },
    ).lean(),
    session,
  );

  const seedNonJanuaryContributionIds = [...new Set(
    seedNonJanuaryTransactions
      .map((transaction) => transaction?.metadata?.contributionId)
      .filter(Boolean)
      .map(toId),
  )];

  const seedNonJanuaryContributions = seedNonJanuaryContributionIds.length
    ? await withSession(
        ContributionModel.find(
          {
            _id: { $in: seedNonJanuaryContributionIds },
            year: Number(year),
            month: { $ne: Number(month) },
            amount: { $gt: 0 },
          },
          {
            _id: 1,
            userId: 1,
            groupId: 1,
            year: 1,
            contributionType: 1,
            amount: 1,
            month: 1,
            status: 1,
            units: 1,
          },
        ).lean(),
        session,
      )
    : [];

  const settingsByTuple = new Map(
    settings.map((setting) => [
      buildTupleKey(
        setting.userId,
        setting.groupId,
        setting.year,
        setting.contributionType || "revolving",
      ),
      setting,
    ]),
  );

  const siblingAmountsByTuple = new Map();
  seedNonJanuaryContributions.forEach((contribution) => {
    const key = buildTupleKey(
      contribution.userId,
      contribution.groupId,
      contribution.year,
      contribution.contributionType || "revolving",
    );
    const current = siblingAmountsByTuple.get(key) || [];
    current.push(roundMoney(contribution.amount));
    siblingAmountsByTuple.set(key, current);
  });

  const groupUnitRatesByGroupType = new Map();
  seedNonJanuaryContributions.forEach((contribution) => {
    if (!isCountedContribution(contribution)) return;
    const units = toNumber(contribution.units);
    const amount = toNumber(contribution.amount);
    if (!(units > 0) || !(amount > 0)) return;

    const key = buildTupleKey(
      contribution.groupId,
      contribution.contributionType || "revolving",
    );
    const current = groupUnitRatesByGroupType.get(key) || [];
    current.push(roundMoney(amount / units));
    groupUnitRatesByGroupType.set(key, current);
  });

  const groupUnitRateMedianByGroupType = new Map(
    Array.from(groupUnitRatesByGroupType.entries()).map(([key, values]) => [
      key,
      median(values),
    ]),
  );

  const transactionByContributionId = new Map(
    validTransactions.map((transaction) => [
      toId(transaction?.metadata?.contributionId),
      transaction,
    ]),
  );

  return {
    year: Number(year),
    month: Number(month),
    groupScope,
    transactions: validTransactions,
    contributions,
    settings,
    tuples,
    contributionById,
    settingsByTuple,
    siblingAmountsByTuple,
    groupUnitRateMedianByGroupType,
    transactionByContributionId,
  };
}

function resolveExpectedAmount({
  contribution,
  transaction,
  setting,
  siblingAmounts,
  groupUnitRate,
}) {
  const marker = getRepairMarker(transaction);
  const siblingMedian = median(siblingAmounts);
  const contributionUnits = toNumber(contribution?.units);
  const settingUnits = toNumber(setting?.units);
  const resolvedUnits = contributionUnits > 0 ? contributionUnits : settingUnits;
  const groupExpectedAmount =
    toNumber(groupUnitRate) > 0 && resolvedUnits > 0
      ? roundMoney(groupUnitRate * resolvedUnits)
      : null;
  const settingExpectedAmount =
    toNumber(setting?.expectedMonthlyAmount) > 0
      ? roundMoney(setting.expectedMonthlyAmount)
      : null;
  const candidates = [];

  if (toNumber(marker?.expectedAmount) > 0) {
    candidates.push({
      source: "repair_marker",
      value: roundMoney(marker.expectedAmount),
    });
  }
  if (groupExpectedAmount) {
    candidates.push({
      source: "group_non_january_unit_median",
      value: groupExpectedAmount,
    });
  }
  if (settingExpectedAmount) {
    candidates.push({
      source: "contribution_setting",
      value: settingExpectedAmount,
    });
  }
  if (toNumber(siblingMedian) > 0) {
    candidates.push({
      source: "sibling_months_median",
      value: roundMoney(siblingMedian),
    });
  }

  const currentAmount = roundMoney(contribution?.amount);
  const markerExpectedAmount =
    toNumber(marker?.expectedAmount) > 0
      ? roundMoney(marker.expectedAmount)
      : null;

  if (markerExpectedAmount && nearlyEqual(currentAmount, markerExpectedAmount)) {
    return {
      expectedAmount: markerExpectedAmount,
      source: "repair_marker",
      sources: candidates,
      reason: "current_matches_repair_marker",
      groupExpectedAmount,
      siblingExpectedAmount: siblingMedian,
      settingExpectedAmount,
    };
  }

  if (
    groupExpectedAmount &&
    nearlyEqual(currentAmount, groupExpectedAmount * 2)
  ) {
    return {
      expectedAmount: groupExpectedAmount,
      source: "group_non_january_unit_median",
      sources: candidates,
      reason: "current_matches_double_group_rate",
      groupExpectedAmount,
      siblingExpectedAmount: siblingMedian,
      settingExpectedAmount,
    };
  }

  if (groupExpectedAmount && nearlyEqual(currentAmount, groupExpectedAmount)) {
    return {
      expectedAmount: groupExpectedAmount,
      source: "group_non_january_unit_median",
      sources: candidates,
      reason: "current_matches_group_rate",
      groupExpectedAmount,
      siblingExpectedAmount: siblingMedian,
      settingExpectedAmount,
    };
  }

  if (toNumber(siblingMedian) > 0 && nearlyEqual(currentAmount, siblingMedian * 2)) {
    return {
      expectedAmount: roundMoney(siblingMedian),
      source: "sibling_months_median",
      sources: candidates,
      reason: "current_matches_double_sibling_median",
      groupExpectedAmount,
      siblingExpectedAmount: roundMoney(siblingMedian),
      settingExpectedAmount,
    };
  }

  if (toNumber(siblingMedian) > 0 && nearlyEqual(currentAmount, siblingMedian)) {
    return {
      expectedAmount: roundMoney(siblingMedian),
      source: "sibling_months_median",
      sources: candidates,
      reason: "current_matches_sibling_median",
      groupExpectedAmount,
      siblingExpectedAmount: roundMoney(siblingMedian),
      settingExpectedAmount,
    };
  }

  if (
    settingExpectedAmount &&
    nearlyEqual(currentAmount, settingExpectedAmount * 2)
  ) {
    return {
      expectedAmount: settingExpectedAmount,
      source: "contribution_setting",
      sources: candidates,
      reason: "current_matches_double_contribution_setting",
      groupExpectedAmount,
      siblingExpectedAmount: siblingMedian,
      settingExpectedAmount,
    };
  }

  if (settingExpectedAmount && nearlyEqual(currentAmount, settingExpectedAmount)) {
    return {
      expectedAmount: settingExpectedAmount,
      source: "contribution_setting",
      sources: candidates,
      reason: "current_matches_contribution_setting",
      groupExpectedAmount,
      siblingExpectedAmount: siblingMedian,
      settingExpectedAmount,
    };
  }

  const uniqueValues = uniqueMoneyValues(candidates.map((item) => item.value));
  if (uniqueValues.length === 0) {
    return {
      expectedAmount: null,
      source: null,
      sources: candidates,
      reason: "no_expected_source",
      groupExpectedAmount,
      siblingExpectedAmount: siblingMedian,
      settingExpectedAmount,
    };
  }

  if (uniqueValues.length > 1) {
    return {
      expectedAmount: null,
      source: null,
      sources: candidates,
      reason: "conflicting_expected_sources",
      groupExpectedAmount,
      siblingExpectedAmount: siblingMedian,
      settingExpectedAmount,
    };
  }

  const expectedAmount = uniqueValues[0];
  const primarySource =
    candidates.find((item) => nearlyEqual(item.value, expectedAmount))?.source ??
    "derived";

  return {
    expectedAmount,
    source: primarySource,
    sources: candidates,
    reason: null,
    groupExpectedAmount,
    siblingExpectedAmount: siblingMedian,
    settingExpectedAmount,
  };
}

export function buildJanuaryContributionDiagnostics(scope) {
  return scope.contributions.map((contribution) => {
    const transaction = scope.transactionByContributionId.get(toId(contribution._id));
    const tupleKey = buildTupleKey(
      contribution.userId,
      contribution.groupId,
      contribution.year,
      contribution.contributionType || "revolving",
    );
    const setting = scope.settingsByTuple.get(tupleKey) || null;
    const siblingAmounts = scope.siblingAmountsByTuple.get(tupleKey) || [];
    const expected = resolveExpectedAmount({
      contribution,
      transaction,
      setting,
      siblingAmounts,
      groupUnitRate: scope.groupUnitRateMedianByGroupType.get(
        buildTupleKey(
          contribution.groupId,
          contribution.contributionType || "revolving",
        ),
      ),
    });
    const currentAmount = roundMoney(contribution.amount);
    const marker = getRepairMarker(transaction);

    let status = "ambiguous";
    if (expected.expectedAmount === null) {
      status = marker ? "drifted_after_repair" : "ambiguous";
    } else if (nearlyEqual(currentAmount, expected.expectedAmount)) {
      status = marker ? "corrected" : "clean_without_marker";
    } else if (nearlyEqual(currentAmount, expected.expectedAmount * 2)) {
      status = "uncorrected";
    } else if (marker) {
      status = "drifted_after_repair";
    } else {
      status = "mismatch";
    }

    return {
      contributionId: toId(contribution._id),
      transactionId: toId(transaction?._id),
      transactionReference: toId(transaction?.reference),
      userId: toId(contribution.userId),
      groupId: toId(contribution.groupId),
      year: Number(contribution.year),
      month: Number(contribution.month),
      contributionType: String(
        contribution.contributionType || "revolving",
      ).trim(),
      status,
      counted: isCountedContribution(contribution),
      currentAmount,
      expectedAmount:
        expected.expectedAmount === null ? null : roundMoney(expected.expectedAmount),
      groupExpectedAmount:
        expected.groupExpectedAmount === null
          ? null
          : roundMoney(expected.groupExpectedAmount),
      siblingExpectedAmount:
        expected.siblingExpectedAmount === null
          ? null
          : roundMoney(expected.siblingExpectedAmount),
      settingExpectedAmount:
        expected.settingExpectedAmount === null
          ? null
          : roundMoney(expected.settingExpectedAmount),
      delta:
        expected.expectedAmount === null
          ? 0
          : roundMoney(expected.expectedAmount - currentAmount),
      repairable:
        ["uncorrected", "drifted_after_repair"].includes(status) &&
        expected.expectedAmount !== null,
      expectedSource: expected.source,
      expectedSources: expected.sources,
      expectedReason: expected.reason,
      hasRepairMarker: Boolean(marker),
      repairMarker: marker,
      transactionAmount: roundMoney(transaction?.amount),
    };
  });
}

export function summarizeDiagnostics(diagnostics) {
  return diagnostics.reduce(
    (summary, item) => {
      summary.total += 1;
      summary.statusCounts[item.status] =
        Number(summary.statusCounts[item.status] ?? 0) + 1;
      if (item.repairable) {
        summary.repairable += 1;
        summary.totalDelta = roundMoney(summary.totalDelta + item.delta);
      }
      return summary;
    },
    {
      total: 0,
      repairable: 0,
      totalDelta: 0,
      statusCounts: {},
    },
  );
}

export function buildRepairDeltaMaps(diagnostics) {
  const membershipDeltas = new Map();
  const groupDeltas = new Map();
  const settingDeltas = new Map();

  diagnostics.forEach((item) => {
    if (!item.repairable || !item.counted) return;
    if (!Number.isFinite(item.delta) || item.delta === 0) return;

    const membershipKey = buildTupleKey(item.userId, item.groupId);
    const groupKey = toId(item.groupId);
    const settingKey = buildTupleKey(
      item.userId,
      item.groupId,
      item.year,
      item.contributionType,
    );

    membershipDeltas.set(
      membershipKey,
      roundMoney(toNumber(membershipDeltas.get(membershipKey)) + item.delta),
    );
    groupDeltas.set(
      groupKey,
      roundMoney(toNumber(groupDeltas.get(groupKey)) + item.delta),
    );
    settingDeltas.set(
      settingKey,
      roundMoney(toNumber(settingDeltas.get(settingKey)) + item.delta),
    );
  });

  return { membershipDeltas, groupDeltas, settingDeltas };
}

export async function aggregateCurrentMembershipTotals(membershipKeys, session) {
  if (membershipKeys.length === 0) return new Map();
  const rows = await withSession(
    ContributionModel.aggregate([
      {
        $match: {
          status: { $in: Array.from(COUNTED_STATUSES) },
          $or: membershipKeys.map((item) => ({
            userId: item.userId,
            groupId: item.groupId,
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
  );

  return new Map(
    rows.map((row) => [
      buildTupleKey(row?._id?.userId, row?._id?.groupId),
      roundMoney(row?.total),
    ]),
  );
}

export async function aggregateCurrentGroupTotals(groupIds, session) {
  if (groupIds.length === 0) return new Map();
  const rows = await withSession(
    ContributionModel.aggregate([
      {
        $match: {
          status: { $in: Array.from(COUNTED_STATUSES) },
          groupId: { $in: groupIds },
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
  );

  return new Map(
    rows.map((row) => [toId(row?._id), roundMoney(row?.total)]),
  );
}

export async function aggregateCurrentSettingTotals(settingKeys, session) {
  if (settingKeys.length === 0) return new Map();
  const rows = await withSession(
    ContributionModel.aggregate([
      {
        $match: {
          status: { $in: Array.from(COUNTED_STATUSES) },
          $or: settingKeys.map((item) => ({
            userId: item.userId,
            groupId: item.groupId,
            year: item.year,
            contributionType: item.contributionType,
          })),
        },
      },
      {
        $group: {
          _id: {
            userId: "$userId",
            groupId: "$groupId",
            year: "$year",
            contributionType: "$contributionType",
          },
          total: { $sum: "$amount" },
        },
      },
    ]),
    session,
  );

  return new Map(
    rows.map((row) => [
      buildTupleKey(
        row?._id?.userId,
        row?._id?.groupId,
        row?._id?.year,
        row?._id?.contributionType,
      ),
      roundMoney(row?.total),
    ]),
  );
}

export async function loadAffectedMembershipDocs(membershipKeys, session) {
  if (membershipKeys.length === 0) return [];
  return withSession(
    GroupMembershipModel.find(
      {
        $or: membershipKeys.map((item) => ({
          userId: item.userId,
          groupId: item.groupId,
        })),
      },
      {
        _id: 1,
        userId: 1,
        groupId: 1,
        totalContributed: 1,
      },
    ).lean(),
    session,
  );
}

export async function loadAffectedGroupDocs(groupIds, session) {
  if (groupIds.length === 0) return [];
  return withSession(
    GroupModel.find(
      { _id: { $in: groupIds } },
      { _id: 1, groupNumber: 1, groupName: 1, totalSavings: 1 },
    ).lean(),
    session,
  );
}

export async function loadAffectedSettingDocs(settingKeys, session) {
  if (settingKeys.length === 0) return [];
  return withSession(
    ContributionSettingModel.find(
      {
        $or: settingKeys.map((item) => ({
          userId: item.userId,
          groupId: item.groupId,
          year: item.year,
          contributionType: item.contributionType,
        })),
      },
      {
        _id: 1,
        userId: 1,
        groupId: 1,
        year: 1,
        contributionType: 1,
        expectedMonthlyAmount: 1,
        totalExpected: 1,
        totalActual: 1,
        outstandingBalance: 1,
      },
    ).lean(),
    session,
  );
}

export function applyDeltaToTotals(baseMap, deltaMap) {
  const next = new Map(baseMap);
  for (const [key, value] of deltaMap.entries()) {
    next.set(key, roundMoney(toNumber(next.get(key)) + value));
  }
  return next;
}

export function buildTransactionMetadataWithRepairMarker(
  transaction,
  {
    correctedAt,
    originalAmount,
    expectedAmount,
    source = "repairJanuaryContributionOverstatement",
    year,
    month,
  },
) {
  const metadata =
    transaction?.metadata && typeof transaction.metadata === "object"
      ? { ...transaction.metadata }
      : {};
  const repairs =
    metadata.repairs && typeof metadata.repairs === "object"
      ? { ...metadata.repairs }
      : {};

  repairs[REPAIR_KEY] = {
    correctedAt,
    source,
    year: Number(year),
    month: Number(month),
    originalAmount: roundMoney(originalAmount),
    expectedAmount: roundMoney(expectedAmount),
  };

  metadata.repairs = repairs;
  return metadata;
}

export function deriveSettingCycleLength(setting, fallback = 12) {
  const expectedMonthlyAmount = toNumber(setting?.expectedMonthlyAmount);
  const totalExpected = toNumber(setting?.totalExpected);
  if (!(expectedMonthlyAmount > 0) || !(totalExpected > 0)) {
    return fallback;
  }

  const rawLength = totalExpected / expectedMonthlyAmount;
  if (!(rawLength > 0)) return fallback;

  const roundedLength = Math.round(rawLength);
  if (roundedLength > 0 && nearlyEqual(rawLength, roundedLength, 0.05)) {
    return roundedLength;
  }

  return roundMoney(rawLength);
}

export {
  roundMoney,
  toId,
  toNumber,
  buildTupleKey,
  nearlyEqual,
};
