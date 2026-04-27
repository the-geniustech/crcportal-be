import {
  buildDefaultMonthlyRates,
  computeAggregateInterestSchedule,
  resolveMonthsToCompute,
  roundMoney,
} from "../../../utils/contributionInterest.js";

const REPAIR_KEY = "januaryContributionHalved";
const COUNTED_CONTRIBUTION_STATUSES = new Set(["verified", "completed"]);

const asArray = (value) => (Array.isArray(value) ? value : []);

const toNumber = (value, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const toId = (value) => String(value ?? "").trim();

const buildTupleKey = (...parts) => parts.map((part) => toId(part)).join("|");

const isCountedContribution = (contribution) =>
  COUNTED_CONTRIBUTION_STATUSES.has(
    String(contribution?.status || "").trim().toLowerCase(),
  );

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

function deriveSettingCycleLength(setting, fallback = 12) {
  const expectedMonthlyAmount = toNumber(setting?.expectedMonthlyAmount);
  const totalExpected = toNumber(setting?.totalExpected);
  if (!(expectedMonthlyAmount > 0) || !(totalExpected > 0)) {
    return fallback;
  }

  const rawLength = totalExpected / expectedMonthlyAmount;
  if (!(rawLength > 0)) return fallback;

  const roundedLength = Math.round(rawLength);
  if (
    roundedLength > 0 &&
    Math.abs(rawLength - roundedLength) <= 0.05
  ) {
    return roundedLength;
  }

  return roundMoney(rawLength);
}

export function hasJanuaryContributionRepairMarker(meta, year, month = 1) {
  const repair = meta?.repairs?.[REPAIR_KEY]?.[String(year)];
  if (!repair) return false;
  return Number(repair.month ?? month) === Number(month);
}

function withRepairMarker(meta, { year, month = 1, correctedAt, source }) {
  const nextMeta = meta && typeof meta === "object" ? { ...meta } : {};
  const repairs =
    nextMeta.repairs && typeof nextMeta.repairs === "object"
      ? { ...nextMeta.repairs }
      : {};
  const repairGroup =
    repairs[REPAIR_KEY] && typeof repairs[REPAIR_KEY] === "object"
      ? { ...repairs[REPAIR_KEY] }
      : {};

  repairGroup[String(year)] = {
    month,
    correctedAt,
    source: source || "january-contribution-repair",
  };

  repairs[REPAIR_KEY] = repairGroup;
  nextMeta.repairs = repairs;
  return nextMeta;
}

function recomputeMembershipTotals(groupMembers, contributions) {
  const totals = new Map();
  asArray(contributions).forEach((contribution) => {
    if (!isCountedContribution(contribution)) return;
    const key = buildTupleKey(contribution.userId, contribution.groupId);
    totals.set(key, roundMoney(toNumber(totals.get(key)) + toNumber(contribution.amount)));
  });

  return asArray(groupMembers).map((membership) => {
    const key = buildTupleKey(membership.userId, membership.groupId);
    return {
      ...membership,
      totalContributed: roundMoney(toNumber(totals.get(key))),
    };
  });
}

function recomputeGroupTotals(groups, contributions) {
  const totals = new Map();
  asArray(contributions).forEach((contribution) => {
    if (!isCountedContribution(contribution)) return;
    const key = toId(contribution.groupId);
    totals.set(key, roundMoney(toNumber(totals.get(key)) + toNumber(contribution.amount)));
  });

  return asArray(groups).map((group) => ({
    ...group,
    totalSavings: roundMoney(toNumber(totals.get(toId(group._id)))),
  }));
}

function buildGroupUnitRateMedianByGroupType(
  contributions,
  { year, month = 1 } = {},
) {
  const groupedValues = new Map();

  asArray(contributions).forEach((contribution) => {
    if (!isCountedContribution(contribution)) return;
    if (Number(contribution.year) !== Number(year)) return;
    if (Number(contribution.month) === Number(month)) return;

    const amount = toNumber(contribution.amount);
    const units = toNumber(contribution.units);
    if (!(amount > 0) || !(units > 0)) return;

    const key = buildTupleKey(
      contribution.groupId,
      contribution.contributionType || "revolving",
    );
    const current = groupedValues.get(key) || [];
    current.push(roundMoney(amount / units));
    groupedValues.set(key, current);
  });

  return new Map(
    Array.from(groupedValues.entries()).map(([key, values]) => [
      key,
      median(values),
    ]),
  );
}

function recomputeContributionSettings(
  contributionSettings,
  contributions,
  { year, month = 1 } = {},
) {
  const totals = new Map();
  asArray(contributions).forEach((contribution) => {
    if (!isCountedContribution(contribution)) return;
    const key = buildTupleKey(
      contribution.userId,
      contribution.groupId,
      contribution.year,
      contribution.contributionType || "revolving",
    );
    totals.set(key, roundMoney(toNumber(totals.get(key)) + toNumber(contribution.amount)));
  });

  const groupUnitRateMedianByGroupType = buildGroupUnitRateMedianByGroupType(
    contributions,
    { year, month },
  );

  return asArray(contributionSettings).map((setting) => {
    const key = buildTupleKey(
      setting.userId,
      setting.groupId,
      setting.year,
      setting.contributionType || "revolving",
    );
    const totalActual = roundMoney(toNumber(totals.get(key)));
    const units = toNumber(setting.units);
    const groupRateKey = buildTupleKey(
      setting.groupId,
      setting.contributionType || "revolving",
    );
    const groupUnitRate = toNumber(
      groupUnitRateMedianByGroupType.get(groupRateKey),
      null,
    );
    const expectedMonthlyAmount =
      groupUnitRate > 0 && units > 0
        ? roundMoney(groupUnitRate * units)
        : roundMoney(toNumber(setting.expectedMonthlyAmount));
    const cycleLength = deriveSettingCycleLength(setting, 12);
    const totalExpected = roundMoney(expectedMonthlyAmount * cycleLength);
    const outstandingBalance = roundMoney(
      Math.max(totalExpected - totalActual, 0),
    );

    return {
      ...setting,
      expectedMonthlyAmount,
      totalExpected,
      totalActual,
      outstandingBalance,
    };
  });
}

function recomputeInterestSummary(meta, contributions, year, now = new Date()) {
  if (!meta || typeof meta !== "object") return meta;

  const monthlyTotals = Array(12).fill(0);
  asArray(contributions).forEach((contribution) => {
    if (!isCountedContribution(contribution)) return;
    if (Number(contribution.year) !== Number(year)) return;
    const month = Number(contribution.month);
    if (!Number.isFinite(month) || month < 1 || month > 12) return;
    monthlyTotals[month - 1] = roundMoney(
      monthlyTotals[month - 1] + toNumber(contribution.amount),
    );
  });

  const monthsToCompute = resolveMonthsToCompute({ year, now });
  const interestSchedule = computeAggregateInterestSchedule({
    monthlyContributions: monthlyTotals,
    monthlyRates: buildDefaultMonthlyRates(),
    monthsToCompute,
  });

  return {
    ...meta,
    interestSummary: {
      monthsComputed: interestSchedule.monthsComputed ?? monthsToCompute,
      totals: interestSchedule.totals,
      schedule: interestSchedule.schedule,
    },
  };
}

function countChangedItems(beforeItems, afterItems, selector) {
  const beforeMap = new Map(
    asArray(beforeItems).map((item) => [selector(item), JSON.stringify(item)]),
  );
  return asArray(afterItems).reduce((count, item) => {
    const key = selector(item);
    return beforeMap.get(key) === JSON.stringify(item) ? count : count + 1;
  }, 0);
}

export function applyJanuaryContributionRepair(
  bundle,
  {
    year,
    month = 1,
    halveJanuary = true,
    markRepaired = false,
    correctedAt = new Date().toISOString(),
    source = "january-contribution-repair",
    now = new Date(),
  } = {},
) {
  const baseBundle = bundle && typeof bundle === "object" ? bundle : {};
  const contributionsBefore = asArray(baseBundle.contributions);
  const transactionsBefore = asArray(baseBundle.transactions);
  const groupMembersBefore = asArray(baseBundle.groupMembers);
  const groupsBefore = asArray(baseBundle.groups);
  const contributionSettingsBefore = asArray(baseBundle.contributionSettings);
  const metaBefore =
    baseBundle.meta && typeof baseBundle.meta === "object"
      ? { ...baseBundle.meta }
      : {};

  let januaryMatched = 0;
  let januaryAmountBefore = 0;
  let januaryAmountAfter = 0;

  const contributions = contributionsBefore.map((contribution) => {
    const nextContribution = { ...contribution };
    const isTargetJanuary =
      Number(contribution?.year) === Number(year) &&
      Number(contribution?.month) === Number(month);

    if (isTargetJanuary) {
      januaryMatched += 1;
      januaryAmountBefore = roundMoney(
        januaryAmountBefore + toNumber(contribution.amount),
      );
      if (halveJanuary) {
        nextContribution.amount = roundMoney(toNumber(contribution.amount) / 2);
      }
      januaryAmountAfter = roundMoney(
        januaryAmountAfter + toNumber(nextContribution.amount),
      );
    }

    return nextContribution;
  });

  const contributionById = new Map(
    contributions.map((contribution) => [toId(contribution._id), contribution]),
  );

  const transactions = transactionsBefore.map((transaction) => {
    const nextTransaction = { ...transaction };
    const metadata =
      nextTransaction.metadata && typeof nextTransaction.metadata === "object"
        ? { ...nextTransaction.metadata }
        : {};
    const directContributionId = toId(metadata.contributionId);
    const relatedContribution =
      (directContributionId && contributionById.get(directContributionId)) || null;

    if (
      nextTransaction.type === "group_contribution" &&
      relatedContribution &&
      Number(relatedContribution.year) === Number(year) &&
      Number(relatedContribution.month) === Number(month)
    ) {
      nextTransaction.amount = roundMoney(toNumber(relatedContribution.amount));
    }

    nextTransaction.metadata = metadata;
    return nextTransaction;
  });

  const groupMembers = recomputeMembershipTotals(groupMembersBefore, contributions);
  const groups = recomputeGroupTotals(groupsBefore, contributions);
  const contributionSettings = recomputeContributionSettings(
    contributionSettingsBefore,
    contributions,
    { year, month },
  );

  let meta = recomputeInterestSummary(metaBefore, contributions, year, now);
  if (markRepaired) {
    meta = withRepairMarker(meta, {
      year,
      month,
      correctedAt,
      source,
    });
  }

  const result = {
    ...baseBundle,
    contributions,
    transactions,
    groupMembers,
    groups,
    contributionSettings,
    meta,
  };

  return {
    bundle: result,
    summary: {
      year: Number(year),
      month: Number(month),
      januaryMatched,
      januaryAmountBefore: roundMoney(januaryAmountBefore),
      januaryAmountAfter: roundMoney(januaryAmountAfter),
      januaryAmountDelta: roundMoney(januaryAmountAfter - januaryAmountBefore),
      changedContributions: countChangedItems(
        contributionsBefore,
        contributions,
        (item) => toId(item._id),
      ),
      changedTransactions: countChangedItems(
        transactionsBefore,
        transactions,
        (item) => toId(item._id || item.reference),
      ),
      changedGroupMembers: countChangedItems(
        groupMembersBefore,
        groupMembers,
        (item) => toId(item._id || buildTupleKey(item.userId, item.groupId)),
      ),
      changedGroups: countChangedItems(
        groupsBefore,
        groups,
        (item) => toId(item._id || item.groupNumber),
      ),
      changedContributionSettings: countChangedItems(
        contributionSettingsBefore,
        contributionSettings,
        (item) =>
          toId(
            item._id ||
              buildTupleKey(
                item.userId,
                item.groupId,
                item.year,
                item.contributionType,
              ),
          ),
      ),
      changedMeta: JSON.stringify(metaBefore) === JSON.stringify(meta) ? 0 : 1,
    },
  };
}
