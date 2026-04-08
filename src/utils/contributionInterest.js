import { ContributionInterestSettingModel } from "../models/ContributionInterestSetting.js";

export const DEFAULT_INTEREST_RATE_PER_THOUSAND = 35;
export const INDIVIDUAL_INTEREST_SHARE = 0.69;

export const INTEREST_SHARING_CATEGORIES = [
  { key: "individuals", label: "Individuals", percentage: 69 },
  { key: "champions", label: "Champions", percentage: 20 },
  { key: "patronage", label: "Patronage", percentage: 3 },
  { key: "group_leaders", label: "Group Leaders", percentage: 2.5 },
  { key: "apex_coordinators", label: "Apex Coordinators", percentage: 2 },
  { key: "maintenance", label: "Maintenance", percentage: 2 },
  { key: "running_cost", label: "Running Cost", percentage: 1.5 },
];

export const MONTH_LABELS = [
  { value: 1, short: "Jan", long: "January" },
  { value: 2, short: "Feb", long: "February" },
  { value: 3, short: "Mar", long: "March" },
  { value: 4, short: "Apr", long: "April" },
  { value: 5, short: "May", long: "May" },
  { value: 6, short: "Jun", long: "June" },
  { value: 7, short: "Jul", long: "July" },
  { value: 8, short: "Aug", long: "August" },
  { value: 9, short: "Sep", long: "September" },
  { value: 10, short: "Oct", long: "October" },
  { value: 11, short: "Nov", long: "November" },
  { value: 12, short: "Dec", long: "December" },
];

const MAX_RATE_PER_THOUSAND = 1000;
const MAX_MONTHS = 12;

function normalizeMonthsToCompute(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return MAX_MONTHS;
  return Math.max(0, Math.min(MAX_MONTHS, Math.floor(num)));
}

export function resolveMonthsToCompute({ year, now = new Date(), capMonth = MAX_MONTHS } = {}) {
  const safeYear = Number(year);
  const cap = normalizeMonthsToCompute(capMonth);
  if (!Number.isFinite(safeYear)) return cap;
  const currentYear = now.getFullYear();
  if (safeYear < currentYear) return cap;
  if (safeYear > currentYear) return 0;
  const currentMonth = now.getMonth() + 1;
  return Math.min(currentMonth, cap);
}

export function roundMoney(value, decimals = 2) {
  const safe = Number(value);
  if (!Number.isFinite(safe)) return 0;
  const factor = 10 ** decimals;
  return Math.round(safe * factor) / factor;
}

export function buildDefaultMonthlyRates() {
  return MONTH_LABELS.reduce((acc, month) => {
    acc[month.value] = DEFAULT_INTEREST_RATE_PER_THOUSAND;
    return acc;
  }, {});
}

function normalizeRate(value) {
  const num = Number(value);
  if (!Number.isFinite(num) || num < 0) return null;
  return Math.min(num, MAX_RATE_PER_THOUSAND);
}

export function parseMonthlyRatesInput(input) {
  const updates = {};
  if (!input) return updates;

  if (input instanceof Map) {
    input.forEach((value, key) => {
      const month = Number(key);
      const rate = normalizeRate(value);
      if (!Number.isFinite(month) || month < 1 || month > 12) return;
      if (rate === null) return;
      updates[month] = rate;
    });
    return updates;
  }

  if (Array.isArray(input)) {
    input.forEach((entry) => {
      const month = Number(entry?.month ?? entry?.m ?? entry?.key);
      const rate = normalizeRate(entry?.ratePerThousand ?? entry?.rate ?? entry?.value);
      if (!Number.isFinite(month) || month < 1 || month > 12) return;
      if (rate === null) return;
      updates[month] = rate;
    });
    return updates;
  }

  if (typeof input === "object") {
    Object.entries(input).forEach(([key, value]) => {
      const month = Number(key);
      const rate = normalizeRate(value);
      if (!Number.isFinite(month) || month < 1 || month > 12) return;
      if (rate === null) return;
      updates[month] = rate;
    });
  }

  return updates;
}

export function buildMonthlyRatesResponse(monthlyRates) {
  const rates = monthlyRates || buildDefaultMonthlyRates();
  return MONTH_LABELS.map((month) => {
    const ratePerThousand = Number(rates?.[month.value] ?? DEFAULT_INTEREST_RATE_PER_THOUSAND);
    const safeRate = Number.isFinite(ratePerThousand)
      ? ratePerThousand
      : DEFAULT_INTEREST_RATE_PER_THOUSAND;
    return {
      month: month.value,
      label: month.long,
      shortLabel: month.short,
      ratePerThousand: safeRate,
      ratePct: roundMoney(safeRate / 10, 2),
    };
  });
}

export async function getMonthlyInterestRates(year) {
  if (!year) return buildDefaultMonthlyRates();
  const existing = await ContributionInterestSettingModel.findOne({ year }).lean();
  const defaults = buildDefaultMonthlyRates();
  if (!existing?.monthlyRates) return defaults;
  const updates = parseMonthlyRatesInput(existing.monthlyRates);
  return { ...defaults, ...updates };
}

export async function upsertMonthlyInterestRates({ year, rates, updatedBy }) {
  const defaults = buildDefaultMonthlyRates();
  const updates = parseMonthlyRatesInput(rates);
  const nextRates = { ...defaults, ...updates };

  const doc = await ContributionInterestSettingModel.findOneAndUpdate(
    { year },
    {
      $set: {
        monthlyRates: nextRates,
        updatedBy: updatedBy || null,
      },
    },
    { upsert: true, new: true, setDefaultsOnInsert: true },
  );

  return doc?.toObject ? doc.toObject() : doc;
}

export function computeAggregateInterestSchedule({
  monthlyContributions,
  monthlyRates,
  startingBalance = 0,
  monthsToCompute,
} = {}) {
  const limit = normalizeMonthsToCompute(
    Number.isFinite(Number(monthsToCompute)) ? monthsToCompute : MAX_MONTHS,
  );
  const contributions = Array.from({ length: MAX_MONTHS }, (_, idx) => {
    if (limit === 0 || idx >= limit) return 0;
    return Number(monthlyContributions?.[idx] ?? 0);
  });
  const rates = monthlyRates || buildDefaultMonthlyRates();
  let balance = Number(startingBalance ?? 0);
  if (!Number.isFinite(balance)) balance = 0;

  const schedule = MONTH_LABELS.map((month, idx) => {
    const ratePerThousand = Number(rates?.[month.value] ?? DEFAULT_INTEREST_RATE_PER_THOUSAND);
    const safeRate = Number.isFinite(ratePerThousand)
      ? ratePerThousand
      : DEFAULT_INTEREST_RATE_PER_THOUSAND;
    const shouldCompute = limit > 0 && idx < limit;
    const interest = shouldCompute ? roundMoney(balance * (safeRate / 1000)) : 0;
    const contribution = shouldCompute ? contributions[idx] : 0;
    const total = shouldCompute ? roundMoney(contribution + interest) : 0;
    if (shouldCompute) {
      balance = roundMoney(balance + total);
    }
    return {
      month: month.value,
      label: month.long,
      shortLabel: month.short,
      ratePerThousand: safeRate,
      ratePct: roundMoney(safeRate / 10, 2),
      contributions: contribution,
      interest,
      total,
      cumulativeTotal: balance,
    };
  });

  const totals = schedule.reduce(
    (acc, row) => {
      acc.contributions += Number(row.contributions || 0);
      acc.interest += Number(row.interest || 0);
      acc.total += Number(row.total || 0);
      return acc;
    },
    { contributions: 0, interest: 0, total: 0 },
  );

  return { schedule, totals, monthsComputed: limit };
}

export function computeInterestAllocation({
  contributionsByMember,
  monthlyRates,
  individualShare = INDIVIDUAL_INTEREST_SHARE,
  monthsToCompute,
} = {}) {
  const memberIds = Array.from(contributionsByMember?.keys?.() || []);
  const rates = monthlyRates || buildDefaultMonthlyRates();
  const limit = normalizeMonthsToCompute(
    Number.isFinite(Number(monthsToCompute)) ? monthsToCompute : MAX_MONTHS,
  );

  const memberBalances = new Map();
  const memberInterestByMonth = new Map();
  memberIds.forEach((id) => {
    memberBalances.set(id, 0);
    memberInterestByMonth.set(id, Array(12).fill(0));
  });

  const contributionsByMonthTotals = Array(MAX_MONTHS).fill(0);
  memberIds.forEach((id) => {
    const months = contributionsByMember.get(id) || [];
    for (let idx = 0; idx < MAX_MONTHS; idx += 1) {
      if (limit === 0 || idx >= limit) continue;
      contributionsByMonthTotals[idx] += Number(months[idx] ?? 0);
    }
  });

  const poolInterestByMonth = Array(MAX_MONTHS).fill(0);
  const poolBalanceByMonth = Array(MAX_MONTHS).fill(0);
  let poolBalance = 0;

  for (let idx = 0; idx < limit; idx += 1) {
    const month = idx + 1;
    const ratePerThousand = Number(rates?.[month] ?? DEFAULT_INTEREST_RATE_PER_THOUSAND);
    const safeRate = Number.isFinite(ratePerThousand)
      ? ratePerThousand
      : DEFAULT_INTEREST_RATE_PER_THOUSAND;
    const poolInterest = roundMoney(poolBalance * (safeRate / 1000));
    poolInterestByMonth[idx] = poolInterest;

    const totalMemberBalance = memberIds.reduce(
      (sum, id) => sum + Number(memberBalances.get(id) ?? 0),
      0,
    );
    const interestForMembers = roundMoney(poolInterest * individualShare);

    let allocated = 0;
    let maxBalance = -Infinity;
    let maxBalanceId = null;

    memberIds.forEach((id) => {
      const balance = Number(memberBalances.get(id) ?? 0);
      if (balance > maxBalance) {
        maxBalance = balance;
        maxBalanceId = id;
      }
      const ratio = totalMemberBalance > 0 ? balance / totalMemberBalance : 0;
      const rawInterest = interestForMembers * ratio;
      const rounded = roundMoney(rawInterest);
      const months = memberInterestByMonth.get(id);
      months[idx] = rounded;
      memberInterestByMonth.set(id, months);
      allocated += rounded;
    });

    const diff = roundMoney(interestForMembers - allocated);
    if (diff !== 0 && maxBalanceId) {
      const months = memberInterestByMonth.get(maxBalanceId);
      months[idx] = roundMoney(Number(months[idx] ?? 0) + diff);
      memberInterestByMonth.set(maxBalanceId, months);
    }

    memberIds.forEach((id) => {
      const contribution = Number(
        (contributionsByMember.get(id) || [])[idx] ?? 0,
      );
      const interest = Number((memberInterestByMonth.get(id) || [])[idx] ?? 0);
      const nextBalance = roundMoney(
        Number(memberBalances.get(id) ?? 0) + contribution + interest,
      );
      memberBalances.set(id, nextBalance);
    });

    const monthContribution = contributionsByMonthTotals[idx] ?? 0;
    poolBalance = roundMoney(poolBalance + monthContribution + poolInterest);
    poolBalanceByMonth[idx] = poolBalance;
  }

  if (limit < MAX_MONTHS) {
    for (let idx = limit; idx < MAX_MONTHS; idx += 1) {
      poolInterestByMonth[idx] = 0;
      poolBalanceByMonth[idx] = poolBalance;
    }
  }

  return {
    memberInterestByMonth,
    poolInterestByMonth,
    poolBalanceByMonth,
    contributionsByMonthTotals,
    monthsComputed: limit,
  };
}
