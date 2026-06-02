export const ContributionWindow = {
  startDay: 1,
  endDay: 31,
};

export const ContributionSettingsWindow = {
  startMonth: 1,
  endMonth: 5,
};

export const ContributionUnitBase = 1000;
export const ContributionInterestPerUnit = 35;

export const ContributionTypeAliases = {
  regular: "revolving",
  special_savings: "special",
  end_well: "endwell",
  festival: "festive",
};

export const ContributionTypeCanonical = [
  "revolving",
  "special",
  "endwell",
  "festive",
];

export const ContributionTypes = Array.from(
  new Set([
    ...ContributionTypeCanonical,
    ...Object.keys(ContributionTypeAliases),
  ]),
);

export const ContributionTypeConfig = {
  revolving: {
    key: "revolving",
    label: "Revolving Contribution",
    minAmount: ContributionUnitBase,
    unitAmount: 1000,
    stepAmount: ContributionUnitBase,
    notes:
      "Uniform monthly contribution with NGN 1,000 per unit. Amounts must be positive multiples of NGN 1,000.",
  },
  special: {
    key: "special",
    label: "Special Contribution",
    minAmount: ContributionUnitBase,
    unitAmount: 1000,
    stepAmount: ContributionUnitBase,
    notes:
      "Flexible savings with NGN 1,000 per unit. Amounts must be positive multiples of NGN 1,000.",
  },
  endwell: {
    key: "endwell",
    label: "Endwell Contribution",
    minAmount: ContributionUnitBase,
    unitAmount: 1000,
    stepAmount: ContributionUnitBase,
    notes:
      "Saved towards retirement for a minimum of five years. Amounts must be positive multiples of NGN 1,000.",
  },
  festive: {
    key: "festive",
    label: "Festive Contribution",
    minAmount: ContributionUnitBase,
    unitAmount: 1000,
    stepAmount: ContributionUnitBase,
    notes:
      "Contribution tied to a specific festival. Amounts must be positive multiples of NGN 1,000.",
  },
};

export function normalizeContributionType(type) {
  if (!type) return null;
  const value = String(type).trim().toLowerCase();
  if (!value) return null;
  if (ContributionTypeAliases[value]) return ContributionTypeAliases[value];
  if (ContributionTypeCanonical.includes(value)) return value;
  return null;
}

export function getContributionTypeConfig(type) {
  const canonical = normalizeContributionType(type);
  if (!canonical) return null;
  return ContributionTypeConfig[canonical] || null;
}

export function getContributionTypeMatch(type) {
  const canonical = normalizeContributionType(type);
  if (!canonical) return null;
  const legacy = Object.entries(ContributionTypeAliases)
    .filter(([, mapped]) => mapped === canonical)
    .map(([legacyKey]) => legacyKey);
  return Array.from(new Set([canonical, ...legacy]));
}

export function resolvePlannedContributionUnits(settings, year, type = "revolving") {
  if (!settings || !year) return null;
  const settingsYear = Number(settings?.year);
  if (!Number.isFinite(settingsYear) || settingsYear !== year) return null;
  const rawUnits = settings?.units;
  if (typeof rawUnits === "number" || typeof rawUnits === "string") {
    const num = Number(rawUnits);
    return Number.isInteger(num) && num > 0 ? num : null;
  }
  if (!rawUnits || typeof rawUnits !== "object") return null;
  const num = Number(rawUnits?.[type]);
  return Number.isInteger(num) && num > 0 ? num : null;
}

export function resolveExpectedContributionAmount({
  settings,
  year,
  groupMonthlyContribution,
  type = "revolving",
} = {}) {
  const config = getContributionTypeConfig(type);
  const unitAmount = Number(config?.unitAmount ?? ContributionUnitBase);
  const minAmount = Number(config?.minAmount ?? 0);
  const base = Number(groupMonthlyContribution ?? 0);
  const fallbackBaseline = Math.max(minAmount, Number.isFinite(base) ? base : 0);

  const plannedUnits = resolvePlannedContributionUnits(settings, year, type);
  if (plannedUnits && Number.isFinite(unitAmount) && unitAmount > 0) {
    const computed = plannedUnits * unitAmount;
    return Math.max(computed, minAmount);
  }

  return fallbackBaseline > 0 ? fallbackBaseline : 0;
}

export function isContributionWindowOpen(date = new Date()) {
  const day = date.getDate();
  return day >= ContributionWindow.startDay || day <= ContributionWindow.endDay;
}

export function getContributionWindowStatus(date = new Date()) {
  return {
    startDay: ContributionWindow.startDay,
    endDay: ContributionWindow.endDay,
    isOpen: isContributionWindowOpen(date),
  };
}

export function isContributionSettingsWindowOpen(date = new Date()) {
  const month = date.getMonth() + 1;
  return (
    month >= ContributionSettingsWindow.startMonth &&
    month <= ContributionSettingsWindow.endMonth
  );
}

export function getContributionSettingsWindowStatus(date = new Date()) {
  return {
    startMonth: ContributionSettingsWindow.startMonth,
    endMonth: ContributionSettingsWindow.endMonth,
    isOpen: isContributionSettingsWindowOpen(date),
  };
}

export function isContributionMonthAllowed(type, month) {
  const cfg = getContributionTypeConfig(type);
  if (!cfg) return false;
  const allowed = cfg.allowedMonths;
  if (!Array.isArray(allowed) || allowed.length === 0) return true;
  const m = Number(month);
  return Number.isFinite(m) && allowed.includes(m);
}

export function isContributionAmountValid(type, amount) {
  const cfg = getContributionTypeConfig(type);
  if (!cfg) return false;
  const value = Number(amount);
  const step = Number(cfg.stepAmount || cfg.unitAmount || ContributionUnitBase);
  if (!Number.isFinite(value) || value <= 0) return false;
  if (!Number.isFinite(step) || step <= 0) return false;
  return value >= Number(cfg.minAmount || ContributionUnitBase) && value % step === 0;
}

export function calculateContributionUnits(amount) {
  const value = Number(amount);
  if (!Number.isFinite(value) || value <= 0) return 0;
  return value / ContributionUnitBase;
}

export function calculateContributionInterest(amount) {
  // Interest on contribution is now computed on cumulative balances
  // using monthly interest settings; keep legacy helper returning 0
  // to avoid applying the old per-unit logic.
  return 0;
}

export function isContributionInterestEligible(type) {
  return true;
}

export function calculateContributionInterestForType(type, amount) {
  return 0;
}
