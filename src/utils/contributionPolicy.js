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
    minAmount: 5000,
    unitAmount: 1000,
    stepAmount: 5000,
    notes:
      "Uniform monthly contribution with NGN 1,000 per unit. Minimum NGN 5,000 per month.",
  },
  special: {
    key: "special",
    label: "Special Contribution",
    minAmount: 5000,
    unitAmount: 1000,
    stepAmount: 5000,
    notes:
      "Flexible savings with NGN 1,000 per unit. Minimum NGN 5,000 per contribution.",
  },
  endwell: {
    key: "endwell",
    label: "Endwell Contribution",
    minAmount: 5000,
    notes:
      "Saved towards retirement for a minimum of five years. Notify the association one month before withdrawal.",
  },
  festive: {
    key: "festive",
    label: "Festive Contribution",
    minAmount: 2000,
    notes:
      "Contribution tied to a specific festival. Withdrawals are only for the intended festival.",
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
  if (!Number.isFinite(value) || value < Number(cfg.minAmount || 0))
    return false;
  const step = cfg.stepAmount || cfg.unitAmount;
  if (step) {
    return value % step === 0;
  }
  return true;
}

export function calculateContributionUnits(amount) {
  const value = Number(amount);
  if (!Number.isFinite(value) || value <= 0) return 0;
  return value / ContributionUnitBase;
}

export function calculateContributionInterest(amount) {
  const units = calculateContributionUnits(amount);
  if (!units) return 0;
  return Math.round(units * ContributionInterestPerUnit * 100) / 100;
}

export function isContributionInterestEligible(type) {
  const normalized = normalizeContributionType(type);
  if (normalized) return normalized === "revolving";
  if (type === null || typeof type === "undefined") return true;
  const raw = String(type || "").trim();
  if (!raw) return true;
  return false;
}

export function calculateContributionInterestForType(type, amount) {
  if (!isContributionInterestEligible(type)) return 0;
  return calculateContributionInterest(amount);
}
