export const ContributionWindow = {
  startDay: 27,
  endDay: 4,
};

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
  new Set([...ContributionTypeCanonical, ...Object.keys(ContributionTypeAliases)]),
);

export const ContributionTypeConfig = {
  revolving: {
    key: "revolving",
    label: "Revolving Contribution",
    minAmount: 5000,
    unitAmount: 5000,
    allowedMonths: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    notes: "Uniform monthly contribution from January to October. Withdrawals are only allowed at October end.",
  },
  special: {
    key: "special",
    label: "Special Contribution",
    minAmount: 500000,
    allowedMonths: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    notes:
      "Voluntary bulk contributions from January to October. Withdrawable at month end; interest paid by October end.",
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
  if (!Number.isFinite(value) || value < Number(cfg.minAmount || 0)) return false;
  if (cfg.unitAmount) {
    return value % cfg.unitAmount === 0;
  }
  return true;
}
