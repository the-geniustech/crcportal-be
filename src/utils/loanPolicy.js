export const LoanFacilityTypes = ["revolving", "special", "bridging", "soft"];
export const LoanInterestRateTypes = ["annual", "monthly", "total"];

export const ContributionWindow = {
  startDay: 1,
  endDay: 31,
};

export const LoanRepaymentDeadlines = {
  generalMonth: 10,
  bridgingMonth: 1,
};

export const LoanFacilities = {
  revolving: {
    key: "revolving",
    label: "Revolving Loan",
    interestRate: 3.5,
    interestRateType: "monthly",
    maxAmountRule: "contributions",
  },
  special: {
    key: "special",
    label: "Special Loan",
    interestRate: 3.5,
    interestRateType: "monthly",
    qualificationRequired: true,
  },
  bridging: {
    key: "bridging",
    label: "Bridging Loan",
    interestRateMin: 4,
    interestRateMax: 10,
    interestRateType: "monthly",
    availabilityMonths: [10, 11, 12],
  },
  soft: {
    key: "soft",
    label: "Soft Loan",
    interestRate: 25,
    interestRateType: "total",
    availabilityMonths: [11, 12],
    termMonths: 10,
  },
};

export function getLoanFacility(type) {
  if (!type) return null;
  return LoanFacilities[String(type)] || null;
}

export function isLoanFacilityAvailable(type, date = new Date()) {
  const facility = getLoanFacility(type);
  if (!facility) return false;
  const months = facility.availabilityMonths || [];
  if (!months.length) return true;
  const nowMonth = date.getMonth() + 1;
  return months.includes(nowMonth);
}

export function getLoanInterestConfig(type) {
  const facility = getLoanFacility(type);
  if (!facility) {
    return {
      rateType: "annual",
      fixedRate: null,
      minRate: null,
      maxRate: null,
      termMonths: null,
    };
  }

  return {
    rateType: facility.interestRateType || "annual",
    fixedRate:
      typeof facility.interestRate === "number" ? facility.interestRate : null,
    minRate:
      typeof facility.interestRateMin === "number"
        ? facility.interestRateMin
        : null,
    maxRate:
      typeof facility.interestRateMax === "number"
        ? facility.interestRateMax
        : null,
    termMonths:
      typeof facility.termMonths === "number" ? facility.termMonths : null,
  };
}

export function isInterestRateAllowed(type, rate) {
  if (rate == null) return true;
  const cfg = getLoanInterestConfig(type);
  const num = Number(rate);
  if (!Number.isFinite(num)) return false;
  if (cfg.fixedRate != null) return num === cfg.fixedRate;
  if (cfg.minRate != null && num < cfg.minRate) return false;
  if (cfg.maxRate != null && num > cfg.maxRate) return false;
  return true;
}

export function resolveInterestRate(type, requestedRate) {
  const cfg = getLoanInterestConfig(type);
  if (cfg.fixedRate != null) {
    return { rate: cfg.fixedRate, rateType: cfg.rateType };
  }
  if (requestedRate != null && Number.isFinite(Number(requestedRate))) {
    return { rate: Number(requestedRate), rateType: cfg.rateType };
  }
  if (cfg.minRate != null) {
    return { rate: cfg.minRate, rateType: cfg.rateType };
  }
  return { rate: 0, rateType: cfg.rateType };
}

export function getLoanRepaymentDeadline(loanType, startDate) {
  if (!startDate) return null;
  const start = new Date(startDate);
  if (Number.isNaN(start.getTime())) return null;

  if (loanType === "bridging") {
    return new Date(start.getFullYear() + 1, 0, 31, 23, 59, 59, 999);
  }

  return new Date(start.getFullYear(), 9, 31, 23, 59, 59, 999);
}
