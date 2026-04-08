import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { ContributionModel } from "../models/Contribution.js";
import {
  buildMonthlyRatesResponse,
  computeAggregateInterestSchedule,
  getMonthlyInterestRates,
  INTEREST_SHARING_CATEGORIES,
  resolveMonthsToCompute,
  roundMoney,
  upsertMonthlyInterestRates,
} from "../utils/contributionInterest.js";
import {
  getContributionTypeMatch,
  normalizeContributionType,
  getContributionTypeConfig,
} from "../utils/contributionPolicy.js";
import {
  generateContributionIncomeSummaryPdfBuffer,
} from "../services/pdf/contributionIncomeSummaryPdf.js";
import {
  generateContributionInterestSharingPdfBuffer,
} from "../services/pdf/contributionInterestSharingPdf.js";

const PaidContributionStatuses = ["completed", "verified"];

const CSV_BOM = "\uFEFF";

function csvEscape(value) {
  const raw = String(value ?? "");
  if (/[",\n]/.test(raw)) {
    return `"${raw.replace(/"/g, '""')}"`;
  }
  return raw;
}

function buildCsv(rows) {
  return rows.map((row) => row.map((value) => csvEscape(value)).join(",")).join("\n");
}

function parseYear(value, fallback) {
  const year = Number(value ?? fallback);
  if (!Number.isFinite(year) || year < 2000 || year > 2100) return null;
  return year;
}

function parseContributionType(raw) {
  if (!raw) return null;
  const canonical = normalizeContributionType(raw);
  return canonical || null;
}

function formatContributionTypeLabel(contributionType) {
  if (!contributionType) return "All Contributions";
  const config = getContributionTypeConfig(contributionType);
  return config?.label || contributionType;
}

async function getContributionTotalsByMonth({ year, contributionType } = {}) {
  const match = {
    year,
    status: { $in: PaidContributionStatuses },
  };
  if (contributionType) {
    const matchTypes = getContributionTypeMatch(contributionType) || [contributionType];
    if (contributionType === "revolving") {
      match.$or = [
        { contributionType: { $in: matchTypes } },
        { contributionType: { $exists: false } },
        { contributionType: null },
      ];
    } else {
      match.contributionType = { $in: matchTypes };
    }
  }

  const rows = await ContributionModel.aggregate([
    { $match: match },
    { $group: { _id: "$month", total: { $sum: "$amount" } } },
  ]);

  const totals = Array(12).fill(0);
  rows.forEach((row) => {
    const month = Number(row._id);
    if (!Number.isFinite(month) || month < 1 || month > 12) return;
    totals[month - 1] = Number(row.total || 0);
  });

  return totals;
}

export const getContributionInterestSettings = catchAsync(async (req, res, next) => {
  const now = new Date();
  const year = parseYear(req.query?.year, now.getFullYear());
  if (!year) return next(new AppError("Invalid year", 400));

  const monthlyRates = await getMonthlyInterestRates(year);
  const rates = buildMonthlyRatesResponse(monthlyRates);

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      year,
      rates,
    },
  });
});

export const updateContributionInterestSettings = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const now = new Date();
  const year = parseYear(req.body?.year ?? req.query?.year, now.getFullYear());
  if (!year) return next(new AppError("Invalid year", 400));

  const payloadRates =
    req.body?.rates ?? req.body?.monthlyRates ?? req.body?.interestRates ?? {};

  const updated = await upsertMonthlyInterestRates({
    year,
    rates: payloadRates,
    updatedBy: req.user.profileId ?? null,
  });

  const monthlyRates = await getMonthlyInterestRates(year);
  const rates = buildMonthlyRatesResponse(monthlyRates);

  return sendSuccess(res, {
    statusCode: 200,
    message: "Interest settings updated",
    data: {
      year,
      rates,
      updatedAt: updated?.updatedAt ?? null,
      updatedBy: updated?.updatedBy ?? null,
    },
  });
});

export const getContributionIncomeSummary = catchAsync(async (req, res, next) => {
  const now = new Date();
  const year = parseYear(req.query?.year, now.getFullYear());
  if (!year) return next(new AppError("Invalid year", 400));

  const contributionType = parseContributionType(req.query?.contributionType);
  if (req.query?.contributionType && !contributionType) {
    return next(new AppError("Invalid contributionType", 400));
  }

  const monthlyContributions = await getContributionTotalsByMonth({
    year,
    contributionType,
  });
  const monthlyRates = await getMonthlyInterestRates(year);
  const monthsToCompute = resolveMonthsToCompute({ year, now });
  const { schedule, totals } = computeAggregateInterestSchedule({
    monthlyContributions,
    monthlyRates,
    monthsToCompute,
  });

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      year,
      contributionType: contributionType || null,
      monthsComputed: monthsToCompute,
      months: schedule,
      totals: {
        contributions: roundMoney(totals.contributions),
        interest: roundMoney(totals.interest),
        total: roundMoney(totals.total),
      },
    },
  });
});

export const getContributionInterestSharing = catchAsync(async (req, res, next) => {
  const now = new Date();
  const year = parseYear(req.query?.year, now.getFullYear());
  if (!year) return next(new AppError("Invalid year", 400));

  const contributionType = parseContributionType(req.query?.contributionType);
  if (req.query?.contributionType && !contributionType) {
    return next(new AppError("Invalid contributionType", 400));
  }

  const monthlyContributions = await getContributionTotalsByMonth({
    year,
    contributionType,
  });
  const monthlyRates = await getMonthlyInterestRates(year);
  const monthsToCompute = resolveMonthsToCompute({ year, now });
  const { totals } = computeAggregateInterestSchedule({
    monthlyContributions,
    monthlyRates,
    monthsToCompute,
  });

  const totalInterest = roundMoney(totals.interest);
  const categories = INTEREST_SHARING_CATEGORIES.map((category) => {
    const amount = roundMoney((totalInterest * category.percentage) / 100);
    return {
      key: category.key,
      label: category.label,
      percentage: category.percentage,
      amount,
      amountShared: amount,
    };
  });

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      year,
      contributionType: contributionType || null,
      monthsComputed: monthsToCompute,
      totalInterest,
      categories,
    },
  });
});

export const exportContributionIncomeSummary = catchAsync(async (req, res, next) => {
  const now = new Date();
  const year = parseYear(req.query?.year, now.getFullYear());
  if (!year) return next(new AppError("Invalid year", 400));

  const contributionType = parseContributionType(req.query?.contributionType);
  if (req.query?.contributionType && !contributionType) {
    return next(new AppError("Invalid contributionType", 400));
  }

  const format = String(req.query?.format ?? "pdf").trim().toLowerCase();
  if (format !== "csv" && format !== "pdf") {
    return next(new AppError("Invalid format. Use csv or pdf.", 400));
  }

  const monthlyContributions = await getContributionTotalsByMonth({
    year,
    contributionType,
  });
  const monthlyRates = await getMonthlyInterestRates(year);
  const monthsToCompute = resolveMonthsToCompute({ year, now });
  const { schedule, totals } = computeAggregateInterestSchedule({
    monthlyContributions,
    monthlyRates,
    monthsToCompute,
  });

  const contributionTypeLabel = formatContributionTypeLabel(contributionType);
  const filenameBase = `summary-income-${year}-${contributionType || "all"}`;

  if (format === "csv") {
    const rows = [
      [
        "Month",
        "Monthly Contributions",
        "Interest",
        "Total",
        "Cumulative Total",
      ],
      ...schedule.map((row) => [
        row.label,
        roundMoney(row.contributions),
        roundMoney(row.interest),
        roundMoney(row.total),
        roundMoney(row.cumulativeTotal),
      ]),
    ];

    const lastCumulative =
      schedule[Math.max(0, monthsToCompute - 1)]?.cumulativeTotal ??
      schedule[schedule.length - 1]?.cumulativeTotal ??
      0;

    rows.push([
      "Totals",
      roundMoney(totals.contributions),
      roundMoney(totals.interest),
      roundMoney(totals.total),
      roundMoney(lastCumulative),
    ]);

    const csv = CSV_BOM + buildCsv(rows);
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${filenameBase}.csv"`,
    );
    return res.status(200).send(csv);
  }

  const pdfBuffer = await generateContributionIncomeSummaryPdfBuffer({
    year,
    contributionTypeLabel,
    generatedAt: now,
    totals: {
      contributions: roundMoney(totals.contributions),
      interest: roundMoney(totals.interest),
      total: roundMoney(totals.total),
    },
    schedule,
    monthsComputed: monthsToCompute,
  });

  res.setHeader("Content-Type", "application/pdf");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="${filenameBase}.pdf"`,
  );
  return res.status(200).send(pdfBuffer);
});

export const exportContributionInterestSharing = catchAsync(async (req, res, next) => {
  const now = new Date();
  const year = parseYear(req.query?.year, now.getFullYear());
  if (!year) return next(new AppError("Invalid year", 400));

  const contributionType = parseContributionType(req.query?.contributionType);
  if (req.query?.contributionType && !contributionType) {
    return next(new AppError("Invalid contributionType", 400));
  }

  const format = String(req.query?.format ?? "pdf").trim().toLowerCase();
  if (format !== "csv" && format !== "pdf") {
    return next(new AppError("Invalid format. Use csv or pdf.", 400));
  }

  const monthlyContributions = await getContributionTotalsByMonth({
    year,
    contributionType,
  });
  const monthlyRates = await getMonthlyInterestRates(year);
  const monthsToCompute = resolveMonthsToCompute({ year, now });
  const { totals } = computeAggregateInterestSchedule({
    monthlyContributions,
    monthlyRates,
    monthsToCompute,
  });

  const totalInterest = roundMoney(totals.interest);
  const categories = INTEREST_SHARING_CATEGORIES.map((category) => {
    const amount = roundMoney((totalInterest * category.percentage) / 100);
    return {
      key: category.key,
      label: category.label,
      percentage: category.percentage,
      amount,
      amountShared: amount,
    };
  });

  const contributionTypeLabel = formatContributionTypeLabel(contributionType);
  const filenameBase = `interest-sharing-${year}-${contributionType || "all"}`;

  if (format === "csv") {
    const rows = [
      ["Category", "Percentage", "Amount", "Amount Shared"],
      ...categories.map((category) => [
        category.label,
        category.percentage,
        category.amount,
        category.amountShared,
      ]),
      ["Totals", 100, totalInterest, totalInterest],
    ];

    const csv = CSV_BOM + buildCsv(rows);
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${filenameBase}.csv"`,
    );
    return res.status(200).send(csv);
  }

  const pdfBuffer = await generateContributionInterestSharingPdfBuffer({
    year,
    contributionTypeLabel,
    generatedAt: now,
    totalInterest,
    categories,
    monthsComputed: monthsToCompute,
  });

  res.setHeader("Content-Type", "application/pdf");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="${filenameBase}.pdf"`,
  );
  return res.status(200).send(pdfBuffer);
});
