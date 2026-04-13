import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { ContributionModel } from "../models/Contribution.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import { TransactionModel } from "../models/Transaction.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { MeetingAttendanceModel } from "../models/MeetingAttendance.js";
import { buildCreditScoreData } from "./creditScoreController.js";
import {
  generateMemberFinancialReportPdfBuffer,
  formatCurrency as formatCurrencyPdf,
  formatDate as formatDatePdf,
} from "../services/pdf/memberFinancialReportPdf.js";

function formatNaira(amount) {
  return formatCurrencyPdf(amount);
}

function buildPeriodLabel(start, end, fallbackLabel) {
  if (fallbackLabel) return fallbackLabel;
  if (start && end) {
    return `${formatDatePdf(start)} - ${formatDatePdf(end)}`;
  }
  if (start) return `From ${formatDatePdf(start)}`;
  if (end) return `Up to ${formatDatePdf(end)}`;
  return "All Time";
}

function resolvePeriod(period) {
  if (!period || String(period).toLowerCase() === "all time") {
    return { label: "All Time", start: null, end: null };
  }

  if (typeof period === "string") {
    const value = String(period).trim();
    if (/^\d{4}$/.test(value)) {
      const year = Number(value);
      return {
        label: value,
        start: new Date(Date.UTC(year, 0, 1, 0, 0, 0)),
        end: new Date(Date.UTC(year, 11, 31, 23, 59, 59, 999)),
        year,
      };
    }
    if (/^\d{4}-\d{2}$/.test(value)) {
      const [y, m] = value.split("-").map(Number);
      if (m >= 1 && m <= 12) {
        return {
          label: value,
          start: new Date(Date.UTC(y, m - 1, 1, 0, 0, 0)),
          end: new Date(Date.UTC(y, m, 0, 23, 59, 59, 999)),
          year: y,
          month: m,
        };
      }
    }
    return null;
  }

  if (typeof period === "object") {
    const type = String(period?.type || period?.rangeType || "").toLowerCase();
    const year = Number(period?.year);
    if (type === "year" && Number.isFinite(year)) {
      return {
        label: String(year),
        start: new Date(Date.UTC(year, 0, 1, 0, 0, 0)),
        end: new Date(Date.UTC(year, 11, 31, 23, 59, 59, 999)),
        year,
      };
    }
    if ((type === "5years" || type === "5_years") && Number.isFinite(year)) {
      const startYear = year - 4;
      return {
        label: `${startYear}-${year}`,
        start: new Date(Date.UTC(startYear, 0, 1, 0, 0, 0)),
        end: new Date(Date.UTC(year, 11, 31, 23, 59, 59, 999)),
      };
    }
    if ((type === "10years" || type === "10_years") && Number.isFinite(year)) {
      const startYear = year - 9;
      return {
        label: `${startYear}-${year}`,
        start: new Date(Date.UTC(startYear, 0, 1, 0, 0, 0)),
        end: new Date(Date.UTC(year, 11, 31, 23, 59, 59, 999)),
      };
    }
    if (type === "custom") {
      const start = period?.startDate
        ? new Date(String(period.startDate))
        : null;
      const end = period?.endDate ? new Date(String(period.endDate)) : null;
      if (
        !start ||
        !end ||
        Number.isNaN(start.getTime()) ||
        Number.isNaN(end.getTime())
      ) {
        return null;
      }
      if (start.getTime() > end.getTime()) return null;
      return {
        label: buildPeriodLabel(start, end),
        start,
        end,
      };
    }
  }

  return null;
}

export const generateMyFinancialReport = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const type = String(req.body?.type || "").trim();
  const period = req.body?.period;
  const memberName = String(req.body?.memberName || "Member");

  const allowed = [
    "contribution_statement",
    "loan_history",
    "credit_score_report",
    "annual_summary",
  ];
  if (!allowed.includes(type)) {
    return next(
      new AppError(`Invalid report type. Allowed: ${allowed.join(", ")}`, 400),
    );
  }

  const resolved = resolvePeriod(period);
  if (!resolved) {
    return next(new AppError("Invalid period selection", 400));
  }
  const periodLabel = buildPeriodLabel(
    resolved.start,
    resolved.end,
    resolved.label,
  );

  let reportTitle = "Financial Report";
  let summaryItems = [];
  let sections = [];

  if (type === "contribution_statement") {
    reportTitle = "Contribution Statement";
    const filter = { userId: req.user.profileId };
    if (resolved?.start && resolved?.end) {
      filter.createdAt = { $gte: resolved.start, $lte: resolved.end };
    }

    const contributions = await ContributionModel.find(filter)
      .sort({ createdAt: -1 })
      .populate("groupId", "groupName")
      .lean();

    const totalAmount = contributions.reduce(
      (sum, c) => sum + Number(c.amount || 0),
      0,
    );
    const uniqueGroups = new Set(
      contributions.map((c) => c.groupId?.groupName).filter(Boolean),
    );
    const avgContribution =
      contributions.length > 0 ? totalAmount / contributions.length : 0;

    summaryItems = [
      { label: "Total Contributions", value: formatNaira(totalAmount) },
      { label: "Contribution Count", value: `${contributions.length}` },
      { label: "Avg Contribution", value: formatNaira(avgContribution) },
      { label: "Groups Covered", value: `${uniqueGroups.size}` },
    ];

    sections = [
      {
        type: "table",
        title: "Contribution Activity",
        columns: [
          { key: "date", label: "Date", width: 0.18 },
          { key: "type", label: "Type", width: 0.2 },
          { key: "group", label: "Group", width: 0.34 },
          { key: "amount", label: "Amount", width: 0.28, align: "right" },
        ],
        rows: contributions.map((c) => ({
          date: formatDatePdf(c.createdAt || new Date()),
          type: c.contributionType || "-",
          group: c.groupId?.groupName || "-",
          amount: formatNaira(c.amount),
        })),
      },
    ];
  }

  if (type === "loan_history") {
    reportTitle = "Loan Repayment History";
    const loanFilter = { userId: req.user.profileId };
    if (resolved?.start && resolved?.end) {
      loanFilter.createdAt = { $gte: resolved.start, $lte: resolved.end };
    }

    const loans = await LoanApplicationModel.find(loanFilter)
      .sort({ createdAt: -1 })
      .lean();

    const loanIds = loans.map((l) => l._id);

    const repayments = await TransactionModel.find({
      userId: req.user.profileId,
      type: "loan_repayment",
      ...(resolved?.start && resolved?.end
        ? { date: { $gte: resolved.start, $lte: resolved.end } }
        : {}),
      ...(loanIds.length > 0 ? { loanId: { $in: loanIds } } : {}),
    })
      .sort({ date: -1 })
      .lean();

    const totalBorrowed = loans.reduce(
      (sum, l) => sum + Number(l.loanAmount || 0),
      0,
    );
    const totalRepaid = repayments.reduce(
      (sum, r) => sum + Number(r.amount || 0),
      0,
    );

    summaryItems = [
      { label: "Loans Taken", value: `${loans.length}` },
      { label: "Total Borrowed", value: formatNaira(totalBorrowed) },
      { label: "Total Repaid", value: formatNaira(totalRepaid) },
      {
        label: "Outstanding Balance",
        value: formatNaira(Math.max(0, totalBorrowed - totalRepaid)),
      },
    ];

    sections = [
      {
        type: "table",
        title: "Loans",
        columns: [
          { key: "date", label: "Date", width: 0.22 },
          { key: "amount", label: "Amount", width: 0.28, align: "right" },
          { key: "status", label: "Status", width: 0.2 },
          { key: "reference", label: "Reference", width: 0.3 },
        ],
        rows: loans.map((l) => ({
          date: formatDatePdf(l.createdAt || new Date()),
          amount: formatNaira(l.loanAmount),
          status: String(l.status || "-").toUpperCase(),
          reference: l.reference || l._id?.toString?.() || "-",
        })),
      },
      {
        type: "table",
        title: "Repayments",
        columns: [
          { key: "date", label: "Date", width: 0.22 },
          { key: "amount", label: "Amount", width: 0.28, align: "right" },
          { key: "status", label: "Status", width: 0.2 },
          { key: "reference", label: "Reference", width: 0.3 },
        ],
        rows: repayments.map((r) => ({
          date: formatDatePdf(r.date || new Date()),
          amount: formatNaira(r.amount),
          status: String(r.status || "-").toUpperCase(),
          reference: r.reference || "-",
        })),
      },
    ];
  }

  if (type === "credit_score_report") {
    reportTitle = "Credit Score Report";
    const creditScore = await buildCreditScoreData({
      profileId: req.user.profileId,
      userCreatedAt: req.user.createdAt,
      historyMonths: 6,
    });

    const scoreTier = (score) => {
      if (score >= 750) return "Excellent";
      if (score >= 650) return "Good";
      if (score >= 550) return "Fair";
      return "Needs Improvement";
    };

    summaryItems = [
      {
        label: "Credit Score",
        value: `${creditScore.totalScore} / ${creditScore.maxScore}`,
      },
      {
        label: "Score Change",
        value: `${creditScore.scoreChange} (${creditScore.scoreChangeDirection})`,
      },
      { label: "Last Updated", value: creditScore.lastUpdated },
      { label: "Tier", value: scoreTier(creditScore.totalScore) },
    ];

    const factorRows = Object.entries(creditScore.factors || {}).map(
      ([key, factor]) => ({
        factor: key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase()),
        score: `${factor.score}/${factor.maxScore}`,
        status: String(factor.status || "-").toUpperCase(),
      }),
    );

    const insightItems = [];
    Object.entries(creditScore.factors || {}).forEach(([key, factor]) => {
      const detail = (factor.details || [])[0];
      if (detail) {
        const label = key
          .replace(/_/g, " ")
          .replace(/\b\w/g, (c) => c.toUpperCase());
        insightItems.push(`${label}: ${detail.name} (${detail.value})`);
      }
    });

    sections = [
      {
        type: "table",
        title: "Score Factors",
        columns: [
          { key: "factor", label: "Factor", width: 0.5 },
          { key: "score", label: "Score", width: 0.25 },
          { key: "status", label: "Status", width: 0.25 },
        ],
        rows: factorRows,
      },
      {
        type: "list",
        title: "Key Drivers",
        items:
          insightItems.length > 0
            ? insightItems
            : ["No additional insights available."],
      },
    ];
  }

  if (type === "annual_summary") {
    reportTitle = "Annual Financial Summary";
    const txFilter = { userId: req.user.profileId };
    if (resolved?.start && resolved?.end) {
      txFilter.date = { $gte: resolved.start, $lte: resolved.end };
    }

    const attendanceFilter = { userId: req.user.profileId };
    if (resolved?.start && resolved?.end) {
      attendanceFilter.createdAt = { $gte: resolved.start, $lte: resolved.end };
    }

    const loanFilter = { userId: req.user.profileId };
    if (resolved?.start && resolved?.end) {
      loanFilter.createdAt = { $gte: resolved.start, $lte: resolved.end };
    }

    const [contribs, repayments, memberships, attendance, loansTaken] =
      await Promise.all([
        TransactionModel.find({
          ...txFilter,
          type: "group_contribution",
          status: "success",
        }).lean(),
        TransactionModel.find({
          ...txFilter,
          type: "loan_repayment",
          status: "success",
        }).lean(),
        GroupMembershipModel.find({
          userId: req.user.profileId,
          status: "active",
        }).lean(),
        MeetingAttendanceModel.find(attendanceFilter).lean(),
        LoanApplicationModel.countDocuments(loanFilter),
      ]);

    const summary = {
      totalContributions: contribs.reduce(
        (sum, t) => sum + Number(t.amount || 0),
        0,
      ),
      totalLoanRepayments: repayments.reduce(
        (sum, t) => sum + Number(t.amount || 0),
        0,
      ),
      loansTaken,
      activeGroups: memberships.length,
      meetingsAttended: attendance.filter(
        (a) => a.status === "present" || a.status === "late",
      ).length,
    };

    summaryItems = [
      {
        label: "Total Contributions",
        value: formatNaira(summary.totalContributions),
      },
      {
        label: "Total Loan Repayments",
        value: formatNaira(summary.totalLoanRepayments),
      },
      { label: "Loans Taken", value: `${summary.loansTaken}` },
      { label: "Active Groups", value: `${summary.activeGroups}` },
    ];

    sections = [
      {
        type: "table",
        title: "Summary Metrics",
        columns: [
          { key: "metric", label: "Metric", width: 0.6 },
          { key: "value", label: "Value", width: 0.4, align: "right" },
        ],
        rows: [
          {
            metric: "Total Contributions",
            value: formatNaira(summary.totalContributions),
          },
          {
            metric: "Total Loan Repayments",
            value: formatNaira(summary.totalLoanRepayments),
          },
          { metric: "Loans Taken", value: summary.loansTaken },
          { metric: "Active Groups", value: summary.activeGroups },
          { metric: "Meetings Attended", value: summary.meetingsAttended },
        ],
      },
    ];
  }

  const pdfBuffer = await generateMemberFinancialReportPdfBuffer({
    title: reportTitle,
    memberName,
    periodLabel,
    generatedAt: new Date(),
    summaryItems,
    sections,
    footerNote:
      "CRC Champions Revolving Contributions - Member Financial Report",
  });
  const base64 = pdfBuffer.toString("base64");
  const safePeriod = periodLabel
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9-]/gi, "")
    .toLowerCase();
  const filename = `${type.replace(/_/g, "-")}-${safePeriod || "all-time"}.pdf`;

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      filename,
      mimeType: "application/pdf",
      contentBase64: base64,
    },
  });
});
