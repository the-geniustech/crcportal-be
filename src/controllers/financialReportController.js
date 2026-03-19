import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { ContributionModel } from "../models/Contribution.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import { TransactionModel } from "../models/Transaction.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { MeetingAttendanceModel } from "../models/MeetingAttendance.js";
import { buildCreditScoreData } from "./creditScoreController.js";

function parsePeriod(period) {
  if (!period || String(period).toLowerCase() === "all time") return null;
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

function escapePdfText(text) {
  return String(text).replace(/\\/g, "\\\\").replace(/\(/g, "\\(").replace(/\)/g, "\\)");
}

function generateSimplePdf(lines = []) {
  const header = "%PDF-1.4\n";
  const objects = [];

  const fontObj = "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>";
  const pagesObj = "<< /Type /Pages /Kids [3 0 R] /Count 1 >>";
  const catalogObj = "<< /Type /Catalog /Pages 2 0 R >>";

  const contentLines = [];
  contentLines.push("BT");
  contentLines.push("/F1 12 Tf");
  contentLines.push("72 720 Td");
  const maxLines = 45;
  lines.slice(0, maxLines).forEach((line, idx) => {
    const safe = escapePdfText(line);
    if (idx > 0) contentLines.push("0 -16 Td");
    contentLines.push(`(${safe}) Tj`);
  });
  contentLines.push("ET");
  const contentStream = contentLines.join("\n");
  const contentObj = `<< /Length ${contentStream.length} >>\nstream\n${contentStream}\nendstream`;

  objects.push(catalogObj);
  objects.push(pagesObj);
  objects.push(
    "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
  );
  objects.push(contentObj);
  objects.push(fontObj);

  let offset = header.length;
  const xref = ["xref", `0 ${objects.length + 1}`, "0000000000 65535 f "];
  const body = objects
    .map((obj, idx) => {
      const entry = `${idx + 1} 0 obj\n${obj}\nendobj\n`;
      xref.push(String(offset).padStart(10, "0") + " 00000 n ");
      offset += entry.length;
      return entry;
    })
    .join("");

  const trailer = `trailer << /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${offset}\n%%EOF`;

  return Buffer.from(header + body + xref.join("\n") + "\n" + trailer);
}

function formatNaira(amount) {
  const n = Number(amount || 0);
  if (!Number.isFinite(n)) return "NGN 0";
  return `NGN ${Math.round(n).toLocaleString("en-NG")}`;
}

function buildContributionLines({ memberName, periodLabel, contributions }) {
  const lines = [
    `Contribution Statement - ${memberName}`,
    `Period: ${periodLabel}`,
    "",
    "Date | Amount | Type | Group",
  ];
  contributions.forEach((c) => {
    lines.push(
      `${String(c.date).slice(0, 10)} | ${formatNaira(c.amount)} | ${c.type} | ${c.groupName || "-"}`,
    );
  });
  const total = contributions.reduce((sum, c) => sum + Number(c.amount || 0), 0);
  lines.push("", `Total Contributions: ${formatNaira(total)}`);
  return lines;
}

function buildLoanLines({ memberName, periodLabel, loans, repayments }) {
  const lines = [
    `Loan History - ${memberName}`,
    `Period: ${periodLabel}`,
    "",
    "Loans:",
  ];
  loans.forEach((l) => {
    lines.push(
      `${String(l.createdAt).slice(0, 10)} | ${formatNaira(l.loanAmount)} | ${l.status}`,
    );
  });
  lines.push("", "Repayments:");
  repayments.forEach((r) => {
    lines.push(
      `${String(r.date).slice(0, 10)} | ${formatNaira(r.amount)} | ${r.reference || "-"}`,
    );
  });
  return lines;
}

function buildCreditScoreLines({ memberName, periodLabel, creditScore }) {
  const lines = [
    `Credit Score Report - ${memberName}`,
    `Period: ${periodLabel}`,
    "",
    `Score: ${creditScore.totalScore} / ${creditScore.maxScore}`,
    `Last Updated: ${creditScore.lastUpdated}`,
    `Score Change: ${creditScore.scoreChange} (${creditScore.scoreChangeDirection})`,
    "",
    "Factors:",
  ];
  Object.entries(creditScore.factors || {}).forEach(([key, factor]) => {
    lines.push(`${key}: ${factor.score}/${factor.maxScore} (${factor.status})`);
  });
  return lines;
}

function buildAnnualSummaryLines({ memberName, periodLabel, summary }) {
  const lines = [
    `Annual Financial Summary - ${memberName}`,
    `Period: ${periodLabel}`,
    "",
    `Total Contributions: ${formatNaira(summary.totalContributions)}`,
    `Total Loan Repayments: ${formatNaira(summary.totalLoanRepayments)}`,
    `Loans Taken: ${summary.loansTaken}`,
    `Active Groups: ${summary.activeGroups}`,
    `Meetings Attended: ${summary.meetingsAttended}`,
  ];
  return lines;
}

export const generateMyFinancialReport = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

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
    return next(new AppError(`Invalid report type. Allowed: ${allowed.join(", ")}`, 400));
  }

  const parsed = parsePeriod(period);
  const periodLabel = parsed?.label || "All Time";

  let lines = [];

  if (type === "contribution_statement") {
    const filter = { userId: req.user.profileId };
    if (parsed?.year) filter.year = parsed.year;
    if (parsed?.month) filter.month = parsed.month;

    const contributions = await ContributionModel.find(filter)
      .sort({ year: -1, month: -1, createdAt: -1 })
      .populate("groupId", "groupName")
      .lean();

    const mapped = contributions.map((c) => ({
      date: c.createdAt || new Date().toISOString(),
      amount: c.amount,
      type: c.contributionType,
      groupName: c.groupId?.groupName,
    }));

    lines = buildContributionLines({ memberName, periodLabel, contributions: mapped });
  }

  if (type === "loan_history") {
    const loanFilter = { userId: req.user.profileId };
    if (parsed?.start && parsed?.end) {
      loanFilter.createdAt = { $gte: parsed.start, $lte: parsed.end };
    }

    const loans = await LoanApplicationModel.find(loanFilter)
      .sort({ createdAt: -1 })
      .lean();

    const loanIds = loans.map((l) => l._id);

    const repayments = await TransactionModel.find({
      userId: req.user.profileId,
      type: "loan_repayment",
      ...(parsed?.start && parsed?.end ? { date: { $gte: parsed.start, $lte: parsed.end } } : {}),
      ...(loanIds.length > 0 ? { loanId: { $in: loanIds } } : {}),
    })
      .sort({ date: -1 })
      .lean();

    lines = buildLoanLines({
      memberName,
      periodLabel,
      loans,
      repayments,
    });
  }

  if (type === "credit_score_report") {
    const creditScore = await buildCreditScoreData({
      profileId: req.user.profileId,
      userCreatedAt: req.user.createdAt,
      historyMonths: 6,
    });
    lines = buildCreditScoreLines({ memberName, periodLabel, creditScore });
  }

  if (type === "annual_summary") {
    const txFilter = { userId: req.user.profileId };
    if (parsed?.start && parsed?.end) {
      txFilter.date = { $gte: parsed.start, $lte: parsed.end };
    }

    const [contribs, repayments, memberships, attendance] = await Promise.all([
      TransactionModel.find({ ...txFilter, type: "group_contribution", status: "success" }).lean(),
      TransactionModel.find({ ...txFilter, type: "loan_repayment", status: "success" }).lean(),
      GroupMembershipModel.find({ userId: req.user.profileId, status: "active" }).lean(),
      MeetingAttendanceModel.find({ userId: req.user.profileId }).lean(),
    ]);

    const summary = {
      totalContributions: contribs.reduce((sum, t) => sum + Number(t.amount || 0), 0),
      totalLoanRepayments: repayments.reduce((sum, t) => sum + Number(t.amount || 0), 0),
      loansTaken: await LoanApplicationModel.countDocuments({ userId: req.user.profileId }),
      activeGroups: memberships.length,
      meetingsAttended: attendance.filter((a) => a.status === "present" || a.status === "late").length,
    };

    lines = buildAnnualSummaryLines({ memberName, periodLabel, summary });
  }

  const pdfBuffer = generateSimplePdf(lines);
  const base64 = pdfBuffer.toString("base64");
  const filename = `${type.replace(/_/g, "-")}-${periodLabel.replace(/\s+/g, "-").toLowerCase()}.pdf`;

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      filename,
      mimeType: "application/pdf",
      contentBase64: base64,
    },
  });
});
