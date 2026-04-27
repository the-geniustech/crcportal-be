import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import AppError from "../utils/AppError.js";
import { canViewFullGroupData, resolveScopedGroupUserId } from "../utils/groupAccess.js";

import { LoanApplicationModel } from "../models/LoanApplication.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { LoanRepaymentScheduleItemModel } from "../models/LoanRepaymentScheduleItem.js";
import {
  getLoanRemainingBreakdown,
  getLoanRepaymentToDate,
  syncLoanRepaymentState,
} from "../services/loanRepaymentService.js";
import { generateGroupLoanLedgerPdfBuffer } from "../services/pdf/groupLoanLedgerPdf.js";
import { generateGroupLoanLedgerWorkbookBuffer } from "../services/groupLoanLedgerWorkbook.js";

const roundCurrency = (value) =>
  Math.round((Number(value || 0) + Number.EPSILON) * 100) / 100;

const buildAggregateBreakdown = ({ principal, totalRepayable, totalRepaid }) => {
  const safePrincipal = Math.max(0, roundCurrency(principal ?? 0));
  const safeTotalRepayable = Math.max(
    safePrincipal,
    roundCurrency(totalRepayable ?? safePrincipal),
  );
  const totalInterest = Math.max(
    0,
    roundCurrency(safeTotalRepayable - safePrincipal),
  );
  const safeTotalRepaid = Math.max(
    0,
    Math.min(roundCurrency(totalRepaid ?? 0), safeTotalRepayable),
  );
  const repaidInterest = Math.min(safeTotalRepaid, totalInterest);
  const repaidPrincipal = Math.min(
    safePrincipal,
    Math.max(0, roundCurrency(safeTotalRepaid - repaidInterest)),
  );
  const remainingInterest = Math.max(
    0,
    roundCurrency(totalInterest - repaidInterest),
  );
  const remainingPrincipal = Math.max(
    0,
    roundCurrency(safePrincipal - repaidPrincipal),
  );

  return {
    repaidPrincipal,
    repaidInterest,
    remainingPrincipal,
    remainingInterest,
  };
};

const buildScheduleBreakdown = (scheduleItem) => {
  const principalAmount = Math.max(
    0,
    roundCurrency(scheduleItem?.principalAmount ?? 0),
  );
  const interestAmount = Math.max(
    0,
    roundCurrency(scheduleItem?.interestAmount ?? 0),
  );
  const totalAmount = Math.max(
    principalAmount + interestAmount,
    roundCurrency(scheduleItem?.totalAmount ?? principalAmount + interestAmount),
  );
  const paidInterest = Math.max(
    0,
    Math.min(
      roundCurrency(scheduleItem?.paidInterestAmount ?? scheduleItem?.paidAmount ?? 0),
      interestAmount,
    ),
  );
  const paidPrincipal = Math.max(
    0,
    Math.min(
      roundCurrency(
        scheduleItem?.paidPrincipalAmount ??
          Math.max(0, Number(scheduleItem?.paidAmount ?? 0) - paidInterest),
      ),
      principalAmount,
    ),
  );

  return {
    repaidPrincipal: paidPrincipal,
    repaidInterest: paidInterest,
    remainingPrincipal: Math.max(
      0,
      roundCurrency(principalAmount - paidPrincipal),
    ),
    remainingInterest: Math.max(
      0,
      roundCurrency(interestAmount - paidInterest),
    ),
  };
};

const loanLedgerCollator = new Intl.Collator("en", {
  numeric: true,
  sensitivity: "base",
});

const normalizeLoanType = (value) => {
  const raw = String(value || "")
    .trim()
    .toLowerCase();
  if (!raw) return "revolving";
  if (raw.includes("revolv")) return "revolving";
  if (raw.includes("special")) return "special";
  if (raw.includes("bridg") || raw.includes("bridge")) return "bridging";
  if (raw.includes("soft")) return "soft";
  return "revolving";
};

const getLoanStatusKey = (status) => {
  const normalized = String(status || "")
    .trim()
    .toLowerCase();
  if (!normalized) return "unknown";
  if (["overdue", "defaulted", "default"].includes(normalized))
    return "overdue";
  if (["completed", "repaid", "closed", "paid"].includes(normalized))
    return "repaid";
  if (["disbursed", "active"].includes(normalized)) return "active";
  if (["approved"].includes(normalized)) return "approved";
  if (["pending", "under_review", "review"].includes(normalized))
    return "pending";
  if (["rejected", "declined", "cancelled"].includes(normalized))
    return "rejected";
  return "unknown";
};

const normalizeLoanLedgerSort = (value) => {
  const raw = String(value || "")
    .trim()
    .toLowerCase();
  const allowed = new Set([
    "updated_desc",
    "updated_asc",
    "name_asc",
    "name_desc",
    "serial_asc",
    "serial_desc",
    "principal_desc",
    "principal_asc",
    "outstanding_desc",
    "outstanding_asc",
    "repaid_desc",
    "repaid_asc",
  ]);
  return allowed.has(raw) ? raw : "updated_desc";
};

const formatDateLabel = (value) => {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleDateString("en-NG", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
};

const resolveLoanDisplay = (loan) => {
  const principal = Math.max(
    0,
    roundCurrency(loan.approvedAmount ?? loan.loanAmount ?? 0),
  );
  const remainingBalance =
    loan.remainingBalance === null || loan.remainingBalance === undefined
      ? null
      : roundCurrency(loan.remainingBalance);
  const totalRepayable =
    loan.totalRepayable === null || loan.totalRepayable === undefined
      ? null
      : roundCurrency(loan.totalRepayable);
  const fallbackRepaymentToDate =
    loan.repaymentToDate === null || loan.repaymentToDate === undefined
      ? totalRepayable != null && remainingBalance != null
        ? roundCurrency(Math.max(0, totalRepayable - remainingBalance))
        : 0
      : roundCurrency(loan.repaymentToDate);
  const fallbackSplit = buildAggregateBreakdown({
    principal,
    totalRepayable,
    totalRepaid: fallbackRepaymentToDate,
  });
  const repaidPrincipal = roundCurrency(
    loan.repaidPrincipalToDate ?? fallbackSplit.repaidPrincipal,
  );
  const repaidInterest = roundCurrency(
    loan.repaidInterestToDate ?? fallbackSplit.repaidInterest,
  );
  const remainingPrincipal = roundCurrency(
    loan.remainingPrincipalBalance ?? fallbackSplit.remainingPrincipal,
  );
  const remainingInterest = roundCurrency(
    loan.remainingInterestBalance ?? fallbackSplit.remainingInterest,
  );
  const remaining =
    remainingBalance != null
      ? roundCurrency(Math.max(0, remainingBalance))
      : roundCurrency(remainingPrincipal + remainingInterest);
  const repaymentToDate =
    loan.repaymentToDate === null || loan.repaymentToDate === undefined
      ? roundCurrency(repaidPrincipal + repaidInterest)
      : roundCurrency(loan.repaymentToDate);
  const interestPatronage = roundCurrency(
    loan.interestPatronageAccrued ?? repaidInterest * 0.03,
  );
  const progressBase =
    totalRepayable != null && totalRepayable > 0 ? totalRepayable : principal;
  const progressNumerator =
    totalRepayable != null && totalRepayable > 0
      ? repaymentToDate
      : repaidPrincipal;
  const progressValue =
    progressBase > 0
      ? Math.min(100, Math.max(0, (progressNumerator / progressBase) * 100))
      : null;

  return {
    principal,
    remaining,
    remainingPrincipal,
    remainingInterest,
    repaymentToDate,
    repaidPrincipal,
    repaidInterest,
    interestPatronage,
    progressValue,
  };
};

const buildLoanSummary = (loans) =>
  loans.reduce(
    (acc, loan) => {
      const display = resolveLoanDisplay(loan);
      const statusKey = getLoanStatusKey(loan.status);
      acc.total += 1;
      acc.principal += display.principal;
      acc.outstanding += Math.max(display.remaining || 0, 0);
      acc.remainingPrincipal += display.remainingPrincipal;
      acc.remainingInterest += display.remainingInterest;
      acc.repaid += display.repaymentToDate;
      acc.repaidPrincipal += display.repaidPrincipal;
      acc.repaidInterest += display.repaidInterest;
      acc.patronage += display.interestPatronage;
      if (statusKey === "active") acc.active += 1;
      if (statusKey === "overdue") acc.overdue += 1;
      if (statusKey === "repaid") acc.repaidCount += 1;
      return acc;
    },
    {
      total: 0,
      active: 0,
      overdue: 0,
      repaidCount: 0,
      principal: 0,
      outstanding: 0,
      remainingPrincipal: 0,
      remainingInterest: 0,
      repaid: 0,
      repaidPrincipal: 0,
      repaidInterest: 0,
      patronage: 0,
    },
  );

const buildLoanExportRows = (loans) =>
  loans.map((loan) => {
    const display = resolveLoanDisplay(loan);
    const loanCode =
      loan.loanCode || `LN-${String(loan._id || loan.id).slice(-6).toUpperCase()}`;
    const loanTypeLabel =
      {
        revolving: "Revolving Loan",
        special: "Special Loan",
        bridging: "Bridging Loan",
        soft: "Soft Loan",
      }[loan.typeKey] || "Loan";
    return {
      loanCode,
      loanType: loanTypeLabel,
      borrowerName: loan.borrowerName || "Member",
      memberSerial: loan.memberSerial || "-",
      borrowerEmail: loan.borrowerEmail || "-",
      borrowerPhone: loan.borrowerPhone || "-",
      principal: display.principal,
      interestRate: loan.interestLabel || "-",
      remainingPrincipal: display.remainingPrincipal,
      remainingInterest: display.remainingInterest,
      repaidPrincipal: display.repaidPrincipal,
      repaidInterest: display.repaidInterest,
      patronage: display.interestPatronage,
      status: String(loan.status || "-"),
      progress:
        display.progressValue == null
          ? "-"
          : `${Math.round(display.progressValue)}%`,
      disbursedAt: formatDateLabel(loan.disbursedAt),
      updatedAt: formatDateLabel(loan.updatedAt || loan.createdAt),
      updatedAtValue: new Date(loan.updatedAt || loan.createdAt || 0).getTime(),
    };
  });

const filterLoanExportRows = (rows, searchQuery) => {
  const query = String(searchQuery || "")
    .trim()
    .toLowerCase();
  if (!query) return rows;
  return rows.filter((row) =>
    [
      row.loanCode,
      row.loanType,
      row.borrowerName,
      row.memberSerial,
      row.borrowerEmail,
      row.borrowerPhone,
      row.status,
    ]
      .join(" ")
      .toLowerCase()
      .includes(query),
  );
};

const sortLoanExportRows = (rows, sortBy) =>
  [...rows].sort((left, right) => {
    switch (sortBy) {
      case "updated_asc":
        return Number(left.updatedAtValue || 0) - Number(right.updatedAtValue || 0);
      case "name_asc":
        return loanLedgerCollator.compare(left.borrowerName, right.borrowerName);
      case "name_desc":
        return loanLedgerCollator.compare(right.borrowerName, left.borrowerName);
      case "serial_asc":
        return loanLedgerCollator.compare(left.memberSerial, right.memberSerial);
      case "serial_desc":
        return loanLedgerCollator.compare(right.memberSerial, left.memberSerial);
      case "principal_desc":
        return Number(right.principal || 0) - Number(left.principal || 0);
      case "principal_asc":
        return Number(left.principal || 0) - Number(right.principal || 0);
      case "outstanding_desc":
        return (
          Number(right.remainingPrincipal || 0) +
          Number(right.remainingInterest || 0) -
          (Number(left.remainingPrincipal || 0) + Number(left.remainingInterest || 0))
        );
      case "outstanding_asc":
        return (
          Number(left.remainingPrincipal || 0) +
          Number(left.remainingInterest || 0) -
          (Number(right.remainingPrincipal || 0) + Number(right.remainingInterest || 0))
        );
      case "repaid_desc":
        return (
          Number(right.repaidPrincipal || 0) +
          Number(right.repaidInterest || 0) -
          (Number(left.repaidPrincipal || 0) + Number(left.repaidInterest || 0))
        );
      case "repaid_asc":
        return (
          Number(left.repaidPrincipal || 0) +
          Number(left.repaidInterest || 0) -
          (Number(right.repaidPrincipal || 0) + Number(right.repaidInterest || 0))
        );
      case "updated_desc":
      default:
        return Number(right.updatedAtValue || 0) - Number(left.updatedAtValue || 0);
    }
  });

const csvEscape = (value) => {
  const raw = String(value ?? "");
  if (/[",\n]/.test(raw)) return `"${raw.replace(/"/g, '""')}"`;
  return raw;
};

async function loadEnrichedGroupLoans(req) {
  const group = req.group;
  const canViewAll = canViewFullGroupData(req);
  const scopedUserId = resolveScopedGroupUserId(req);

  if (!canViewAll && !scopedUserId) {
    throw new AppError("User profile not found", 400);
  }

  const filter = { groupId: group._id };
  if (req.query?.status) {
    const status = String(req.query.status).trim().toLowerCase();
    if (status === "active") {
      filter.status = { $in: ["disbursed", "defaulted"] };
    } else if (status === "overdue") {
      filter.status = { $in: ["overdue", "defaulted", "default"] };
    } else if (status === "repaid") {
      filter.status = { $in: ["completed", "repaid", "closed", "paid"] };
    } else if (status === "pending") {
      filter.status = { $in: ["pending", "under_review", "review"] };
    } else if (status === "rejected") {
      filter.status = { $in: ["rejected", "declined", "cancelled"] };
    } else if (status !== "all") {
      filter.status = status;
    }
  }
  if (!canViewAll && scopedUserId) {
    filter.userId = scopedUserId;
  }

  const loans = await LoanApplicationModel.find(filter)
    .populate("userId", "fullName email phone")
    .sort({ createdAt: -1 });

  const activeLoans = loans.filter((loan) =>
    ["disbursed", "defaulted"].includes(String(loan.status || "")),
  );
  if (activeLoans.length > 0) {
    await Promise.all(
      activeLoans.map((loan) =>
        syncLoanRepaymentState(loan, { asOf: new Date() }),
      ),
    );
  }

  const loanIds = loans.map((loan) => loan._id);
  const borrowerIds = loans
    .map((loan) => {
      if (loan.userId && typeof loan.userId === "object" && loan.userId._id) {
        return String(loan.userId._id);
      }
      if (typeof loan.userId === "string") return loan.userId;
      return null;
    })
    .filter(Boolean);

  const [memberships, repaymentScheduleItems] =
    loanIds.length === 0
      ? [[], []]
      : await Promise.all([
          borrowerIds.length === 0
            ? Promise.resolve([])
            : GroupMembershipModel.find({
                groupId: group._id,
                userId: { $in: borrowerIds },
              })
                .select("userId memberSerial")
                .lean(),
          LoanRepaymentScheduleItemModel.find({
            loanApplicationId: { $in: loanIds },
            isProjected: { $ne: true },
          })
            .select(
              "loanApplicationId principalAmount interestAmount totalAmount paidAmount paidPrincipalAmount paidInterestAmount",
            )
            .lean(),
        ]);

  const membershipSerialByUserId = new Map(
    memberships.map((membership) => [
      String(membership.userId),
      membership.memberSerial ?? null,
    ]),
  );

  const scheduleBreakdownByLoanId = new Map();
  for (const item of repaymentScheduleItems) {
    const loanId = String(item.loanApplicationId);
    const current = scheduleBreakdownByLoanId.get(loanId) ?? {
      repaidPrincipal: 0,
      repaidInterest: 0,
      remainingPrincipal: 0,
      remainingInterest: 0,
    };
    const breakdown = buildScheduleBreakdown(item);
    current.repaidPrincipal = roundCurrency(
      current.repaidPrincipal + breakdown.repaidPrincipal,
    );
    current.repaidInterest = roundCurrency(
      current.repaidInterest + breakdown.repaidInterest,
    );
    current.remainingPrincipal = roundCurrency(
      current.remainingPrincipal + breakdown.remainingPrincipal,
    );
    current.remainingInterest = roundCurrency(
      current.remainingInterest + breakdown.remainingInterest,
    );
    scheduleBreakdownByLoanId.set(loanId, current);
  }

  return loans.map((loan) => {
    const plainLoan =
      typeof loan.toObject === "function" ? loan.toObject() : loan;
    const borrower =
      plainLoan.userId && typeof plainLoan.userId === "object"
        ? plainLoan.userId
        : null;
    const borrowerId =
      borrower && borrower._id
        ? String(borrower._id)
        : typeof plainLoan.userId === "string"
          ? plainLoan.userId
          : null;
    const principal = roundCurrency(
      plainLoan.approvedAmount ?? plainLoan.loanAmount ?? 0,
    );
    const totalRepayable =
      plainLoan.totalRepayable === null || plainLoan.totalRepayable === undefined
        ? null
        : roundCurrency(plainLoan.totalRepayable);
    const remainingBreakdown = getLoanRemainingBreakdown(plainLoan);
    const persistedRemaining =
      plainLoan.remainingBalance === null || plainLoan.remainingBalance === undefined
        ? null
        : roundCurrency(plainLoan.remainingBalance);
    const persistedRepaid = roundCurrency(getLoanRepaymentToDate(plainLoan));
    const scheduleBreakdown = scheduleBreakdownByLoanId.get(String(loan._id));
    const fallbackBreakdown = buildAggregateBreakdown({
      principal,
      totalRepayable: totalRepayable ?? principal,
      totalRepaid: persistedRepaid ?? 0,
    });
    const repaidPrincipalToDate = roundCurrency(
      plainLoan.totalPrincipalPaid ??
        scheduleBreakdown?.repaidPrincipal ??
        fallbackBreakdown.repaidPrincipal,
    );
    const repaidInterestToDate = roundCurrency(
      plainLoan.totalInterestPaid ??
        scheduleBreakdown?.repaidInterest ??
        fallbackBreakdown.repaidInterest,
    );
    const remainingPrincipalBalance = roundCurrency(
      remainingBreakdown.principalOutstanding ??
        scheduleBreakdown?.remainingPrincipal ??
        fallbackBreakdown.remainingPrincipal,
    );
    const remainingInterestBalance = roundCurrency(
      remainingBreakdown.accruedInterestBalance ??
        scheduleBreakdown?.remainingInterest ??
        fallbackBreakdown.remainingInterest,
    );
    const computedRemainingBalance = roundCurrency(
      remainingPrincipalBalance + remainingInterestBalance,
    );
    const computedRepaymentToDate = roundCurrency(
      repaidPrincipalToDate + repaidInterestToDate,
    );
    const interestRateValue =
      plainLoan.approvedInterestRate ?? plainLoan.interestRate ?? null;
    const interestRateLabel =
      interestRateValue != null && Number.isFinite(Number(interestRateValue))
        ? `${Number(interestRateValue)}%`
        : "-";

    return {
      ...plainLoan,
      typeKey: normalizeLoanType(plainLoan.loanType),
      interestLabel: interestRateLabel,
      memberSerial: borrowerId
        ? membershipSerialByUserId.get(borrowerId) ?? null
        : null,
      remainingBalance:
        persistedRemaining !== null ? persistedRemaining : computedRemainingBalance,
      repaymentToDate:
        persistedRepaid !== null ? persistedRepaid : computedRepaymentToDate,
      remainingPrincipalBalance,
      remainingInterestBalance,
      repaidPrincipalToDate,
      repaidInterestToDate,
      interestPatronageAccrued: roundCurrency(repaidInterestToDate * 0.03),
      borrowerName:
        borrower && typeof borrower.fullName === "string" ? borrower.fullName : null,
      borrowerEmail:
        borrower && typeof borrower.email === "string" ? borrower.email : null,
      borrowerPhone:
        borrower && typeof borrower.phone === "string" ? borrower.phone : null,
    };
  });
}

export const listGroupLoans = catchAsync(async (req, res) => {
  const enrichedLoans = await loadEnrichedGroupLoans(req);

  return sendSuccess(res, {
    statusCode: 200,
    results: enrichedLoans.length,
    data: { loans: enrichedLoans },
  });
});

export const exportGroupLoanLedger = catchAsync(async (req, res, next) => {
  const group = req.group;
  const format = String(req.query?.format || "csv")
    .trim()
    .toLowerCase();
  if (!["csv", "pdf", "xlsx"].includes(format)) {
    return next(new AppError("Invalid export format", 400));
  }

  const selectedType = normalizeLoanType(req.query?.loanType);
  const searchQuery = String(req.query?.search || "").trim();
  const sortBy = normalizeLoanLedgerSort(req.query?.sortBy || req.query?.sort);
  const statusLabel =
    String(req.query?.status || "all")
      .trim()
      .toLowerCase() || "all";

  const enrichedLoans = await loadEnrichedGroupLoans(req);
  const scopedLoans = enrichedLoans.filter((loan) => loan.typeKey === selectedType);
  const filteredRows = sortLoanExportRows(
    filterLoanExportRows(buildLoanExportRows(scopedLoans), searchQuery),
    sortBy,
  );

  if (filteredRows.length === 0) {
    return next(new AppError("No loans matched the current filters.", 400));
  }

  const summary = buildLoanSummary(scopedLoans.filter((loan) => {
    const row = buildLoanExportRows([loan])[0];
    return filterLoanExportRows([row], searchQuery).length > 0;
  }));

  const filenameBase = `loan-ledger-${String(group.groupName || "group")
    .toLowerCase()
    .replace(/\s+/g, "-")}-${selectedType}-${new Date()
    .toISOString()
    .slice(0, 10)}`;

  if (format === "csv") {
    const headers = [
      "Loan Code",
      "Loan Type",
      "Borrower Name",
      "Membership Serial",
      "Borrower Email",
      "Borrower Phone",
      "Principal",
      "Interest Rate",
      "Remaining Principal Due",
      "Remaining Interest Due",
      "Principal Paid",
      "Interest Paid",
      "Interest Patronage",
      "Status",
      "Progress",
      "Disbursed At",
      "Updated At",
    ];
    const csv = `\uFEFF${[
      headers,
      ...filteredRows.map((row) => [
        row.loanCode,
        row.loanType,
        row.borrowerName,
        row.memberSerial,
        row.borrowerEmail,
        row.borrowerPhone,
        row.principal,
        row.interestRate,
        row.remainingPrincipal,
        row.remainingInterest,
        row.repaidPrincipal,
        row.repaidInterest,
        row.patronage,
        row.status,
        row.progress,
        row.disbursedAt,
        row.updatedAt,
      ]),
      [
        "Totals",
        `${filteredRows.length} loans`,
        "-",
        "-",
        "-",
        "-",
        summary.principal,
        "-",
        summary.remainingPrincipal,
        summary.remainingInterest,
        summary.repaidPrincipal,
        summary.repaidInterest,
        summary.patronage,
        statusLabel === "all" ? "All statuses" : statusLabel,
        "-",
        "-",
        "-",
      ],
    ]
      .map((row) => row.map((value) => csvEscape(value)).join(","))
      .join("\n")}`;

    res.setHeader("Content-Type", "text/csv");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${filenameBase}.csv"`,
    );
    return res.status(200).send(csv);
  }

  if (format === "xlsx") {
    const workbookBuffer = await generateGroupLoanLedgerWorkbookBuffer({
      groupName: group.groupName || "Group",
      loanTypeLabel: filteredRows[0]?.loanType || "Loan Portfolio",
      statusLabel: statusLabel === "all" ? "All Statuses" : statusLabel,
      generatedAt: new Date(),
      rows: filteredRows,
      summary: {
        ...summary,
        total: filteredRows.length,
        statusLabel: statusLabel === "all" ? "All Statuses" : statusLabel,
      },
    });

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    );
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${filenameBase}.xlsx"`,
    );
    return res.status(200).send(workbookBuffer);
  }

  const pdfBuffer = await generateGroupLoanLedgerPdfBuffer({
    groupName: group.groupName || "Group",
    loanTypeLabel: filteredRows[0]?.loanType || "Loan Portfolio",
    statusLabel: statusLabel === "all" ? "All Statuses" : statusLabel,
    generatedAt: new Date(),
    rows: filteredRows.map((row) => ({
      ...row,
      principal: row.principal.toLocaleString("en-NG"),
      remainingPrincipal: row.remainingPrincipal.toLocaleString("en-NG"),
      remainingInterest: row.remainingInterest.toLocaleString("en-NG"),
      repaidPrincipal: row.repaidPrincipal.toLocaleString("en-NG"),
      repaidInterest: row.repaidInterest.toLocaleString("en-NG"),
      patronage: row.patronage.toLocaleString("en-NG"),
    })),
    summary: {
      ...summary,
      total: filteredRows.length,
      statusLabel: statusLabel === "all" ? "All Statuses" : statusLabel,
    },
  });

  res.setHeader("Content-Type", "application/pdf");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="${filenameBase}.pdf"`,
  );
  return res.status(200).send(pdfBuffer);
});
