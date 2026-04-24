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

export const listGroupLoans = catchAsync(async (req, res) => {
  const group = req.group;
  const canViewAll = canViewFullGroupData(req);
  const scopedUserId = resolveScopedGroupUserId(req);

  if (!canViewAll && !scopedUserId) {
    throw new AppError("User profile not found", 400);
  }

  const filter = { groupId: group._id };
  if (req.query?.status) {
    const status = String(req.query.status).trim();
    if (status === "active") {
      filter.status = { $in: ["disbursed", "defaulted"] };
    } else {
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
      activeLoans.map((loan) => syncLoanRepaymentState(loan, { asOf: new Date() })),
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

  const enrichedLoans = loans.map((loan) => {
    const plainLoan =
      typeof loan.toObject === "function" ? loan.toObject() : loan;
    const borrower =
      plainLoan.userId && typeof plainLoan.userId === "object" ? plainLoan.userId : null;
    const borrowerId =
      borrower && borrower._id
        ? String(borrower._id)
        : typeof plainLoan.userId === "string"
          ? plainLoan.userId
          : null;
    const borrowerName =
      borrower && typeof borrower.fullName === "string" ? borrower.fullName : null;
    const borrowerEmail =
      borrower && typeof borrower.email === "string" ? borrower.email : null;
    const borrowerPhone =
      borrower && typeof borrower.phone === "string" ? borrower.phone : null;
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

    return {
      ...plainLoan,
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
      borrowerName,
      borrowerEmail,
      borrowerPhone,
    };
  });

  return sendSuccess(res, {
    statusCode: 200,
    results: enrichedLoans.length,
    data: { loans: enrichedLoans },
  });
});
