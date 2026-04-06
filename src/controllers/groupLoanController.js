import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import AppError from "../utils/AppError.js";
import { canViewFullGroupData, resolveScopedGroupUserId } from "../utils/groupAccess.js";

import { LoanApplicationModel } from "../models/LoanApplication.js";

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
    .sort({ createdAt: -1 })
    .lean();

  const enrichedLoans = loans.map((loan) => {
    const totalRepayable = Number(loan.totalRepayable ?? 0);
    const remainingBalance = Number(loan.remainingBalance ?? 0);
    const repaymentToDate =
      Number.isFinite(totalRepayable) &&
      totalRepayable > 0 &&
      Number.isFinite(remainingBalance)
        ? Math.max(0, totalRepayable - remainingBalance)
        : null;
    const borrower =
      loan.userId && typeof loan.userId === "object" ? loan.userId : null;
    const borrowerName =
      borrower && typeof borrower.fullName === "string" ? borrower.fullName : null;
    const borrowerEmail =
      borrower && typeof borrower.email === "string" ? borrower.email : null;
    const borrowerPhone =
      borrower && typeof borrower.phone === "string" ? borrower.phone : null;
    return {
      ...loan,
      repaymentToDate,
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
