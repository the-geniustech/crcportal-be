import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { LoanApplicationModel } from "../models/LoanApplication.js";

export const listGroupLoans = catchAsync(async (req, res) => {
  const group = req.group;

  const filter = { groupId: group._id };
  if (req.query?.status) {
    const status = String(req.query.status).trim();
    if (status === "active") {
      filter.status = { $in: ["disbursed", "defaulted"] };
    } else {
      filter.status = status;
    }
  }

  const loans = await LoanApplicationModel.find(filter)
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
    return { ...loan, repaymentToDate };
  });

  return sendSuccess(res, {
    statusCode: 200,
    results: enrichedLoans.length,
    data: { loans: enrichedLoans },
  });
});
