import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import { ContributionModel } from "../models/Contribution.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import { LoanRepaymentScheduleItemModel } from "../models/LoanRepaymentScheduleItem.js";

export const getDashboardSummary = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const profileId = req.user.profileId;

  const contributionAgg = await ContributionModel.aggregate([
    {
      $match: {
        userId: profileId,
        status: { $in: ["completed", "verified"] },
      },
    },
    { $group: { _id: null, total: { $sum: "$amount" } } },
  ]);

  const totalContributions = Number(contributionAgg?.[0]?.total ?? 0);

  const activeLoans = await LoanApplicationModel.find({
    userId: profileId,
    status: { $in: ["disbursed", "defaulted"] },
  })
    .select("_id remainingBalance loanCode groupName")
    .lean();

  const activeLoanOutstanding = activeLoans.reduce(
    (sum, loan) => sum + Number(loan.remainingBalance ?? 0),
    0,
  );

  const activeLoanIds = activeLoans.map((loan) => loan._id);
  let nextPayment = null;

  if (activeLoanIds.length > 0) {
    const scheduleItem = await LoanRepaymentScheduleItemModel.findOne({
      loanApplicationId: { $in: activeLoanIds },
      status: { $in: ["pending", "upcoming", "overdue"] },
    })
      .sort({ dueDate: 1 })
      .lean();

    if (scheduleItem) {
      const loanMeta = activeLoans.find(
        (loan) => String(loan._id) === String(scheduleItem.loanApplicationId),
      );
      nextPayment = {
        loanId: scheduleItem.loanApplicationId,
        loanCode: loanMeta?.loanCode ?? null,
        groupName: loanMeta?.groupName ?? null,
        dueDate: scheduleItem.dueDate,
        amount: scheduleItem.totalAmount,
        status: scheduleItem.status,
      };
    }
  }

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      totalContributions,
      activeLoanOutstanding,
      nextPayment,
    },
  });
});
