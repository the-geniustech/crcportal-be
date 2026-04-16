import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import { LoanGuarantorModel } from "../models/LoanGuarantor.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import { LoanRepaymentScheduleItemModel } from "../models/LoanRepaymentScheduleItem.js";
import { GuarantorNotificationModel } from "../models/GuarantorNotification.js";
import { ProfileModel } from "../models/Profile.js";
import { getLoanScheduleOutstandingAmount } from "../services/loanRepaymentService.js";

function mapLoanStatus(appStatus) {
  if (appStatus === "completed") return "completed";
  if (appStatus === "defaulted") return "defaulted";
  if (appStatus === "disbursed") return "active";
  return "active";
}

export const listMyGuarantorRequests = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const records = await LoanGuarantorModel.find({
    guarantorUserId: req.user.profileId,
    status: "pending",
  }).sort({ createdAt: -1 });

  const loanIds = records.map((r) => r.loanApplicationId);
  const loans = await LoanApplicationModel.find({ _id: { $in: loanIds } });
  const loanById = new Map(loans.map((l) => [String(l._id), l]));

  const borrowerIds = loans.map((l) => l.userId);
  const borrowers = await ProfileModel.find({ _id: { $in: borrowerIds } }).select(
    "fullName email phone",
  );
  const borrowerById = new Map(borrowers.map((p) => [String(p._id), p]));

  const requests = records.map((r) => {
    const loan = loanById.get(String(r.loanApplicationId));
    const borrower = loan ? borrowerById.get(String(loan.userId)) : null;
    const loanAmount = loan?.approvedAmount ?? loan?.loanAmount ?? 0;
    const liabilityAmount = Math.round((Number(r.liabilityPercentage) / 100) * Number(loanAmount));

    return {
      id: r._id,
      guarantorId: r._id,
      loanId: loan?._id || r.loanApplicationId,
      loanCode: loan?.loanCode || null,
      borrowerName: borrower?.fullName || "Member",
      borrowerEmail: borrower?.email || "",
      borrowerPhone: borrower?.phone || "",
      loanAmount,
      liabilityPercentage: r.liabilityPercentage,
      liabilityAmount,
      requestMessage: r.requestMessage || "",
      requestDate: r.createdAt,
      status: r.status,
      loanPurpose: loan?.loanPurpose || "",
      repaymentTerm: loan?.repaymentPeriod || null,
    };
  });

  return sendSuccess(res, { statusCode: 200, results: requests.length, data: { requests } });
});

export const respondToGuarantorRequest = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));
  if (!req.loanGuarantor) return next(new AppError("Missing guarantor context", 500));

  const nextStatus = String(req.body?.status || "").toLowerCase();
  if (!["accepted", "rejected"].includes(nextStatus)) {
    return next(new AppError("Invalid status", 400));
  }

  if (req.loanGuarantor.status !== "pending") {
    return next(new AppError("Guarantor request has already been responded to", 400));
  }

  req.loanGuarantor.status = nextStatus;
  req.loanGuarantor.responseComment = req.body?.responseComment ?? null;
  req.loanGuarantor.respondedAt = new Date();
  await req.loanGuarantor.save();

  await GuarantorNotificationModel.create({
    guarantorId: req.loanGuarantor._id,
    notificationType: "request_response",
    message:
      nextStatus === "accepted"
        ? "You accepted a guarantor request."
        : "You rejected a guarantor request.",
    sentVia: [],
    readAt: null,
  });

  return sendSuccess(res, { statusCode: 200, data: { guarantor: req.loanGuarantor } });
});

export const listMyGuarantorCommitments = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const records = await LoanGuarantorModel.find({
    guarantorUserId: req.user.profileId,
    status: "accepted",
  }).sort({ createdAt: -1 });

  const loanIds = records.map((r) => r.loanApplicationId);
  const loans = await LoanApplicationModel.find({ _id: { $in: loanIds } });
  const loanById = new Map(loans.map((l) => [String(l._id), l]));

  const borrowerIds = loans.map((l) => l.userId);
  const borrowers = await ProfileModel.find({ _id: { $in: borrowerIds } }).select("fullName");
  const borrowerById = new Map(borrowers.map((p) => [String(p._id), p]));

  const commitments = [];
  for (const r of records) {
    const loan = loanById.get(String(r.loanApplicationId));
    if (!loan) continue;

    const borrower = borrowerById.get(String(loan.userId));
    const loanAmount = loan.approvedAmount ?? loan.loanAmount ?? 0;
    const totalRepayable = loan.totalRepayable ?? loanAmount;
    const remainingBalance = loan.remainingBalance ?? 0;
    const totalPaid = Math.max(0, Number(totalRepayable) - Number(remainingBalance));
    const progressPercentage =
      totalRepayable > 0 ? Math.round((totalPaid / totalRepayable) * 100) : 0;

    const nextPayment = await LoanRepaymentScheduleItemModel.findOne({
      loanApplicationId: loan._id,
      status: { $in: ["pending", "upcoming", "overdue"] },
    }).sort({ installmentNumber: 1 });

    const missedPayments = await LoanRepaymentScheduleItemModel.countDocuments({
      loanApplicationId: loan._id,
      status: "overdue",
    });

    const liabilityAmount = Math.round((Number(r.liabilityPercentage) / 100) * Number(loanAmount));

    commitments.push({
      id: r._id,
      guarantorId: r._id,
      loanId: loan._id,
      loanCode: loan.loanCode || null,
      borrowerName: borrower?.fullName || "Member",
      loanAmount,
      liabilityAmount,
      liabilityPercentage: r.liabilityPercentage,
      loanStatus: mapLoanStatus(loan.status),
      disbursedDate: loan.disbursedAt,
      remainingBalance,
      nextPaymentDate: nextPayment?.dueDate || null,
      nextPaymentAmount: nextPayment
        ? getLoanScheduleOutstandingAmount(nextPayment)
        : null,
      missedPayments,
      totalPaid,
      progressPercentage,
    });
  }

  return sendSuccess(res, { statusCode: 200, results: commitments.length, data: { commitments } });
});

export const listMyGuarantorNotifications = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const guarantorIds = await LoanGuarantorModel.find({
    guarantorUserId: req.user.profileId,
  }).distinct("_id");

  const notifications = await GuarantorNotificationModel.find({
    guarantorId: { $in: guarantorIds },
  }).sort({ createdAt: -1 });

  const unread = notifications.filter((n) => !n.readAt).length;

  return sendSuccess(res, {
    statusCode: 200,
    results: notifications.length,
    data: { notifications, unread },
  });
});

export const markGuarantorNotificationRead = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const notificationId = req.params.notificationId || req.params.id;
  if (!notificationId) return next(new AppError("Missing notification id", 400));

  const guarantorIds = await LoanGuarantorModel.find({
    guarantorUserId: req.user.profileId,
  }).distinct("_id");

  const notification = await GuarantorNotificationModel.findOne({
    _id: notificationId,
    guarantorId: { $in: guarantorIds },
  });

  if (!notification) return next(new AppError("Notification not found", 404));

  if (!notification.readAt) {
    notification.readAt = new Date();
    await notification.save();
  }

  return sendSuccess(res, { statusCode: 200, data: { notification } });
});
