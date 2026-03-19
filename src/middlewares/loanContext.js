import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import { LoanGuarantorModel } from "../models/LoanGuarantor.js";

export const loadLoanApplication = catchAsync(async (req, res, next) => {
  const applicationId = req.params.applicationId || req.params.loanId || req.params.id;
  if (!applicationId) return next(new AppError("Missing loan application id", 400));

  const app = await LoanApplicationModel.findById(applicationId);
  if (!app) return next(new AppError("Loan application not found", 404));

  req.loanApplication = app;
  return next();
});

export function requireLoanOwnerOrAdmin() {
  return (req, res, next) => {
    if (!req.user) return next(new AppError("Not authenticated", 401));
    if (!req.loanApplication) return next(new AppError("Missing loan context", 500));

    if (req.user.role === "admin") return next();

    if (!req.user.profileId) {
      return next(new AppError("User profile not found", 400));
    }

    if (String(req.loanApplication.userId) !== String(req.user.profileId)) {
      return next(new AppError("You do not have access to this loan", 403));
    }

    return next();
  };
}

export const loadLoanGuarantor = catchAsync(async (req, res, next) => {
  const guarantorId = req.params.guarantorId || req.params.id;
  if (!guarantorId) return next(new AppError("Missing guarantor id", 400));

  const record = await LoanGuarantorModel.findById(guarantorId);
  if (!record) return next(new AppError("Guarantor request not found", 404));

  req.loanGuarantor = record;
  return next();
});

export function requireGuarantorOwnerOrAdmin() {
  return (req, res, next) => {
    if (!req.user) return next(new AppError("Not authenticated", 401));
    if (!req.loanGuarantor) return next(new AppError("Missing guarantor context", 500));

    if (req.user.role === "admin") return next();
    if (!req.user.profileId) return next(new AppError("User profile not found", 400));

    if (String(req.loanGuarantor.guarantorUserId) !== String(req.user.profileId)) {
      return next(new AppError("You do not have access to this guarantor request", 403));
    }

    return next();
  };
}

