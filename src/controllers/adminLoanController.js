import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { LoanApplicationModel, LoanApplicationStatuses } from "../models/LoanApplication.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { ProfileModel } from "../models/Profile.js";
import {
  getLoanInterestConfig,
  isInterestRateAllowed,
} from "../utils/loanPolicy.js";

async function getManageableGroupIds(req) {
  if (!req.user) throw new AppError("Not authenticated", 401);
  if (!req.user.profileId) throw new AppError("User profile not found", 400);

  if (req.user.role === "admin") return null;

  if (req.user.role !== "groupCoordinator") {
    throw new AppError("Insufficient permissions", 403);
  }

  const memberships = await GroupMembershipModel.find(
    { userId: req.user.profileId, role: "coordinator", status: "active" },
    { groupId: 1 },
  ).lean();

  return memberships.map((m) => String(m.groupId));
}

export const ensureAdminLoanAccess = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (req.user.role === "admin") return next();

  if (req.user.role !== "groupCoordinator") {
    return next(new AppError("Insufficient permissions", 403));
  }

  const loan = req.loanApplication;
  if (!loan) return next(new AppError("Missing loan context", 500));
  if (!loan.groupId) {
    return next(new AppError("Only admins can manage this loan", 403));
  }

  const manageableGroupIds = await getManageableGroupIds(req);
  if (!manageableGroupIds || !manageableGroupIds.includes(String(loan.groupId))) {
    return next(new AppError("You cannot manage loans for this group", 403));
  }

  return next();
});

export const listAdminLoanApplications = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const manageableGroupIds = await getManageableGroupIds(req);

  const filter = {};

  if (manageableGroupIds) {
    filter.groupId = { $in: manageableGroupIds };
  }

  if (typeof req.query?.status === "string" && req.query.status.trim()) {
    const status = String(req.query.status).trim();
    if (LoanApplicationStatuses.includes(status)) filter.status = status;
  }

  const search = typeof req.query?.search === "string" ? req.query.search.trim() : "";
  if (search) {
    filter.$or = [
      { loanCode: { $regex: search, $options: "i" } },
      { groupName: { $regex: search, $options: "i" } },
      { loanPurpose: { $regex: search, $options: "i" } },
    ];
  }

  const page = Math.max(1, parseInt(String(req.query?.page ?? "1"), 10) || 1);
  const limit = Math.min(200, Math.max(1, parseInt(String(req.query?.limit ?? "50"), 10) || 50));
  const skip = (page - 1) * limit;

  const [applications, total] = await Promise.all([
    LoanApplicationModel.find(filter).sort({ createdAt: -1 }).skip(skip).limit(limit).lean(),
    LoanApplicationModel.countDocuments(filter),
  ]);

  const profileIds = applications.map((a) => a.userId).filter(Boolean);
  const profiles = await ProfileModel.find(
    { _id: { $in: profileIds } },
    { fullName: 1, email: 1, phone: 1 },
  ).lean();
  const profileById = new Map(profiles.map((p) => [String(p._id), p]));

  const enriched = applications.map((a) => ({
    ...a,
    applicant: profileById.get(String(a.userId)) || null,
  }));

  return sendSuccess(res, {
    statusCode: 200,
    results: enriched.length,
    total,
    page,
    limit,
    data: { applications: enriched },
  });
});

export const reviewAdminLoanApplication = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const { applicationId } = req.params;
  const app = await LoanApplicationModel.findById(applicationId);
  if (!app) return next(new AppError("Loan application not found", 404));

  if (req.user.role !== "admin") {
    if (!app.groupId) return next(new AppError("Only admins can review this loan", 403));

    const manageableGroupIds = await getManageableGroupIds(req);
    if (!manageableGroupIds || !manageableGroupIds.includes(String(app.groupId))) {
      return next(new AppError("You cannot review loans for this group", 403));
    }
  }

  const { status, reviewNotes, approvedAmount, approvedInterestRate } = req.body || {};

  const allowedStatuses = new Set(["under_review", "approved", "rejected"]);
  if (!status || !allowedStatuses.has(String(status))) {
    return next(new AppError("Invalid review status", 400));
  }

  app.status = status;
  app.reviewNotes = reviewNotes ?? app.reviewNotes;
  app.reviewedBy = req.user.profileId;
  app.reviewedAt = new Date();

  if (status === "approved") {
    if (typeof approvedAmount !== "undefined" && approvedAmount !== null) {
      app.approvedAmount = Number(approvedAmount);
    }
    if (typeof approvedInterestRate !== "undefined" && approvedInterestRate !== null) {
      if (!isInterestRateAllowed(app.loanType || "revolving", approvedInterestRate)) {
        return next(new AppError("approvedInterestRate is not allowed for this loan type", 400));
      }
      app.approvedInterestRate = Number(approvedInterestRate);
    }
    if (!app.interestRateType) {
      const cfg = getLoanInterestConfig(app.loanType || "revolving");
      app.interestRateType = cfg.rateType;
    }
    app.approvedAt = new Date();
  }

  await app.save();

  return sendSuccess(res, { statusCode: 200, data: { application: app } });
});
