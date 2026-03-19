import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";

export const loadGroup = catchAsync(async (req, res, next) => {
  const groupId = req.params.groupId || req.params.id;
  if (!groupId) return next(new AppError("Missing group id", 400));

  const group = await GroupModel.findById(groupId);
  if (!group) return next(new AppError("Group not found", 404));

  req.group = group;
  return next();
});

export const loadMyGroupMembership = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.group) return next(new AppError("Missing group context", 500));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const membership = await GroupMembershipModel.findOne({
    groupId: req.group._id,
    userId: req.user.profileId,
  });

  req.groupMembership = membership || null;
  return next();
});

export function requireActiveMembership() {
  return (req, res, next) => {
    if (req.user?.role === "admin") return next();
    if (!req.groupMembership) return next(new AppError("Not a group member", 403));
    if (req.groupMembership.status !== "active") {
      return next(new AppError("Group membership is not active", 403));
    }
    return next();
  };
}

export function requireGroupRole(...allowedRoles) {
  return (req, res, next) => {
    if (req.user?.role === "admin") return next();
    if (!req.groupMembership) return next(new AppError("Not a group member", 403));
    if (req.groupMembership.status !== "active") {
      return next(new AppError("Group membership is not active", 403));
    }
    if (!allowedRoles.includes(req.groupMembership.role)) {
      return next(new AppError("Insufficient group permissions", 403));
    }
    return next();
  };
}

export function requireGroupReadAccess() {
  return (req, res, next) => {
    if (
      ["admin", "groupCoordinator", "groupGuarantor", "group_guarantor"].includes(
        req.user?.role || "",
      )
    ) {
      return next();
    }
    if (!req.groupMembership) return next(new AppError("Not a group member", 403));
    if (req.groupMembership.status !== "active") {
      return next(new AppError("Group membership is not active", 403));
    }
    return next();
  };
}
