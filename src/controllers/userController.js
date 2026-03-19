import { ProfileModel } from "../models/Profile.js";
import { UserModel } from "../models/User.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

function pick(obj, allowedKeys) {
  const out = {};
  for (const key of allowedKeys) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      out[key] = obj[key];
    }
  }
  return out;
}

export const getMe = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const profile = await ProfileModel.findById(req.user.profileId);
  if (!profile) return next(new AppError("Profile not found", 404));

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      user: {
        id: req.user._id,
        email: req.user.email,
        phone: req.user.phone,
        role: req.user.role,
        profileId: req.user.profileId,
        createdAt: req.user.createdAt,
        updatedAt: req.user.updatedAt,
      },
      profile,
    },
  });
});

export const updateMe = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const updates = pick(req.body || {}, [
    "fullName",
    "phone",
    "dateOfBirth",
    "address",
    "city",
    "state",
    "occupation",
    "employer",
    "nextOfKinName",
    "nextOfKinPhone",
    "nextOfKinRelationship",
    "avatar",
  ]);
  if (Object.keys(updates).length === 0) {
    return next(new AppError("No updatable fields provided", 400));
  }

  const profile = await ProfileModel.findByIdAndUpdate(req.user.profileId, updates, {
    new: true,
    runValidators: true,
  });

  if (!profile) return next(new AppError("Profile not found", 404));

  return sendSuccess(res, {
    statusCode: 200,
    data: { profile },
  });
});

export const listUsers = catchAsync(async (req, res) => {
  const filter = {};
  if (req.query?.role) {
    filter.role = req.query.role;
  }

  const users = await UserModel.find(filter)
    .sort({ createdAt: -1 })
    .limit(200);

  const profileIds = users.map((u) => u.profileId).filter(Boolean);
  const profiles = await ProfileModel.find({ _id: { $in: profileIds } });
  const profileById = new Map(profiles.map((p) => [String(p._id), p]));

  return sendSuccess(res, {
    statusCode: 200,
    results: users.length,
    data: {
      users: users.map((u) => ({
        id: u._id,
        email: u.email,
        phone: u.phone,
        role: u.role,
        profileId: u.profileId,
        createdAt: u.createdAt,
        updatedAt: u.updatedAt,
        profile: profileById.get(String(u.profileId)) || null,
      })),
    },
  });
});

export const updateUserRole = catchAsync(async (req, res, next) => {
  const { id } = req.params;
  const { role } = req.body || {};

  const allowed = ["member", "groupCoordinator", "groupGuarantor", "admin"];
  if (!allowed.includes(role)) {
    return next(new AppError(`Invalid role. Allowed: ${allowed.join(", ")}`, 400));
  }

  const user = await UserModel.findById(id);
  if (!user) return next(new AppError("User not found", 404));

  user.role = role;
  await user.save({ validateBeforeSave: false });

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      user: {
        id: user._id,
        email: user.email,
        phone: user.phone,
        role: user.role,
        profileId: user.profileId,
        createdAt: user.createdAt,
        updatedAt: user.updatedAt,
      },
    },
  });
});

export const listMyGroups = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId) return next(new AppError("User profile not found", 400));

  const memberships = await GroupMembershipModel.find({
    userId: req.user.profileId,
    status: "active",
  })
    .sort({ joinedAt: -1 })
    .populate("groupId");

  return sendSuccess(res, {
    statusCode: 200,
    results: memberships.length,
    data: { memberships },
  });
});
