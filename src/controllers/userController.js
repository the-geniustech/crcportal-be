import mongoose from "mongoose";
import { ProfileModel } from "../models/Profile.js";
import { UserModel } from "../models/User.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import {
  getContributionSettingsWindowStatus,
  resolveExpectedContributionAmount,
} from "../utils/contributionPolicy.js";
import { sha256 } from "../utils/crypto.js";
import { sendPhoneOtp } from "../services/sms/sendPhoneOtp.js";
import { sendEmailOtp } from "../services/mail/sendEmailOtp.js";
import {
  UserRoles,
  coerceUserRoles,
  normalizeUserRoles,
  pickPrimaryRole,
} from "../utils/roles.js";
import { normalizeNigerianPhone } from "../utils/phone.js";
import {
  AuditActions,
  AuditEntityTypes,
  createAuditLog,
} from "../services/auditLog.js";

function pick(obj, allowedKeys) {
  const out = {};
  for (const key of allowedKeys) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      out[key] = obj[key];
    }
  }
  return out;
}

function normalizeEmail(email) {
  return String(email || "").trim().toLowerCase();
}

function normalizePhone(phone) {
  return normalizeNigerianPhone(phone);
}

function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value || "").trim());
}

function generateOtp() {
  return String(Math.floor(100000 + Math.random() * 900000));
}

const OTP_TTL_MINUTES = 10;
const OTP_RESEND_COOLDOWN_MS = 60_000;

const PlannedContributionUnitTypes = ["revolving", "endwell", "festive"];

function normalizeContributionUnits(rawUnits, storedYear, currentYear) {
  const base = {
    revolving: null,
    endwell: null,
    festive: null,
  };
  if (storedYear !== currentYear) return base;
  if (typeof rawUnits === "number" || typeof rawUnits === "string") {
    const num = Number(rawUnits);
    if (Number.isFinite(num)) base.revolving = num;
    return base;
  }
  if (!rawUnits || typeof rawUnits !== "object") return base;
  PlannedContributionUnitTypes.forEach((key) => {
    const value = rawUnits[key];
    if (value === null) {
      base[key] = null;
      return;
    }
    const num = Number(value);
    if (Number.isFinite(num)) {
      base[key] = num;
    }
  });
  return base;
}

function resolveContributionSettings(profile, now = new Date()) {
  const currentYear = now.getFullYear();
  const stored = profile?.contributionSettings || {};
  const storedYear = Number(stored.year);
  const units = normalizeContributionUnits(
    stored?.units,
    storedYear,
    currentYear,
  );
  const windowStatus = getContributionSettingsWindowStatus(now);
  return {
    year: currentYear,
    units,
    updatedAt: stored?.updatedAt ?? null,
    canEdit: windowStatus.isOpen,
    window: {
      startMonth: windowStatus.startMonth,
      endMonth: windowStatus.endMonth,
    },
  };
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
        role: pickPrimaryRole(normalizeUserRoles(req.user)),
        roles: normalizeUserRoles(req.user),
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

  if (Object.prototype.hasOwnProperty.call(updates, "nextOfKinPhone")) {
    const rawPhone = updates.nextOfKinPhone;
    if (rawPhone === null || rawPhone === undefined || rawPhone === "") {
      updates.nextOfKinPhone = null;
    } else {
      const normalized = normalizePhone(rawPhone);
      if (!normalized) {
        return next(new AppError("Provide a valid phone number", 400));
      }
      updates.nextOfKinPhone = normalized;
    }
  }

  const profile = await ProfileModel.findByIdAndUpdate(
    req.user.profileId,
    updates,
    {
      new: true,
      runValidators: true,
    },
  );

  if (!profile) return next(new AppError("Profile not found", 404));

  return sendSuccess(res, {
    statusCode: 200,
    data: { profile },
  });
});

export const requestEmailChange = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const rawEmail = req.body?.email || req.body?.newEmail;
  if (!rawEmail) return next(new AppError("New email is required", 400));
  const newEmail = normalizeEmail(rawEmail);
  if (!isValidEmail(newEmail)) {
    return next(new AppError("Provide a valid email address", 400));
  }

  if (req.user.email && normalizeEmail(req.user.email) === newEmail) {
    return next(new AppError("New email matches current email", 400));
  }

  const existing = await UserModel.findOne({
    email: newEmail,
    _id: { $ne: req.user._id },
  });
  if (existing) {
    return next(new AppError("Email is already in use", 409));
  }

  const user = await UserModel.findById(req.user._id).select(
    "+emailChangeOtpSentAt",
  );
  if (!user) return next(new AppError("User not found", 404));

  const now = Date.now();
  const lastSent = user.emailChangeOtpSentAt
    ? new Date(user.emailChangeOtpSentAt).getTime()
    : 0;
  if (lastSent && now - lastSent < OTP_RESEND_COOLDOWN_MS) {
    const wait = Math.ceil(
      (OTP_RESEND_COOLDOWN_MS - (now - lastSent)) / 1000,
    );
    return next(
      new AppError(`Please wait ${wait}s before requesting a new code.`, 429),
    );
  }

  const otp = generateOtp();

  user.pendingEmail = newEmail;
  user.emailChangeOtpHash = sha256(otp);
  user.emailChangeOtpExpiresAt = new Date(now + OTP_TTL_MINUTES * 60 * 1000);
  user.emailChangeOtpSentAt = new Date(now);

  await user.save({ validateBeforeSave: false });

  await sendEmailOtp({
    toEmail: newEmail,
    otp,
    ttlMinutes: OTP_TTL_MINUTES,
    purpose: "email change",
  });

  return sendSuccess(res, {
    statusCode: 200,
    message: "Verification code sent to your new email.",
    data: {
      pendingEmail: newEmail,
      expiresInMinutes: OTP_TTL_MINUTES,
    },
  });
});

export const confirmEmailChange = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const rawEmail = req.body?.email || req.body?.newEmail;
  const otp = req.body?.otp;
  if (!rawEmail || !otp) {
    return next(new AppError("Email and OTP are required", 400));
  }

  const newEmail = normalizeEmail(rawEmail);
  if (!isValidEmail(newEmail)) {
    return next(new AppError("Provide a valid email address", 400));
  }

  const user = await UserModel.findById(req.user._id).select(
    "+emailChangeOtpHash +emailChangeOtpExpiresAt",
  );
  if (!user) return next(new AppError("User not found", 404));

  if (!user.pendingEmail || normalizeEmail(user.pendingEmail) !== newEmail) {
    return next(
      new AppError("No pending email change found for this address.", 400),
    );
  }

  if (!user.emailChangeOtpHash || !user.emailChangeOtpExpiresAt) {
    return next(
      new AppError(
        "No verification request found. Please request a new code.",
        400,
      ),
    );
  }

  if (user.emailChangeOtpExpiresAt.getTime() <= Date.now()) {
    return next(
      new AppError(
        "Verification code has expired. Please request a new code.",
        400,
      ),
    );
  }

  if (sha256(String(otp).trim()) !== user.emailChangeOtpHash) {
    return next(new AppError("Invalid verification code", 400));
  }

  const existing = await UserModel.findOne({
    email: newEmail,
    _id: { $ne: user._id },
  });
  if (existing) {
    return next(new AppError("Email is already in use", 409));
  }

  user.email = newEmail;
  user.emailVerifiedAt = new Date();
  user.pendingEmail = null;
  user.emailChangeOtpHash = null;
  user.emailChangeOtpExpiresAt = null;
  user.emailChangeOtpSentAt = null;

  await user.save({ validateBeforeSave: false });

  await ProfileModel.findByIdAndUpdate(
    user.profileId,
    { email: newEmail },
    { new: true, runValidators: true },
  );

  return sendSuccess(res, {
    statusCode: 200,
    message: "Email updated successfully.",
    data: {
      user: {
        id: user._id,
        email: user.email,
        phone: user.phone,
        role: pickPrimaryRole(normalizeUserRoles(user)),
        roles: normalizeUserRoles(user),
      },
    },
  });
});

export const requestPhoneChange = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const rawPhone = req.body?.phone || req.body?.newPhone;
  if (!rawPhone) return next(new AppError("New phone number is required", 400));
  const newPhone = normalizePhone(rawPhone);
  if (!newPhone) return next(new AppError("Provide a valid phone number", 400));

  if (req.user.phone && normalizePhone(req.user.phone) === newPhone) {
    return next(new AppError("New phone matches current phone number", 400));
  }

  const existing = await UserModel.findOne({
    phone: newPhone,
    _id: { $ne: req.user._id },
  });
  if (existing) {
    return next(new AppError("Phone number is already in use", 409));
  }

  const user = await UserModel.findById(req.user._id).select(
    "+phoneChangeOtpSentAt",
  );
  if (!user) return next(new AppError("User not found", 404));

  const now = Date.now();
  const lastSent = user.phoneChangeOtpSentAt
    ? new Date(user.phoneChangeOtpSentAt).getTime()
    : 0;
  if (lastSent && now - lastSent < OTP_RESEND_COOLDOWN_MS) {
    const wait = Math.ceil(
      (OTP_RESEND_COOLDOWN_MS - (now - lastSent)) / 1000,
    );
    return next(
      new AppError(`Please wait ${wait}s before requesting a new code.`, 429),
    );
  }

  const otp = generateOtp();

  user.pendingPhone = newPhone;
  user.phoneChangeOtpHash = sha256(otp);
  user.phoneChangeOtpExpiresAt = new Date(now + OTP_TTL_MINUTES * 60 * 1000);
  user.phoneChangeOtpSentAt = new Date(now);

  await user.save({ validateBeforeSave: false });

  await sendPhoneOtp({ toPhone: newPhone, otp, ttlMinutes: OTP_TTL_MINUTES });

  return sendSuccess(res, {
    statusCode: 200,
    message: "Verification code sent to your new phone number.",
    data: {
      pendingPhone: newPhone,
      expiresInMinutes: OTP_TTL_MINUTES,
    },
  });
});

export const confirmPhoneChange = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const rawPhone = req.body?.phone || req.body?.newPhone;
  const otp = req.body?.otp;
  if (!rawPhone || !otp) {
    return next(new AppError("Phone and OTP are required", 400));
  }

  const newPhone = normalizePhone(rawPhone);
  if (!newPhone) return next(new AppError("Provide a valid phone number", 400));

  const user = await UserModel.findById(req.user._id).select(
    "+phoneChangeOtpHash +phoneChangeOtpExpiresAt",
  );
  if (!user) return next(new AppError("User not found", 404));

  if (!user.pendingPhone || normalizePhone(user.pendingPhone) !== newPhone) {
    return next(
      new AppError("No pending phone change found for this number.", 400),
    );
  }

  if (!user.phoneChangeOtpHash || !user.phoneChangeOtpExpiresAt) {
    return next(
      new AppError(
        "No verification request found. Please request a new code.",
        400,
      ),
    );
  }

  if (user.phoneChangeOtpExpiresAt.getTime() <= Date.now()) {
    return next(
      new AppError(
        "Verification code has expired. Please request a new code.",
        400,
      ),
    );
  }

  if (sha256(String(otp).trim()) !== user.phoneChangeOtpHash) {
    return next(new AppError("Invalid verification code", 400));
  }

  const existing = await UserModel.findOne({
    phone: newPhone,
    _id: { $ne: user._id },
  });
  if (existing) {
    return next(new AppError("Phone number is already in use", 409));
  }

  user.phone = newPhone;
  user.phoneVerifiedAt = new Date();
  user.pendingPhone = null;
  user.phoneChangeOtpHash = null;
  user.phoneChangeOtpExpiresAt = null;
  user.phoneChangeOtpSentAt = null;

  await user.save({ validateBeforeSave: false });

  await ProfileModel.findByIdAndUpdate(
    user.profileId,
    { phone: newPhone },
    { new: true, runValidators: true },
  );

  return sendSuccess(res, {
    statusCode: 200,
    message: "Phone number updated successfully.",
    data: {
      user: {
        id: user._id,
        email: user.email,
        phone: user.phone,
        role: pickPrimaryRole(normalizeUserRoles(user)),
        roles: normalizeUserRoles(user),
      },
    },
  });
});

export const getMyContributionSettings = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const profile = await ProfileModel.findById(req.user.profileId);
  if (!profile) return next(new AppError("Profile not found", 404));

  const settings = resolveContributionSettings(profile, new Date());

  return sendSuccess(res, {
    statusCode: 200,
    data: { settings },
  });
});

export const updateMyContributionSettings = catchAsync(
  async (req, res, next) => {
    if (!req.user) return next(new AppError("Not authenticated", 401));
    if (!req.user.profileId)
      return next(new AppError("User profile not found", 400));

    const rawUnits = req.body?.units;
    const now = new Date();
    const currentYear = now.getFullYear();
    const year = Number(req.body?.year ?? currentYear);
    const windowStatus = getContributionSettingsWindowStatus(now);

    const parsedUnits = {};
    if (rawUnits === null || typeof rawUnits === "undefined") {
      return next(new AppError("units must be provided", 400));
    }

    if (typeof rawUnits === "number" || typeof rawUnits === "string") {
      const num = Number(rawUnits);
      if (!Number.isFinite(num)) {
        return next(new AppError("units must be a valid number", 400));
      }
      if (num < 5 || num % 5 !== 0) {
        return next(
          new AppError("units must be at least 5 and in multiples of 5", 400),
        );
      }
      parsedUnits.revolving = num;
    } else if (rawUnits && typeof rawUnits === "object") {
      let hasAny = false;
      for (const key of PlannedContributionUnitTypes) {
        if (!Object.prototype.hasOwnProperty.call(rawUnits, key)) continue;
        hasAny = true;
        const value = rawUnits[key];
        if (value === null) {
          parsedUnits[key] = null;
          continue;
        }
        const num = Number(value);
        if (!Number.isFinite(num)) {
          return next(new AppError(`${key} units must be a valid number`, 400));
        }
        if (num < 5 || num % 5 !== 0) {
          return next(
            new AppError(
              `${key} units must be at least 5 and in multiples of 5`,
              400,
            ),
          );
        }
        parsedUnits[key] = num;
      }
      if (!hasAny) {
        return next(
          new AppError(
            "At least one contribution unit value must be provided",
            400,
          ),
        );
      }
    } else {
      return next(new AppError("units must be a valid object", 400));
    }
    if (!Number.isFinite(year) || year !== currentYear) {
      return next(
        new AppError(
          "Contribution settings are only for the current year",
          400,
        ),
      );
    }
    if (!windowStatus.isOpen) {
      return next(
        new AppError(
          "Contribution settings can only be updated between January and February",
          400,
        ),
      );
    }

    const profile = await ProfileModel.findById(req.user.profileId);
    if (!profile) return next(new AppError("Profile not found", 404));

    const existing = profile?.contributionSettings || {};
    const storedYear = Number(existing.year);
    const currentUnits = normalizeContributionUnits(
      existing?.units,
      storedYear,
      currentYear,
    );
    const nextUnits = {
      ...currentUnits,
      ...parsedUnits,
    };

    const updatedContributionSettings = {
      year: currentYear,
      units: nextUnits,
      updatedAt: now,
    };

    // Persist only the contribution settings slice so unrelated legacy fields
    // like phone numbers cannot block this update path.
    const updatedProfile = await ProfileModel.findByIdAndUpdate(
      profile._id,
      {
        $set: {
          contributionSettings: updatedContributionSettings,
        },
      },
      {
        new: true,
        runValidators: true,
      },
    );
    if (!updatedProfile) return next(new AppError("Profile not found", 404));

    const settings = resolveContributionSettings(updatedProfile, now);

    return sendSuccess(res, {
      statusCode: 200,
      data: { settings },
    });
  },
);

export const listUsers = catchAsync(async (req, res) => {
  const filter = {};
  if (req.query?.role) {
    const normalized = coerceUserRoles(req.query.role);
    if (normalized.length > 0) {
      filter.$or = [{ role: normalized[0] }, { roles: normalized[0] }];
    }
  }

  const users = await UserModel.find(filter).sort({ createdAt: -1 }).limit(200);

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
        role: pickPrimaryRole(normalizeUserRoles(u)),
        roles: normalizeUserRoles(u),
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
  const { role, roles } = req.body || {};
  const hasRolesArray = Array.isArray(roles);
  const rolesPayload = hasRolesArray ? roles : role;
  const normalized = coerceUserRoles(rolesPayload);
  if (normalized.length === 0) {
    return next(
      new AppError(`Invalid role(s). Allowed: ${UserRoles.join(", ")}`, 400),
    );
  }

  const session = await mongoose.startSession();
  let responseUser = null;

  try {
    await session.withTransaction(async () => {
      const user = await UserModel.findById(id).session(session);
      if (!user) {
        throw new AppError("User not found", 404);
      }

      const beforeRoles = [...normalizeUserRoles(user)].sort();
      const beforePrimaryRole = pickPrimaryRole(beforeRoles);
      const finalRoles = hasRolesArray
        ? Array.from(new Set(normalized))
        : Array.from(new Set([...beforeRoles, ...normalized]));

      user.roles = finalRoles;
      user.role = pickPrimaryRole(finalRoles);
      await user.save({ session, validateBeforeSave: false });

      const afterRoles = [...normalizeUserRoles(user)].sort();
      const afterPrimaryRole = pickPrimaryRole(afterRoles);
      const addedRoles = afterRoles.filter((entry) => !beforeRoles.includes(entry));
      const removedRoles = beforeRoles.filter((entry) => !afterRoles.includes(entry));
      const promotedToAdmin =
        !beforeRoles.includes("admin") && afterRoles.includes("admin");

      if (
        promotedToAdmin ||
        addedRoles.length > 0 ||
        removedRoles.length > 0 ||
        beforePrimaryRole !== afterPrimaryRole
      ) {
        await createAuditLog(
          {
            req,
            action: promotedToAdmin
              ? AuditActions.ADMIN_USER_PROMOTE_ADMIN
              : AuditActions.ADMIN_USER_ROLE_UPDATE,
            entityType: AuditEntityTypes.USER,
            entityId: user._id,
            targetUserId: user._id,
            targetProfileId: user.profileId,
            summary: promotedToAdmin
              ? `Granted admin access to ${user.email || user.phone || String(user._id)}.`
              : `Updated roles for ${user.email || user.phone || String(user._id)}.`,
            metadata: {
              requestedRole: hasRolesArray ? null : role ?? null,
              requestedRoles: hasRolesArray ? normalized : null,
              beforeRoles,
              afterRoles,
              addedRoles,
              removedRoles,
              beforePrimaryRole,
              afterPrimaryRole,
            },
          },
          session,
        );
      }

      responseUser = {
        id: user._id,
        email: user.email,
        phone: user.phone,
        role: afterPrimaryRole,
        roles: afterRoles,
        profileId: user.profileId,
        createdAt: user.createdAt,
        updatedAt: user.updatedAt,
      };
    });
  } finally {
    await session.endSession();
  }

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      user: responseUser,
    },
  });
});

export const listMyGroups = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const memberships = await GroupMembershipModel.find({
    userId: req.user.profileId,
    status: "active",
  })
    .sort({ joinedAt: -1 })
    .populate("groupId")
    .lean();

  const profile = await ProfileModel.findById(req.user.profileId, {
    contributionSettings: 1,
  }).lean();
  const currentYear = new Date().getFullYear();

  const enriched = memberships.map((membership) => {
    const group =
      membership && typeof membership.groupId === "object"
        ? membership.groupId
        : null;
    const expectedMonthlyContribution = resolveExpectedContributionAmount({
      settings: profile?.contributionSettings,
      year: currentYear,
      groupMonthlyContribution: group?.monthlyContribution,
      type: "revolving",
    });

    return {
      ...membership,
      expectedMonthlyContribution,
    };
  });

  return sendSuccess(res, {
    statusCode: 200,
    results: enriched.length,
    data: { memberships: enriched },
  });
});
