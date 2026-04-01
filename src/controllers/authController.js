import jwt from "jsonwebtoken";
import speakeasy from "speakeasy";
import qrcode from "qrcode";
import { promisify } from "node:util";

import { UserModel } from "../models/User.js";
import { ProfileModel } from "../models/Profile.js";
import { PhoneOtpSessionModel } from "../models/PhoneOtpSession.js";
import { RefreshTokenModel } from "../models/RefreshToken.js";
import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { LoginHistoryModel } from "../models/LoginHistory.js";
import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import {
  decryptSecret,
  encryptSecret,
  randomId,
  sha256,
} from "../utils/crypto.js";
import {
  signAccessToken,
  signRefreshToken,
  signTwoFactorToken,
  verifyTwoFactorToken,
} from "../utils/tokens.js";
import { sendEmailVerification } from "../services/mail/sendEmailVerification.js";
import { sendPasswordResetEmail } from "../services/mail/sendPasswordResetEmail.js";
import { sendPhoneOtp } from "../services/sms/sendPhoneOtp.js";
import { sendPasswordResetOtp } from "../services/sms/sendPasswordResetOtp.js";

function getAccessSecret() {
  return process.env.JWT_ACCESS_SECRET || process.env.JWT_SECRET;
}

function getRefreshSecret() {
  return process.env.JWT_REFRESH_SECRET;
}

function getPublicBaseUrl() {
  return (
    process.env.PUBLIC_BASE_URL ||
    process.env.APP_URL ||
    `http://localhost:${Number(process.env.PORT) || 4000}`
  );
}

function normalizeEmail(email) {
  return String(email).trim().toLowerCase();
}

function normalizePhone(phone) {
  return String(phone).trim().replace(/\s+/g, "");
}

function generateOtp() {
  return String(Math.floor(100000 + Math.random() * 900000));
}

async function sendEmailVerificationIfNeeded(user, req) {
  if (!user.email) throw new Error("User has no email");
  if (user.emailVerifiedAt) return;

  const now = Date.now();
  const lastSent = user.emailVerificationSentAt
    ? new Date(user.emailVerificationSentAt).getTime()
    : 0;

  if (lastSent && now - lastSent < 60_000) return;

  const token = randomId(32);
  user.emailVerificationTokenHash = sha256(token);
  user.emailVerificationExpiresAt = new Date(now + 30 * 60 * 1000);
  user.emailVerificationSentAt = new Date(now);

  await user.save({ validateBeforeSave: false });

  const clientBase = process.env.CLIENT_URL || getPublicBaseUrl();
  const verificationUrl = `${clientBase}/verify-email?token=${token}`;
  await sendEmailVerification({ toEmail: user.email, verificationUrl });
}

async function sendPhoneOtpIfNeeded(user, req) {
  if (!user.phone) throw new Error("User has no phone");
  if (user.phoneVerifiedAt) return;

  const now = Date.now();
  const lastSent = user.phoneOtpSentAt
    ? new Date(user.phoneOtpSentAt).getTime()
    : 0;

  if (lastSent && now - lastSent < 60_000) return;

  const otp = generateOtp();
  const ttlMinutes = 10;

  user.phoneOtpHash = sha256(otp);
  user.phoneOtpExpiresAt = new Date(now + ttlMinutes * 60 * 1000);
  user.phoneOtpSentAt = new Date(now);

  await user.save({ validateBeforeSave: false });

  await sendPhoneOtp({ toPhone: user.phone, otp, ttlMinutes });
}

async function buildAuthResponse(user, req) {
  await ensureActiveGroupMembership(user);
  const safeUser = {
    id: user._id,
    email: user.email,
    phone: user.phone,
    role: user.role,
    profileId: user.profileId,
    createdAt: user.createdAt,
    updatedAt: user.updatedAt,
  };

  const profile = await ProfileModel.findById(user.profileId);

  const accessToken = signAccessToken({
    userId: user._id.toString(),
    role: user.role,
  });

  const jti = randomId(16);
  const refreshToken = signRefreshToken({ userId: user._id.toString(), jti });

  const decoded = jwt.decode(refreshToken);
  const expiresAt = decoded?.exp
    ? new Date(decoded.exp * 1000)
    : new Date(Date.now() + 30 * 24 * 60 * 60 * 1000);

  await RefreshTokenModel.create({
    userId: user._id,
    jti,
    tokenHash: sha256(refreshToken),
    expiresAt,
    createdByIp: req.ip || null,
    userAgent: req.get("user-agent") || null,
  });

  return { accessToken, refreshToken, user: safeUser, profile };
}

async function ensureActiveGroupMembership(user) {
  if (!user) return;
  const role = String(user.role || "");
  if (role === "admin") return;

  if (!user.profileId) {
    throw new AppError(
      "You are not yet approved in any group. Please ask your group coordinator to add or approve you.",
      403,
    );
  }

  const activeMembership = await GroupMembershipModel.exists({
    userId: user.profileId,
    status: "active",
  });

  if (!activeMembership) {
    throw new AppError(
      "You are not yet approved in any group. Please ask your group coordinator to add or approve you.",
      403,
    );
  }
}

function resolveClientIp(req) {
  const forwarded = req.headers["x-forwarded-for"];
  if (typeof forwarded === "string" && forwarded.length > 0) {
    return forwarded.split(",")[0].trim();
  }
  return req.ip || null;
}

async function recordLoginHistory(user, req, method = "password") {
  try {
    await LoginHistoryModel.create({
      userId: user._id,
      method,
      success: true,
      ip: resolveClientIp(req),
      userAgent: req.get("user-agent") || null,
    });
  } catch (err) {
    // Don't block auth if audit logging fails
  }
}

async function enforceDeletionStatus(user) {
  if (!user) return;
  if (user.deletedAt) {
    throw new AppError(
      "Account has been deleted. Please contact support if this is a mistake.",
      403,
    );
  }

  if (
    user.deletionScheduledFor &&
    new Date(user.deletionScheduledFor).getTime() <= Date.now()
  ) {
    user.active = false;
    user.deletedAt = new Date();
    await user.save({ validateBeforeSave: false });
    throw new AppError(
      "Account has been deleted. Please contact support if this is a mistake.",
      403,
    );
  }
}

function verifyTwoFactorCodeOrThrow(user, token) {
  const secret = user?.twoFactorSecret
    ? decryptSecret(user.twoFactorSecret)
    : null;
  if (!secret) {
    throw new AppError("Two-factor authentication is not configured", 400);
  }

  const ok = speakeasy.totp.verify({
    secret,
    encoding: "base32",
    token: String(token || "").trim(),
    window: 1,
  });

  if (!ok) {
    throw new AppError("Invalid two-factor code", 401);
  }
}

function parseLoginIdentifier(body) {
  const { email, phone, loginId } = body || {};

  if (email) return { kind: "email", value: normalizeEmail(email) };
  if (phone) return { kind: "phone", value: normalizePhone(phone) };

  if (loginId) {
    const val = String(loginId).trim();
    if (val.includes("@")) return { kind: "email", value: normalizeEmail(val) };
    return { kind: "phone", value: normalizePhone(val) };
  }

  return null;
}

export const signup = catchAsync(async (req, res, next) => {
  const { password, fullName } = req.body || {};

  const email = req.body?.email ? normalizeEmail(req.body.email) : null;
  const phone = req.body?.phone ? normalizePhone(req.body.phone) : null;
  const requestedGroupId = req.body?.groupId
    ? String(req.body.groupId).trim()
    : null;

  let requestedGroup = null;
  if (requestedGroupId) {
    requestedGroup = await GroupModel.findById(requestedGroupId);
    if (!requestedGroup) {
      return next(new AppError("Selected group not found", 400));
    }
    if (!requestedGroup.isOpen) {
      return next(
        new AppError("Selected group is not open to new members", 400),
      );
    }
    if (requestedGroup.memberCount >= requestedGroup.maxMembers) {
      return next(new AppError("Selected group is full", 400));
    }
  }

  if (!email && !phone) {
    return next(new AppError("Provide email or phone to sign up", 400));
  }

  if (!password) return next(new AppError("Password is required", 400));
  if (typeof password !== "string" || password.length < 8) {
    return next(
      new AppError("Password must be at least 8 characters long", 400),
    );
  }

  if (email) {
    const existingEmail = await UserModel.findOne({ email });
    if (existingEmail) {
      return next(
        new AppError("An account with this login ID already exists", 409),
      );
    }
  }

  if (phone) {
    const existingPhone = await UserModel.findOne({ phone });
    if (existingPhone) {
      return next(
        new AppError("An account with this login ID already exists", 409),
      );
    }
  }

  const profile = await ProfileModel.create({
    email,
    phone,
    fullName: fullName ?? null,
    membershipStatus: "pending",
    avatar: null,
  });

  const user = await UserModel.create({
    email,
    phone,
    password,
    profileId: profile._id,
    role: "member",
  });

  if (requestedGroup) {
    await GroupMembershipModel.findOneAndUpdate(
      { groupId: requestedGroup._id, userId: profile._id },
      {
        groupId: requestedGroup._id,
        userId: profile._id,
        status: "pending",
        requestedAt: new Date(),
        reviewedAt: null,
        reviewedBy: null,
        reviewNotes: null,
      },
      {
        upsert: true,
        new: true,
        runValidators: true,
        setDefaultsOnInsert: true,
      },
    );
  }

  if (user.email) await sendEmailVerificationIfNeeded(user, req);
  if (!email && user.phone) await sendPhoneOtpIfNeeded(user, req);

  let message = "Signup successful.";
  if (user.email && user.phone) {
    message = "Signup successful. Please verify your email and phone number.";
  } else if (user.email) {
    message =
      "Signup successful. Please check your email to verify your account.";
  } else if (user.phone) {
    message =
      "Signup successful. Please verify your phone number with the OTP sent.";
  }

  return sendSuccess(res, {
    statusCode: 201,
    message,
    data: {
      user: {
        id: user._id,
        email: user.email,
        phone: user.phone,
        role: user.role,
        profileId: user.profileId,
      },
    },
  });
});

export const verifyEmail = catchAsync(async (req, res, next) => {
  const token = req.query?.token;
  const autoLogin = String(req.query?.autoLogin || "").toLowerCase() === "true";

  if (!token || typeof token !== "string") {
    return next(new AppError("Missing token", 400));
  }

  const tokenHash = sha256(token);

  const user = await UserModel.findOne({
    emailVerificationTokenHash: tokenHash,
    emailVerificationExpiresAt: { $gt: new Date() },
  });

  if (!user) {
    return next(
      new AppError("Verification token is invalid or has expired", 400),
    );
  }

  user.emailVerifiedAt = new Date();
  user.emailVerificationTokenHash = null;
  user.emailVerificationExpiresAt = null;

  await user.save({ validateBeforeSave: false });

  if (!autoLogin) {
    return sendSuccess(res, {
      statusCode: 200,
      message: "Email verified successfully. You can now log in.",
    });
  }

  const data = await buildAuthResponse(user, req);

  return sendSuccess(res, {
    statusCode: 200,
    message: "Email verified successfully.",
    accessToken: data.accessToken,
    refreshToken: data.refreshToken,
    data: { user: data.user, profile: data.profile },
  });
});

export const verifyPhone = catchAsync(async (req, res, next) => {
  const { phone, otp } = req.body || {};
  const autoLogin = Boolean(req.body?.autoLogin);

  if (!phone || !otp) {
    return next(new AppError("Phone and OTP are required", 400));
  }

  const normalizedPhone = normalizePhone(phone);
  const user = await UserModel.findOne({ phone: normalizedPhone }).select(
    "+phoneOtpHash +phoneOtpExpiresAt",
  );

  if (!user) return next(new AppError("Invalid phone or OTP", 400));

  if (!user.phoneOtpHash || !user.phoneOtpExpiresAt) {
    return next(
      new AppError("No OTP request found. Please request a new OTP.", 400),
    );
  }

  if (user.phoneOtpExpiresAt.getTime() <= Date.now()) {
    return next(
      new AppError("OTP has expired. Please request a new OTP.", 400),
    );
  }

  if (sha256(String(otp).trim()) !== user.phoneOtpHash) {
    return next(new AppError("Invalid phone or OTP", 400));
  }

  user.phoneVerifiedAt = new Date();
  user.phoneOtpHash = null;
  user.phoneOtpExpiresAt = null;

  await user.save({ validateBeforeSave: false });

  if (!autoLogin) {
    return sendSuccess(res, {
      statusCode: 200,
      message: "Phone verified successfully. You can now log in.",
    });
  }

  const data = await buildAuthResponse(user, req);

  return sendSuccess(res, {
    statusCode: 200,
    message: "Phone verified successfully.",
    accessToken: data.accessToken,
    refreshToken: data.refreshToken,
    data: { user: data.user, profile: data.profile },
  });
});

export const resendVerification = catchAsync(async (req, res, next) => {
  const id = parseLoginIdentifier(req.body);
  if (!id) return next(new AppError("Provide email or phone", 400));

  const user =
    id.kind === "email"
      ? await UserModel.findOne({ email: id.value }).select(
          "+emailVerificationSentAt",
        )
      : await UserModel.findOne({ phone: id.value }).select("+phoneOtpSentAt");

  if (!user) {
    return sendSuccess(res, {
      statusCode: 200,
      message:
        id.kind === "email"
          ? "If an account exists, a verification email has been sent."
          : "If an account exists, an OTP has been sent.",
    });
  }

  if (id.kind === "email") {
    if (user.emailVerifiedAt) {
      return sendSuccess(res, {
        statusCode: 200,
        message: "Email already verified.",
      });
    }

    await sendEmailVerificationIfNeeded(user, req);

    return sendSuccess(res, {
      statusCode: 200,
      message: "Verification email sent.",
    });
  }

  if (user.phoneVerifiedAt) {
    return sendSuccess(res, {
      statusCode: 200,
      message: "Phone already verified.",
    });
  }

  await sendPhoneOtpIfNeeded(user, req);

  return sendSuccess(res, { statusCode: 200, message: "OTP sent." });
});

export const sendPhoneOtpLogin = catchAsync(async (req, res, next) => {
  const { phone, fullName, groupId } = req.body || {};

  if (!phone) return next(new AppError("Phone is required", 400));

  const normalizedPhone = normalizePhone(phone);
  const requestedGroupId = groupId ? String(groupId).trim() : null;
  let requestedGroup = null;
  if (requestedGroupId) {
    requestedGroup = await GroupModel.findById(requestedGroupId);
    if (!requestedGroup) {
      return next(new AppError("Selected group not found", 400));
    }
    if (!requestedGroup.isOpen) {
      return next(
        new AppError("Selected group is not open to new members", 400),
      );
    }
    if (requestedGroup.memberCount >= requestedGroup.maxMembers) {
      return next(new AppError("Selected group is full", 400));
    }
  }
  const otp = generateOtp();
  const ttlMinutes = 10;
  const now = Date.now();

  const pinId = randomId(16);

  await PhoneOtpSessionModel.create({
    pinId,
    phone: normalizedPhone,
    fullName: fullName ? String(fullName).trim() : null,
    groupId: requestedGroup?._id ?? null,
    otpHash: sha256(otp),
    expiresAt: new Date(now + ttlMinutes * 60 * 1000),
    sentAt: new Date(now),
    attempts: 0,
    consumedAt: null,
    ip: req.ip || null,
    userAgent: req.get("user-agent") || null,
  });

  await sendPhoneOtp({ toPhone: normalizedPhone, otp, ttlMinutes });

  return sendSuccess(res, {
    statusCode: 200,
    message: "OTP sent.",
    data: { pinId },
  });
});

export const verifyPhoneOtpLogin = catchAsync(async (req, res, next) => {
  const { pinId, otp } = req.body || {};

  if (!pinId || !otp) {
    return next(new AppError("pinId and otp are required", 400));
  }

  const session = await PhoneOtpSessionModel.findOne({ pinId }).select(
    "+otpHash",
  );

  if (!session || session.consumedAt) {
    return next(
      new AppError("Session expired. Please request a new OTP.", 400),
    );
  }

  if (session.expiresAt.getTime() <= Date.now()) {
    return next(
      new AppError("OTP has expired. Please request a new OTP.", 400),
    );
  }

  const otpHash = sha256(String(otp).trim());
  if (otpHash !== session.otpHash) {
    session.attempts = Number(session.attempts || 0) + 1;
    if (session.attempts >= 5) session.consumedAt = new Date();
    await session.save({ validateBeforeSave: false });
    return next(new AppError("Invalid OTP", 400));
  }

  session.consumedAt = new Date();
  await session.save({ validateBeforeSave: false });

  const phone = session.phone;

  let user = await UserModel.findOne({ phone });
  let isNewUser = false;

  if (!user) {
    isNewUser = true;

    const profile = await ProfileModel.create({
      email: null,
      phone,
      fullName: session.fullName ?? null,
      membershipStatus: "pending",
      avatar: null,
    });

    user = await UserModel.create({
      email: null,
      phone,
      password: randomId(32),
      profileId: profile._id,
      role: "member",
      phoneVerifiedAt: new Date(),
    });
  } else if (!user.phoneVerifiedAt) {
    user.phoneVerifiedAt = new Date();
    await user.save({ validateBeforeSave: false });
  }

  if (session.groupId) {
    const group = await GroupModel.findById(session.groupId);
    if (!group) {
      return next(new AppError("Selected group not found", 400));
    }
    if (!group.isOpen) {
      return next(
        new AppError("Selected group is not open to new members", 400),
      );
    }
    if (group.memberCount >= group.maxMembers) {
      return next(new AppError("Selected group is full", 400));
    }

    const existingMembership = await GroupMembershipModel.findOne({
      groupId: group._id,
      userId: user.profileId,
    }).lean();

    if (!existingMembership || existingMembership.status !== "active") {
      await GroupMembershipModel.findOneAndUpdate(
        { groupId: group._id, userId: user.profileId },
        {
          groupId: group._id,
          userId: user.profileId,
          status: "pending",
          requestedAt: new Date(),
          reviewedAt: null,
          reviewedBy: null,
          reviewNotes: null,
        },
        {
          upsert: true,
          new: true,
          runValidators: true,
          setDefaultsOnInsert: true,
        },
      );
    }
  }

  await enforceDeletionStatus(user);
  await ensureActiveGroupMembership(user);

  if (user.twoFactorEnabled) {
    const twoFactorToken = signTwoFactorToken({
      userId: user._id.toString(),
    });
    return sendSuccess(res, {
      statusCode: 200,
      message: "Two-factor authentication required.",
      twoFactorRequired: true,
      twoFactorToken,
      data: { isNewUser },
    });
  }

  const data = await buildAuthResponse(user, req);
  await recordLoginHistory(user, req, "phone_otp");

  return sendSuccess(res, {
    statusCode: 200,
    message: "Phone verified successfully.",
    accessToken: data.accessToken,
    refreshToken: data.refreshToken,
    data: { isNewUser, user: data.user, profile: data.profile },
  });
});

export const login = catchAsync(async (req, res, next) => {
  const { password } = req.body || {};
  const id = parseLoginIdentifier(req.body);

  if (!id || !password) {
    return next(new AppError("Provide email/phone and password", 400));
  }

  const user =
    id.kind === "email"
      ? await UserModel.findOne({ email: id.value }).select(
          "+password +active +passwordChangedAt +emailVerificationSentAt",
        )
      : await UserModel.findOne({ phone: id.value }).select(
          "+password +active +passwordChangedAt +phoneOtpSentAt",
        );

  if (!user) {
    return next(new AppError("Invalid login credentials", 401));
  }

  await enforceDeletionStatus(user);

  if (user.active === false) {
    return next(new AppError("Invalid login credentials", 401));
  }

  const ok = await user.correctPassword(String(password));
  if (!ok) {
    return next(new AppError("Invalid login credentials", 401));
  }

  if (id.kind === "email" && !user.emailVerifiedAt) {
    await sendEmailVerificationIfNeeded(user, req);
    return next(
      new AppError("Email not verified. Verification email sent.", 401),
    );
  }

  if (id.kind === "phone" && !user.phoneVerifiedAt) {
    await sendPhoneOtpIfNeeded(user, req);
    return next(new AppError("Phone not verified. OTP sent.", 401));
  }

  await ensureActiveGroupMembership(user);

  if (user.twoFactorEnabled) {
    const twoFactorToken = signTwoFactorToken({
      userId: user._id.toString(),
    });
    return sendSuccess(res, {
      statusCode: 200,
      message: "Two-factor authentication required.",
      twoFactorRequired: true,
      twoFactorToken,
    });
  }

  const data = await buildAuthResponse(user, req);
  await recordLoginHistory(user, req, "password");

  return sendSuccess(res, {
    statusCode: 200,
    accessToken: data.accessToken,
    refreshToken: data.refreshToken,
    data: { user: data.user, profile: data.profile },
  });
});

export const refresh = catchAsync(async (req, res, next) => {
  const refreshToken =
    req.body?.refreshToken ||
    req.headers["x-refresh-token"] ||
    req.headers["x-refresh"];

  if (!refreshToken || typeof refreshToken !== "string") {
    return next(new AppError("Missing refreshToken", 400));
  }

  const secret = getRefreshSecret();
  if (!secret) return next(new AppError("Server auth misconfiguration", 500));

  const decoded = await promisify(jwt.verify)(refreshToken, secret);

  if (decoded?.type !== "refresh" || !decoded?.id || !decoded?.jti) {
    return next(new AppError("Invalid refresh token", 401));
  }

  const tokenDoc = await RefreshTokenModel.findOne({
    userId: decoded.id,
    jti: decoded.jti,
  }).select("+tokenHash");

  if (!tokenDoc) return next(new AppError("Refresh token is invalid", 401));
  if (tokenDoc.revokedAt)
    return next(new AppError("Refresh token has been revoked", 401));
  if (tokenDoc.expiresAt.getTime() <= Date.now()) {
    return next(new AppError("Refresh token has expired", 401));
  }

  const incomingHash = sha256(refreshToken);
  if (tokenDoc.tokenHash !== incomingHash) {
    await RefreshTokenModel.updateMany(
      { userId: decoded.id, revokedAt: null },
      { revokedAt: new Date() },
    );

    return next(new AppError("Refresh token is invalid", 401));
  }

  const user = await UserModel.findById(decoded.id).select(
    "+active +passwordChangedAt",
  );

  if (!user) {
    return next(
      new AppError("The user belonging to this token no longer exists", 401),
    );
  }

  await enforceDeletionStatus(user);

  if (user.active === false) {
    return next(
      new AppError("The user belonging to this token no longer exists", 401),
    );
  }

  if (user.changedPasswordAfter(decoded.iat)) {
    await RefreshTokenModel.updateMany(
      { userId: decoded.id, revokedAt: null },
      { revokedAt: new Date() },
    );

    return next(
      new AppError("User recently changed password. Please log in again.", 401),
    );
  }

  await ensureActiveGroupMembership(user);

  const newJti = randomId(16);
  const newRefreshToken = signRefreshToken({
    userId: user._id.toString(),
    jti: newJti,
  });

  const newDecoded = jwt.decode(newRefreshToken);
  const newExpiresAt = newDecoded?.exp
    ? new Date(newDecoded.exp * 1000)
    : new Date(Date.now() + 30 * 24 * 60 * 60 * 1000);

  tokenDoc.revokedAt = new Date();
  tokenDoc.replacedByJti = newJti;
  await tokenDoc.save();

  await RefreshTokenModel.create({
    userId: user._id,
    jti: newJti,
    tokenHash: sha256(newRefreshToken),
    expiresAt: newExpiresAt,
    createdByIp: req.ip || null,
    userAgent: req.get("user-agent") || null,
  });

  const accessToken = signAccessToken({
    userId: user._id.toString(),
    role: user.role,
  });
  const profile = await ProfileModel.findById(user.profileId);

  return sendSuccess(res, {
    statusCode: 200,
    accessToken,
    refreshToken: newRefreshToken,
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
      profile,
    },
  });
});

export const logout = catchAsync(async (req, res, next) => {
  const refreshToken =
    req.body?.refreshToken ||
    req.headers["x-refresh-token"] ||
    req.headers["x-refresh"];

  if (!refreshToken || typeof refreshToken !== "string") {
    return next(new AppError("Missing refreshToken", 400));
  }

  const secret = getRefreshSecret();
  if (!secret) return next(new AppError("Server auth misconfiguration", 500));

  let decoded;
  try {
    decoded = await promisify(jwt.verify)(refreshToken, secret);
  } catch {
    decoded = null;
  }

  if (decoded?.jti && decoded?.id) {
    await RefreshTokenModel.updateOne(
      { userId: decoded.id, jti: decoded.jti, revokedAt: null },
      { revokedAt: new Date() },
    );
  }

  return sendSuccess(res, { statusCode: 200, message: "Logged out" });
});

export const changePassword = catchAsync(async (req, res, next) => {
  const { currentPassword, newPassword } = req.body || {};

  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!currentPassword || !newPassword) {
    return next(
      new AppError("currentPassword and newPassword are required", 400),
    );
  }

  if (typeof newPassword !== "string" || newPassword.length < 8) {
    return next(
      new AppError("Password must be at least 8 characters long", 400),
    );
  }

  const user = await UserModel.findById(req.user._id).select("+password");
  if (!user) return next(new AppError("User not found", 404));

  const ok = await user.correctPassword(String(currentPassword));
  if (!ok) return next(new AppError("Current password is incorrect", 401));

  user.password = String(newPassword);
  await user.save();

  await RefreshTokenModel.updateMany(
    { userId: user._id, revokedAt: null },
    { revokedAt: new Date() },
  );

  const data = await buildAuthResponse(user, req);

  return sendSuccess(res, {
    statusCode: 200,
    message: "Password changed successfully.",
    accessToken: data.accessToken,
    refreshToken: data.refreshToken,
    data: { user: data.user, profile: data.profile },
  });
});

export const forgotPassword = catchAsync(async (req, res, next) => {
  const id = parseLoginIdentifier(req.body);
  if (!id) return next(new AppError("Provide email or phone", 400));

  const user =
    id.kind === "email"
      ? await UserModel.findOne({ email: id.value }).select(
          "+passwordResetSentAt",
        )
      : await UserModel.findOne({ phone: id.value }).select(
          "+passwordResetPhoneOtpSentAt",
        );

  if (!user) {
    return sendSuccess(res, {
      statusCode: 200,
      message:
        id.kind === "email"
          ? "If an account exists, a password reset link has been sent."
          : "If an account exists, a password reset OTP has been sent.",
    });
  }

  const now = Date.now();

  if (id.kind === "email") {
    const lastSent = user.passwordResetSentAt
      ? new Date(user.passwordResetSentAt).getTime()
      : 0;

    if (!lastSent || now - lastSent >= 60_000) {
      const token = randomId(32);
      user.passwordResetTokenHash = sha256(token);
      user.passwordResetExpiresAt = new Date(now + 30 * 60 * 1000);
      user.passwordResetSentAt = new Date(now);

      await user.save({ validateBeforeSave: false });

      const resetBase = process.env.CLIENT_URL || getPublicBaseUrl();
      const resetUrl = `${resetBase}/reset-password?token=${token}`;

      await sendPasswordResetEmail({ toEmail: user.email, resetUrl });
    }

    return sendSuccess(res, {
      statusCode: 200,
      message: "If an account exists, a password reset link has been sent.",
    });
  }

  const lastSent = user.passwordResetPhoneOtpSentAt
    ? new Date(user.passwordResetPhoneOtpSentAt).getTime()
    : 0;

  if (!lastSent || now - lastSent >= 60_000) {
    const otp = generateOtp();
    const ttlMinutes = 10;

    user.passwordResetPhoneOtpHash = sha256(otp);
    user.passwordResetPhoneOtpExpiresAt = new Date(
      now + ttlMinutes * 60 * 1000,
    );
    user.passwordResetPhoneOtpSentAt = new Date(now);

    await user.save({ validateBeforeSave: false });

    await sendPasswordResetOtp({ toPhone: user.phone, otp, ttlMinutes });
  }

  return sendSuccess(res, {
    statusCode: 200,
    message: "If an account exists, a password reset OTP has been sent.",
  });
});

export const resetPassword = catchAsync(async (req, res, next) => {
  const token = req.body?.token || req.query?.token;
  const { phone, otp, password, autoLogin } = req.body || {};

  if (!password) return next(new AppError("Password is required", 400));
  if (typeof password !== "string" || password.length < 8) {
    return next(
      new AppError("Password must be at least 8 characters long", 400),
    );
  }

  let user = null;

  if (token) {
    const tokenHash = sha256(String(token));
    user = await UserModel.findOne({
      passwordResetTokenHash: tokenHash,
      passwordResetExpiresAt: { $gt: new Date() },
    });

    if (!user)
      return next(new AppError("Reset token is invalid or has expired", 400));

    user.passwordResetTokenHash = null;
    user.passwordResetExpiresAt = null;
  } else if (phone && otp) {
    const normalizedPhone = normalizePhone(phone);

    user = await UserModel.findOne({ phone: normalizedPhone }).select(
      "+passwordResetPhoneOtpHash +passwordResetPhoneOtpExpiresAt",
    );

    if (!user) return next(new AppError("Invalid phone or OTP", 400));

    if (
      !user.passwordResetPhoneOtpHash ||
      !user.passwordResetPhoneOtpExpiresAt ||
      user.passwordResetPhoneOtpExpiresAt.getTime() <= Date.now() ||
      sha256(String(otp).trim()) !== user.passwordResetPhoneOtpHash
    ) {
      return next(new AppError("Invalid phone or OTP", 400));
    }

    user.passwordResetPhoneOtpHash = null;
    user.passwordResetPhoneOtpExpiresAt = null;
  } else {
    return next(new AppError("Provide either token (email) or phone+otp", 400));
  }

  user.password = String(password);
  await user.save();

  await RefreshTokenModel.updateMany(
    { userId: user._id, revokedAt: null },
    { revokedAt: new Date() },
  );

  if (!autoLogin) {
    return sendSuccess(res, {
      statusCode: 200,
      message: "Password reset successfully. You can now log in.",
    });
  }

  const data = await buildAuthResponse(user, req);

  return sendSuccess(res, {
    statusCode: 200,
    message: "Password reset successfully.",
    accessToken: data.accessToken,
    refreshToken: data.refreshToken,
    data: { user: data.user, profile: data.profile },
  });
});

export const getTwoFactorStatus = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const user = await UserModel.findById(req.user._id).select(
    "+twoFactorSecret",
  );
  if (!user) return next(new AppError("User not found", 404));

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      enabled: Boolean(user.twoFactorEnabled),
      pendingSetup: Boolean(user.twoFactorSecret && !user.twoFactorEnabled),
      enabledAt: user.twoFactorEnabledAt,
    },
  });
});

export const setupTwoFactor = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const user = await UserModel.findById(req.user._id).select(
    "+twoFactorSecret",
  );
  if (!user) return next(new AppError("User not found", 404));

  if (user.twoFactorEnabled) {
    return next(
      new AppError("Two-factor authentication is already enabled", 400),
    );
  }

  const label = user.email || user.phone || "CRC Connect";
  const secret = speakeasy.generateSecret({
    name: `CRC Connect (${label})`,
  });

  user.twoFactorSecret = encryptSecret(secret.base32);
  user.twoFactorEnabled = false;
  user.twoFactorEnabledAt = null;
  await user.save({ validateBeforeSave: false });

  const qrCodeDataUrl = secret.otpauth_url
    ? await qrcode.toDataURL(secret.otpauth_url)
    : null;

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      secret: secret.base32,
      otpauthUrl: secret.otpauth_url,
      qrCodeDataUrl,
    },
  });
});

export const enableTwoFactor = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const { code } = req.body || {};
  if (!code) return next(new AppError("Two-factor code is required", 400));

  const user = await UserModel.findById(req.user._id).select(
    "+twoFactorSecret",
  );
  if (!user) return next(new AppError("User not found", 404));

  if (!user.twoFactorSecret) {
    return next(new AppError("Two-factor setup has not been initialized", 400));
  }

  verifyTwoFactorCodeOrThrow(user, code);

  user.twoFactorEnabled = true;
  user.twoFactorEnabledAt = new Date();
  await user.save({ validateBeforeSave: false });

  return sendSuccess(res, {
    statusCode: 200,
    message: "Two-factor authentication enabled successfully.",
    data: {
      enabled: true,
      enabledAt: user.twoFactorEnabledAt,
    },
  });
});

export const disableTwoFactor = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const { password, code } = req.body || {};
  if (!password) return next(new AppError("Password is required", 400));
  if (!code) return next(new AppError("Two-factor code is required", 400));

  const user = await UserModel.findById(req.user._id).select(
    "+password +twoFactorSecret",
  );
  if (!user) return next(new AppError("User not found", 404));

  const ok = await user.correctPassword(String(password));
  if (!ok) return next(new AppError("Invalid password", 401));

  verifyTwoFactorCodeOrThrow(user, code);

  user.twoFactorEnabled = false;
  user.twoFactorSecret = null;
  user.twoFactorEnabledAt = null;
  await user.save({ validateBeforeSave: false });

  return sendSuccess(res, {
    statusCode: 200,
    message: "Two-factor authentication disabled.",
    data: { enabled: false },
  });
});

export const verifyTwoFactorLogin = catchAsync(async (req, res, next) => {
  const { token, code } = req.body || {};
  if (!token || !code) {
    return next(new AppError("Two-factor token and code are required", 400));
  }

  const decoded = verifyTwoFactorToken(String(token));
  if (decoded?.type !== "2fa" || !decoded?.id) {
    return next(new AppError("Invalid two-factor token", 401));
  }

  const user = await UserModel.findById(decoded.id).select(
    "+twoFactorSecret +active",
  );
  if (!user) return next(new AppError("User not found", 404));

  await enforceDeletionStatus(user);
  if (user.active === false) {
    return next(new AppError("Account is not active", 403));
  }

  if (!user.twoFactorEnabled) {
    return next(new AppError("Two-factor authentication is not enabled", 400));
  }

  verifyTwoFactorCodeOrThrow(user, code);
  await ensureActiveGroupMembership(user);

  const data = await buildAuthResponse(user, req);
  await recordLoginHistory(user, req, "two_factor");

  return sendSuccess(res, {
    statusCode: 200,
    accessToken: data.accessToken,
    refreshToken: data.refreshToken,
    data: { user: data.user, profile: data.profile },
  });
});

export const getLoginHistory = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const limit = Math.min(Number(req.query?.limit || 20), 100);
  const page = Math.max(Number(req.query?.page || 1), 1);
  const skip = (page - 1) * limit;

  const [items, total] = await Promise.all([
    LoginHistoryModel.find({ userId: req.user._id })
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limit)
      .lean(),
    LoginHistoryModel.countDocuments({ userId: req.user._id }),
  ]);

  return sendSuccess(res, {
    statusCode: 200,
    data: { history: items },
    total,
    page,
    limit,
  });
});

export const getAccountDeletionStatus = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const user = await UserModel.findById(req.user._id);
  if (!user) return next(new AppError("User not found", 404));

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      requestedAt: user.deletionRequestedAt,
      scheduledFor: user.deletionScheduledFor,
      cancelledAt: user.deletionCancelledAt,
      isPending: Boolean(
        user.deletionScheduledFor &&
        new Date(user.deletionScheduledFor).getTime() > Date.now(),
      ),
    },
  });
});

export const requestAccountDeletion = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const { password, code } = req.body || {};
  if (!password) return next(new AppError("Password is required", 400));

  const user = await UserModel.findById(req.user._id).select(
    "+password +twoFactorSecret",
  );
  if (!user) return next(new AppError("User not found", 404));

  const ok = await user.correctPassword(String(password));
  if (!ok) return next(new AppError("Invalid password", 401));

  if (user.twoFactorEnabled) {
    if (!code) {
      return next(new AppError("Two-factor code is required", 400));
    }
    verifyTwoFactorCodeOrThrow(user, code);
  }

  const graceDays = Number(process.env.ACCOUNT_DELETION_GRACE_DAYS || 7);
  const now = new Date();
  const scheduledFor = new Date(
    now.getTime() + graceDays * 24 * 60 * 60 * 1000,
  );

  user.deletionRequestedAt = now;
  user.deletionScheduledFor = scheduledFor;
  user.deletionCancelledAt = null;
  user.deletedAt = null;
  await user.save({ validateBeforeSave: false });

  return sendSuccess(res, {
    statusCode: 200,
    message:
      "Account deletion scheduled. You can cancel before the scheduled date.",
    data: {
      scheduledFor,
      graceDays,
    },
  });
});

export const cancelAccountDeletion = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));

  const { password, code } = req.body || {};
  if (!password) return next(new AppError("Password is required", 400));

  const user = await UserModel.findById(req.user._id).select(
    "+password +twoFactorSecret",
  );
  if (!user) return next(new AppError("User not found", 404));

  const ok = await user.correctPassword(String(password));
  if (!ok) return next(new AppError("Invalid password", 401));

  if (user.twoFactorEnabled) {
    if (!code) {
      return next(new AppError("Two-factor code is required", 400));
    }
    verifyTwoFactorCodeOrThrow(user, code);
  }

  user.deletionRequestedAt = null;
  user.deletionScheduledFor = null;
  user.deletionCancelledAt = new Date();
  user.deletedAt = null;
  await user.save({ validateBeforeSave: false });

  return sendSuccess(res, {
    statusCode: 200,
    message: "Account deletion request cancelled.",
  });
});

export const protect = catchAsync(async (req, res, next) => {
  let token;
  if (req.headers.authorization?.startsWith("Bearer ")) {
    token = req.headers.authorization.split(" ")[1];
  }

  if (!token) {
    return next(
      new AppError("You are not logged in. Please log in to get access.", 401),
    );
  }

  const secret = getAccessSecret();
  if (!secret) return next(new AppError("Server auth misconfiguration", 500));

  const decoded = await promisify(jwt.verify)(token, secret);

  if (decoded?.type !== "access" || !decoded?.id) {
    return next(new AppError("Invalid token. Please log in again.", 401));
  }

  const currentUser = await UserModel.findById(decoded.id).select(
    "+active +passwordChangedAt",
  );

  if (!currentUser) {
    return next(
      new AppError("The user belonging to this token no longer exists", 401),
    );
  }

  await enforceDeletionStatus(currentUser);

  if (currentUser.active === false) {
    return next(
      new AppError("The user belonging to this token no longer exists", 401),
    );
  }

  if (currentUser.changedPasswordAfter(decoded.iat)) {
    return next(
      new AppError("User recently changed password. Please log in again.", 401),
    );
  }

  await ensureActiveGroupMembership(currentUser);

  req.user = currentUser;
  return next();
});

export function restrictTo(...roles) {
  return (req, res, next) => {
    if (!req.user) return next(new AppError("Not authenticated", 401));

    if (!roles.includes(req.user.role)) {
      return next(
        new AppError("You do not have permission to perform this action", 403),
      );
    }

    return next();
  };
}
