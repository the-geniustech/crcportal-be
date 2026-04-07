import jwt from "jsonwebtoken";
import { normalizeUserRoles, pickPrimaryRole } from "./roles.js";

export function signAccessToken({ userId, role, roles }) {
  const secret = process.env.JWT_ACCESS_SECRET || process.env.JWT_SECRET;
  if (!secret) throw new Error("Missing JWT_ACCESS_SECRET (or JWT_SECRET)");

  const normalizedRoles = normalizeUserRoles({
    role,
    roles: Array.isArray(roles) ? roles : undefined,
  });
  const primaryRole = role || pickPrimaryRole(normalizedRoles);

  return jwt.sign(
    { id: userId, role: primaryRole, roles: normalizedRoles, type: "access" },
    secret,
    { expiresIn: process.env.JWT_ACCESS_EXPIRES_IN || "15m" },
  );
}

export function signRefreshToken({ userId, jti }) {
  const secret = process.env.JWT_REFRESH_SECRET;
  if (!secret) throw new Error("Missing JWT_REFRESH_SECRET");

  return jwt.sign(
    { id: userId, jti, type: "refresh" },
    secret,
    { expiresIn: process.env.JWT_REFRESH_EXPIRES_IN || "30d" },
  );
}

export function signTwoFactorToken({ userId }) {
  const secret =
    process.env.JWT_2FA_SECRET ||
    process.env.JWT_ACCESS_SECRET ||
    process.env.JWT_SECRET;
  if (!secret)
    throw new Error("Missing JWT_2FA_SECRET (or JWT_ACCESS_SECRET/JWT_SECRET)");

  return jwt.sign(
    { id: userId, type: "2fa" },
    secret,
    { expiresIn: process.env.JWT_2FA_EXPIRES_IN || "10m" },
  );
}

export function verifyTwoFactorToken(token) {
  const secret =
    process.env.JWT_2FA_SECRET ||
    process.env.JWT_ACCESS_SECRET ||
    process.env.JWT_SECRET;
  if (!secret)
    throw new Error("Missing JWT_2FA_SECRET (or JWT_ACCESS_SECRET/JWT_SECRET)");

  return jwt.verify(token, secret);
}
