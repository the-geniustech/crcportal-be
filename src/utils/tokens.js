import jwt from "jsonwebtoken";

export function signAccessToken({ userId, role }) {
  const secret = process.env.JWT_ACCESS_SECRET || process.env.JWT_SECRET;
  if (!secret) throw new Error("Missing JWT_ACCESS_SECRET (or JWT_SECRET)");

  return jwt.sign(
    { id: userId, role, type: "access" },
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
