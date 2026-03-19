import express from "express";

import rateLimit from "../middlewares/rateLimit.js";
import {
  changePassword,
  forgotPassword,
  login,
  logout,
  protect,
  refresh,
  resendVerification,
  resetPassword,
  signup,
  verifyEmail,
  verifyPhone,
  sendPhoneOtpLogin,
  verifyPhoneOtpLogin,
} from "../controllers/authController.js";
import { getMe } from "../controllers/userController.js";

const router = express.Router();

const resendLimiter = rateLimit({
  windowMs: 60_000,
  max: 5,
  keyGenerator: (req) => {
    const id = (req.body?.email || req.body?.phone || req.body?.loginId || "")
      .toString()
      .toLowerCase();
    return `resend:${req.ip}:${id}`;
  },
  message:
    "Too many verification requests. Please wait a minute and try again.",
});

const phoneOtpSendLimiter = rateLimit({
  windowMs: 60_000,
  max: 5,
  keyGenerator: (req) => {
    const phone = (req.body?.phone || "").toString();
    return `phone-otp-send:${req.ip}:${phone}`;
  },
  message: "Too many OTP requests. Please wait a minute and try again.",
});

const phoneOtpVerifyLimiter = rateLimit({
  windowMs: 10 * 60_000,
  max: 10,
  keyGenerator: (req) => {
    const pinId = (req.body?.pinId || "").toString();
    return `phone-otp-verify:${req.ip}:${pinId}`;
  },
  message: "Too many OTP attempts. Please try again later.",
});

const otpVerifyLimiter = rateLimit({
  windowMs: 10 * 60_000,
  max: 10,
  keyGenerator: (req) => {
    const phone = (req.body?.phone || "").toString();
    return `otp-verify:${req.ip}:${phone}`;
  },
  message: "Too many OTP attempts. Please try again later.",
});

const forgotLimiter = rateLimit({
  windowMs: 10 * 60_000,
  max: 8,
  keyGenerator: (req) => {
    const id = (req.body?.email || req.body?.phone || req.body?.loginId || "")
      .toString()
      .toLowerCase();
    return `forgot:${req.ip}:${id}`;
  },
  message: "Too many password reset requests. Please try again later.",
});

const resetLimiter = rateLimit({
  windowMs: 10 * 60_000,
  max: 12,
  keyGenerator: (req) => {
    const phone = (req.body?.phone || "").toString();
    const token = (req.body?.token || req.query?.token || "")
      .toString()
      .slice(0, 16);
    return `reset:${req.ip}:${phone}:${token}`;
  },
  message: "Too many reset attempts. Please try again later.",
});

router.post("/signup", signup);
router.post("/login", login);
router.post("/refresh", refresh);
router.post("/logout", logout);

router.post("/phone-otp/send", phoneOtpSendLimiter, sendPhoneOtpLogin);
router.post("/phone-otp/verify", phoneOtpVerifyLimiter, verifyPhoneOtpLogin);

router.get("/verify-email", verifyEmail);
router.post("/verify-phone", otpVerifyLimiter, verifyPhone);
router.post("/resend-verification", resendLimiter, resendVerification);

router.post("/forgot-password", forgotLimiter, forgotPassword);
router.post("/reset-password", resetLimiter, resetPassword);

router.patch("/change-password", protect, changePassword);

router.get("/me", protect, getMe);

export default router;
