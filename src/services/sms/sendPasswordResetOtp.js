import { sendTermiiOtpWithFallback } from "./sendTermiiOtpWithFallback.js";

export async function sendPasswordResetOtp({ toPhone, otp, ttlMinutes }) {
  const message = `Your password reset code is ${otp}. It expires in ${ttlMinutes} minutes.`;

  return sendTermiiOtpWithFallback({
    toPhone,
    otp,
    message,
    purposeLabel: "password-reset OTP delivery",
  });
}
