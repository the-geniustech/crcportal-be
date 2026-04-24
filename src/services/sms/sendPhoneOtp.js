import { sendTermiiOtpWithFallback } from "./sendTermiiOtpWithFallback.js";

export async function sendPhoneOtp({ toPhone, otp, ttlMinutes, purpose }) {
  const safePurpose = String(purpose || "").trim();
  const subject = safePurpose
    ? `Your ${safePurpose} code`
    : "Your verification code";
  const message = `${subject} is ${otp}. It expires in ${ttlMinutes} minutes.`;

  return sendTermiiOtpWithFallback({
    toPhone,
    otp,
    message,
    purposeLabel: safePurpose
      ? `${safePurpose} OTP delivery`
      : "phone OTP delivery",
  });
}
