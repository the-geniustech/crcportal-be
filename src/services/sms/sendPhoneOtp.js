import { sendSms } from "./termiiClient.js";

export async function sendPhoneOtp({ toPhone, otp, ttlMinutes, purpose }) {
  const safePurpose = String(purpose || "").trim();
  const subject = safePurpose
    ? `Your ${safePurpose} code`
    : "Your verification code";
  const message = `${subject} is ${otp}. It expires in ${ttlMinutes} minutes.`;
  return sendSms({ to: toPhone, message });
}
