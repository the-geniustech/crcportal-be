import { sendSms } from "./termiiClient.js";

export async function sendPasswordResetOtp({ toPhone, otp, ttlMinutes }) {
  const message = `Your password reset code is ${otp}. It expires in ${ttlMinutes} minutes.`;
  return sendSms({ to: toPhone, message });
}
