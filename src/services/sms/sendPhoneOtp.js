import { sendSms } from "./termiiClient.js";

export async function sendPhoneOtp({ toPhone, otp, ttlMinutes }) {
  const message = `Your verification code is ${otp}. It expires in ${ttlMinutes} minutes.`;
  return sendSms({ to: toPhone, message });
}
