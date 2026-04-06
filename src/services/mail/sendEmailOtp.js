import { sendEmail } from "./resendClient.js";

export async function sendEmailOtp({ toEmail, otp, ttlMinutes, purpose }) {
  const safePurpose = purpose || "email change";
  const subject = "Confirm your email change";
  const text = `Your ${safePurpose} verification code is ${otp}. It expires in ${ttlMinutes} minutes.`;

  const html = `
    <div style="font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; line-height:1.6; color:#111827;">
      <h2 style="margin:0 0 12px;">Confirm your email change</h2>
      <p>Use the verification code below to complete your ${safePurpose}:</p>
      <div style="display:inline-block; background:#ecfdf3; color:#065f46; padding:10px 16px; border-radius:10px; font-size:20px; font-weight:700; letter-spacing:2px;">
        ${otp}
      </div>
      <p style="margin-top:12px;">This code expires in ${ttlMinutes} minutes.</p>
      <p style="color:#6b7280; font-size:12px; margin-top:24px;">
        If you did not request this change, you can ignore this email.
      </p>
    </div>
  `;

  return sendEmail({
    to: [toEmail],
    subject,
    html,
    text,
  });
}
