import { sendEmail } from "./resendClient.js";

export async function sendPasswordResetEmail({ toEmail, resetUrl }) {
  const subject = "Reset your password";

  const text = `Reset your password: ${resetUrl}`;

  const html = `
  <div style="font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; line-height:1.5">
    <h2>Reset your password</h2>
    <p>Click the link below to reset your password:</p>
    <p><a href="${resetUrl}">${resetUrl}</a></p>
    <p>This link expires soon. If you didn't request this, you can ignore this email.</p>
  </div>`;

  return sendEmail({
    to: [toEmail],
    subject,
    html,
    text,
  });
}
