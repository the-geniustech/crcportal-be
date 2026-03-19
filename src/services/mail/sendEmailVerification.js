import { sendEmail } from "./resendClient.js";

export async function sendEmailVerification({ toEmail, verificationUrl }) {
  const subject = "Verify your account";

  const text = `Verify your account: ${verificationUrl}`;

  const html = `
  <div style="font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; line-height:1.5">
    <h2>Verify your account</h2>
    <p>Click the link below to verify your email address:</p>
    <p><a href="${verificationUrl}">${verificationUrl}</a></p>
    <p>If you didn't request this, you can ignore this email.</p>
  </div>`;

  return sendEmail({
    to: [toEmail],
    subject,
    html,
    text,
  });
}
