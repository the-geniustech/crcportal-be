import { Resend } from "resend";
import AppError from "../../utils/AppError.js";

let resendClient;

function getClient() {
  if (resendClient) return resendClient;

  const apiKey = process.env.RESEND_API_KEY;
  if (!apiKey) throw new AppError("Missing RESEND_API_KEY", 400);
  resendClient = new Resend(apiKey);
  return resendClient;
}

export async function sendEmail({ to, subject, html, text, attachments }) {
  const from =
    process.env.RESEND_FROM ||
    process.env.RESEND_FROM_EMAIL ||
    process.env.MAIL_FROM;
  if (!from) throw new AppError("Missing RESEND_FROM", 400);

  const client = getClient();

  // The Resend SDK throws on error; we simply propagate the promise
  return client.emails.send({
    from,
    to,
    subject,
    html,
    text,
    attachments,
  });
}
