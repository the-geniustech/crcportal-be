import AppError from "../../utils/AppError.js";
import { sendEmailOtp } from "../mail/sendEmailOtp.js";
import { sendPhoneOtp } from "../sms/sendPhoneOtp.js";

function maskPhoneNumber(phone) {
  const raw = String(phone || "").trim();
  if (raw.length <= 4) return raw;
  return `${raw.slice(0, 4)}${"*".repeat(Math.max(0, raw.length - 6))}${raw.slice(-2)}`;
}

function maskEmailAddress(email) {
  const raw = String(email || "").trim().toLowerCase();
  const atIndex = raw.indexOf("@");
  if (atIndex <= 1) return raw;
  const local = raw.slice(0, atIndex);
  const domain = raw.slice(atIndex);
  return `${local.slice(0, 1)}${"*".repeat(Math.max(1, local.length - 2))}${local.slice(-1)}${domain}`;
}

function normalizeRecipient(value, { lowercase = false } = {}) {
  const normalized = String(value || "").trim();
  if (!normalized) return "";
  return lowercase ? normalized.toLowerCase() : normalized;
}

function appendDeliveryCandidate(candidates, seen, channel, recipient) {
  if (!recipient) return;
  const key = `${channel}:${recipient}`;
  if (seen.has(key)) return;
  seen.add(key);
  candidates.push({ channel, recipient });
}

function buildDeliveryCandidates(user) {
  const email = normalizeRecipient(user?.email, { lowercase: true });
  const phone = normalizeRecipient(user?.phone);
  const candidates = [];
  const seen = new Set();

  if (user?.emailVerifiedAt) {
    appendDeliveryCandidate(candidates, seen, "email", email);
  }
  appendDeliveryCandidate(candidates, seen, "email", email);

  if (user?.phoneVerifiedAt) {
    appendDeliveryCandidate(candidates, seen, "phone", phone);
  }
  appendDeliveryCandidate(candidates, seen, "phone", phone);

  return candidates;
}

async function deliverOtp({ channel, recipient, otp, ttlMinutes, purpose }) {
  if (channel === "email") {
    await sendEmailOtp({
      toEmail: recipient,
      otp,
      ttlMinutes,
      purpose,
    });
    return {
      channel,
      recipient,
      maskedRecipient: maskEmailAddress(recipient),
    };
  }

  await sendPhoneOtp({
    toPhone: recipient,
    otp,
    ttlMinutes,
    purpose,
  });
  return {
    channel,
    recipient,
    maskedRecipient: maskPhoneNumber(recipient),
  };
}

function buildDeliveryError(failures) {
  const reason = failures
    .map(({ channel, error }) => {
      const message =
        error instanceof Error && error.message
          ? error.message
          : "delivery failed";
      return `${channel}: ${message}`;
    })
    .join("; ");

  return new AppError(
    `Unable to deliver the authorization code to the initiating admin. ${reason}`,
    502,
  );
}

export async function sendAdminAuthorizationOtp({
  user,
  otp,
  ttlMinutes,
  purpose,
}) {
  const candidates = buildDeliveryCandidates(user);

  if (candidates.length === 0) {
    throw new AppError(
      "Authorized user must have a phone number or email to receive OTP.",
      400,
    );
  }

  const failures = [];
  let primaryDelivery = null;
  let primaryIndex = -1;

  for (let index = 0; index < candidates.length; index += 1) {
    const candidate = candidates[index];

    try {
      primaryDelivery = await deliverOtp({
        ...candidate,
        otp,
        ttlMinutes,
        purpose,
      });
      primaryIndex = index;
      break;
    } catch (error) {
      failures.push({
        channel: candidate.channel,
        recipient: candidate.recipient,
        error,
      });
    }
  }

  if (!primaryDelivery) {
    throw buildDeliveryError(failures);
  }

  for (let index = primaryIndex + 1; index < candidates.length; index += 1) {
    const candidate = candidates[index];
    try {
      await deliverOtp({
        ...candidate,
        otp,
        ttlMinutes,
        purpose,
      });
    } catch (error) {
      const message =
        error instanceof Error && error.message
          ? error.message
          : "delivery failed";
      console.warn(
        `Secondary admin OTP delivery via ${candidate.channel} failed for ${purpose}: ${message}`,
      );
    }
  }

  return primaryDelivery;
}
