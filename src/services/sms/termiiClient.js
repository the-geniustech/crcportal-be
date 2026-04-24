import AppError from "../../utils/AppError.js";
import { toNigerianE164 } from "../../utils/phone.js";

function getTermiiBaseUrl() {
  return process.env.TERMII_BASE_URL || "https://api.ng.termii.com";
}

function normalizeTermiiChannel(channel) {
  const normalized = String(channel || process.env.TERMII_CHANNEL || "generic")
    .trim()
    .toLowerCase();
  return normalized || "generic";
}

function parseTermiiPayload(rawText) {
  if (!rawText) return null;

  try {
    return JSON.parse(rawText);
  } catch {
    return { raw: rawText };
  }
}

function formatTermiiErrorMessage(payload) {
  if (!payload || typeof payload !== "object") return "";

  const parts = [];
  if (typeof payload.message === "string" && payload.message.trim()) {
    parts.push(payload.message.trim());
  }
  if (
    typeof payload.code === "string" &&
    payload.code.trim() &&
    payload.code.trim().toLowerCase() !== "ok"
  ) {
    parts.push(`code: ${payload.code.trim()}`);
  }
  if (
    typeof payload.error === "string" &&
    payload.error.trim() &&
    !parts.includes(payload.error.trim())
  ) {
    parts.push(payload.error.trim());
  }

  return parts.join(" - ");
}

function classifyTermiiFailure(payload) {
  const details = [
    payload?.message,
    payload?.error,
    payload?.status,
    payload?.link,
  ]
    .filter((value) => typeof value === "string" && value.trim())
    .join(" ")
    .toLowerCase();

  if (
    details.includes("country inactive") ||
    details.includes("not set up on this route") ||
    details.includes("service is currently not active on your account") ||
    details.includes("service in question is not active on your account")
  ) {
    return "route_inactive";
  }

  if (details.includes("invalid sender id")) {
    return "invalid_sender_id";
  }

  if (details.includes("insufficient balance")) {
    return "insufficient_balance";
  }

  if (details.includes("account is not active")) {
    return "account_inactive";
  }

  return "provider_rejected";
}

function logTermiiFailure({ channel, to, statusCode, payload, error }) {
  const details = {
    channel,
    to,
    statusCode,
    payload,
  };

  if (error) {
    console.error("Termii SMS request failed", {
      ...details,
      error: error instanceof Error ? error.message : String(error),
    });
    return;
  }

  console.error("Termii SMS rejected", details);
}

function buildTermiiAppError({
  channel,
  statusCode,
  payload,
  providerMessage,
}) {
  const category = classifyTermiiFailure(payload);
  let message = `Termii rejected the SMS request${
    providerMessage ? `: ${providerMessage}` : "."
  }`;

  if (category === "route_inactive") {
    message =
      `Termii rejected the SMS request for the ${channel} route because the country or route is not active on this account.` +
      " Activate the route in Termii or contact your account manager/support.";
  } else if (category === "invalid_sender_id") {
    message =
      "Termii rejected the SMS request because the configured sender ID is invalid or not approved for this account.";
  } else if (category === "insufficient_balance") {
    message =
      "Termii rejected the SMS request because the SMS wallet balance is insufficient.";
  } else if (category === "account_inactive") {
    message =
      "Termii rejected the SMS request because this account is not active.";
  }

  const error = new AppError(
    message,
    category === "provider_rejected" && statusCode < 500 ? 400 : 502,
  );
  error.provider = "termii";
  error.providerCategory = category;
  error.providerStatusCode = statusCode;
  error.providerPayload = payload;
  error.providerMessage = providerMessage || null;
  error.deliveryChannel = channel;
  return error;
}

export function normalizeSmsDeliveryStatus(status) {
  return String(status || "").trim().toLowerCase();
}

export function isTerminalUndeliveredSmsStatus(status) {
  return [
    "dnd active on phone number",
    "message failed",
    "failed",
    "rejected",
    "expired",
  ].includes(normalizeSmsDeliveryStatus(status));
}

export function isDeliveredSmsStatus(status) {
  return normalizeSmsDeliveryStatus(status) === "delivered";
}

export async function sendSms({ to, message, channel }) {
  const apiKey = process.env.TERMII_API_KEY;
  const senderId = process.env.TERMII_SENDER_ID;
  const baseUrl = getTermiiBaseUrl();
  const path = process.env.TERMII_SEND_PATH || "/api/sms/send";
  const resolvedChannel = normalizeTermiiChannel(channel);

  const normalized = toNigerianE164(to);
  if (!normalized) {
    throw new AppError("Invalid recipient phone number.", 400);
  }
  to = normalized;

  if (!apiKey) {
    throw new AppError(
      "SMS is not configured correctly on the server. Missing TERMII_API_KEY.",
      500,
    );
  }
  if (!senderId) {
    throw new AppError(
      "SMS is not configured correctly on the server. Missing TERMII_SENDER_ID.",
      500,
    );
  }

  let res;
  try {
    res = await fetch(`${baseUrl}${path}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "User-Agent": process.env.APP_NAME || "crc-backend",
      },
      body: JSON.stringify({
        api_key: apiKey,
        to,
        from: senderId,
        sms: message,
        type: "plain",
        channel: resolvedChannel,
      }),
    });
  } catch (error) {
    logTermiiFailure({
      channel: resolvedChannel,
      to,
      statusCode: null,
      payload: null,
      error,
    });
    throw new AppError(
      `Unable to reach the SMS gateway for ${resolvedChannel} delivery. ${
        error instanceof Error && error.message
          ? error.message
          : "Please try again shortly."
      }`,
      502,
    );
  }

  const rawBody = await res.text().catch(() => "");
  const payload = parseTermiiPayload(rawBody);
  const providerMessage = formatTermiiErrorMessage(payload);

  if (!res.ok) {
    logTermiiFailure({
      channel: resolvedChannel,
      to,
      statusCode: res.status,
      payload,
    });
    throw buildTermiiAppError({
      channel: resolvedChannel,
      statusCode: res.status,
      payload,
      providerMessage,
    });
  }

  const providerCode = String(payload?.code || "")
    .trim()
    .toLowerCase();
  if (providerCode && providerCode !== "ok") {
    logTermiiFailure({
      channel: resolvedChannel,
      to,
      statusCode: res.status,
      payload,
    });
    throw buildTermiiAppError({
      channel: resolvedChannel,
      statusCode: res.status,
      payload,
      providerMessage,
    });
  }

  return {
    ...(payload && typeof payload === "object" ? payload : {}),
    channel: resolvedChannel,
    to,
  };
}

export async function sendVoiceOtpCall({ to, code }) {
  const apiKey = process.env.TERMII_API_KEY;
  const baseUrl = getTermiiBaseUrl();
  const normalized = toNigerianE164(to);
  const numericCode = String(code || "").trim();

  if (!normalized) {
    throw new AppError("Invalid recipient phone number.", 400);
  }
  if (!/^\d{4,8}$/.test(numericCode)) {
    throw new AppError(
      "Voice OTP fallback requires a numeric code between 4 and 8 digits.",
      400,
    );
  }
  if (!apiKey) {
    throw new AppError(
      "SMS is not configured correctly on the server. Missing TERMII_API_KEY.",
      500,
    );
  }

  let res;
  try {
    res = await fetch(`${baseUrl}/api/sms/otp/call`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "User-Agent": process.env.APP_NAME || "crc-backend",
      },
      body: JSON.stringify({
        api_key: apiKey,
        phone_number: normalized.replace(/^\+/, ""),
        code: numericCode,
      }),
    });
  } catch (error) {
    logTermiiFailure({
      channel: "voice_call",
      to: normalized,
      statusCode: null,
      payload: null,
      error,
    });
    throw new AppError(
      `Unable to reach the Termii voice OTP gateway. ${
        error instanceof Error && error.message
          ? error.message
          : "Please try again shortly."
      }`,
      502,
    );
  }

  const rawBody = await res.text().catch(() => "");
  const payload = parseTermiiPayload(rawBody);
  const providerMessage = formatTermiiErrorMessage(payload);

  if (!res.ok) {
    logTermiiFailure({
      channel: "voice_call",
      to: normalized,
      statusCode: res.status,
      payload,
    });
    throw buildTermiiAppError({
      channel: "voice_call",
      statusCode: res.status,
      payload,
      providerMessage,
    });
  }

  const providerCode = String(payload?.code || "")
    .trim()
    .toLowerCase();
  if (providerCode && providerCode !== "ok") {
    logTermiiFailure({
      channel: "voice_call",
      to: normalized,
      statusCode: res.status,
      payload,
    });
    throw buildTermiiAppError({
      channel: "voice_call",
      statusCode: res.status,
      payload,
      providerMessage,
    });
  }

  return {
    ...(payload && typeof payload === "object" ? payload : {}),
    channel: "voice_call",
    to: normalized,
  };
}

export async function lookupPhoneDndStatus(phoneNumber) {
  const apiKey = process.env.TERMII_API_KEY;
  const baseUrl = getTermiiBaseUrl();
  const normalized = toNigerianE164(phoneNumber);

  if (!normalized) {
    throw new AppError("Invalid recipient phone number.", 400);
  }
  if (!apiKey) {
    throw new AppError(
      "SMS is not configured correctly on the server. Missing TERMII_API_KEY.",
      500,
    );
  }

  const url = new URL("/api/check/dnd", baseUrl);
  url.searchParams.set("api_key", apiKey);
  url.searchParams.set("phone_number", normalized.replace(/^\+/, ""));

  let res;
  try {
    res = await fetch(url, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        "User-Agent": process.env.APP_NAME || "crc-backend",
      },
    });
  } catch (error) {
    throw new AppError(
      `Unable to reach the Termii DND status endpoint. ${
        error instanceof Error && error.message
          ? error.message
          : "Please try again shortly."
      }`,
      502,
    );
  }

  const rawBody = await res.text().catch(() => "");
  const payload = parseTermiiPayload(rawBody);
  const providerMessage = formatTermiiErrorMessage(payload);

  if (!res.ok) {
    throw buildTermiiAppError({
      channel: "status_lookup",
      statusCode: res.status,
      payload,
      providerMessage,
    });
  }

  const dndActiveValue = payload?.dnd_active;
  const dndActive =
    typeof dndActiveValue === "boolean"
      ? dndActiveValue
      : String(dndActiveValue || "").trim().toLowerCase() === "true";

  return {
    dndActive,
    status: payload?.status ? String(payload.status) : null,
    message: payload?.message ? String(payload.message) : null,
    network: payload?.network ? String(payload.network) : null,
    networkCode: payload?.network_code ? String(payload.network_code) : null,
    number: payload?.number ? String(payload.number) : normalized,
    raw: payload,
  };
}

export async function lookupSmsHistory(messageId) {
  const apiKey = process.env.TERMII_API_KEY;
  const baseUrl = getTermiiBaseUrl();
  const trimmedMessageId = String(messageId || "").trim();

  if (!trimmedMessageId) return null;
  if (!apiKey) {
    throw new AppError(
      "SMS is not configured correctly on the server. Missing TERMII_API_KEY.",
      500,
    );
  }

  const url = new URL("/api/sms/inbox", baseUrl);
  url.searchParams.set("api_key", apiKey);
  url.searchParams.set("message_id", trimmedMessageId);

  let res;
  try {
    res = await fetch(url, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        "User-Agent": process.env.APP_NAME || "crc-backend",
      },
    });
  } catch (error) {
    throw new AppError(
      `Unable to reach the Termii SMS history endpoint. ${
        error instanceof Error && error.message
          ? error.message
          : "Please try again shortly."
      }`,
      502,
    );
  }

  const rawBody = await res.text().catch(() => "");
  const payload = parseTermiiPayload(rawBody);
  const providerMessage = formatTermiiErrorMessage(payload);

  if (!res.ok) {
    throw buildTermiiAppError({
      channel: "history_lookup",
      statusCode: res.status,
      payload,
      providerMessage,
    });
  }

  if (Array.isArray(payload)) {
    return payload[0] || null;
  }

  if (payload && typeof payload === "object") {
    return payload;
  }

  return null;
}

export async function waitForSmsDeliveryStatus(
  messageId,
  { timeoutMs = 8_000, intervalMs = 1_500 } = {},
) {
  const trimmedMessageId = String(messageId || "").trim();
  if (!trimmedMessageId) return null;

  const startedAt = Date.now();
  let lastEntry = null;

  while (Date.now() - startedAt <= timeoutMs) {
    const historyEntry = await lookupSmsHistory(trimmedMessageId);
    if (historyEntry) {
      lastEntry = historyEntry;
      const status = String(historyEntry.status || "").trim();
      if (
        isDeliveredSmsStatus(status) ||
        isTerminalUndeliveredSmsStatus(status)
      ) {
        return historyEntry;
      }
    }

    if (Date.now() - startedAt >= timeoutMs) break;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  return lastEntry;
}
