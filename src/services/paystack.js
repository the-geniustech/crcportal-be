import crypto from "crypto";

import AppError from "../utils/AppError.js";

const PAYSTACK_BASE_URL = "https://api.paystack.co";

function getSecretKey() {
  const key = process.env.PAYSTACK_SECRET_KEY;
  if (!key) throw new AppError("Missing PAYSTACK_SECRET_KEY", 500);
  return key;
}

async function paystackRequest(path, { method = "GET", body } = {}) {
  const secretKey = getSecretKey();

  const res = await fetch(`${PAYSTACK_BASE_URL}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${secretKey}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  const json = await res.json().catch(() => null);
  if (!res.ok) {
    const msg = json?.message || json?.error || `Paystack request failed (${res.status})`;
    throw new AppError(msg, 502);
  }

  if (!json || json.status !== true) {
    throw new AppError(json?.message || "Paystack returned an error", 502);
  }

  return json;
}

export async function initializeTransaction(input) {
  return paystackRequest("/transaction/initialize", { method: "POST", body: input });
}

export async function verifyTransaction(reference) {
  const encoded = encodeURIComponent(String(reference));
  return paystackRequest(`/transaction/verify/${encoded}`, { method: "GET" });
}

export function computeWebhookSignature(rawBodyBuffer) {
  const secretKey = getSecretKey();
  return crypto.createHmac("sha512", secretKey).update(rawBodyBuffer).digest("hex");
}

export function isValidWebhookSignature(rawBodyBuffer, signatureHeader) {
  if (!signatureHeader || !rawBodyBuffer) return false;
  const expected = computeWebhookSignature(rawBodyBuffer);
  try {
    return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(String(signatureHeader)));
  } catch {
    return false;
  }
}

