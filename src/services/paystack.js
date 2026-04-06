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

export async function listBanks(params = {}) {
  const search = new URLSearchParams();
  if (params.country) search.set("country", String(params.country));
  if (typeof params.use_cursor === "boolean") {
    search.set("use_cursor", String(params.use_cursor));
  } else if (typeof params.useCursor === "boolean") {
    search.set("use_cursor", String(params.useCursor));
  }
  if (params.perPage) search.set("perPage", String(params.perPage));
  if (typeof params.pay_with_bank_transfer === "boolean") {
    search.set("pay_with_bank_transfer", String(params.pay_with_bank_transfer));
  }
  if (typeof params.pay_with_bank === "boolean") {
    search.set("pay_with_bank", String(params.pay_with_bank));
  }
  if (typeof params.enabled_for_verification === "boolean") {
    search.set("enabled_for_verification", String(params.enabled_for_verification));
  }
  if (params.currency) search.set("currency", String(params.currency));
  if (params.gateway) search.set("gateway", String(params.gateway));
  if (params.type) search.set("type", String(params.type));
  if (params.include_nip_sort_code) {
    search.set("include_nip_sort_code", String(params.include_nip_sort_code));
  }
  const qs = search.toString();
  return paystackRequest(`/bank${qs ? `?${qs}` : ""}`, { method: "GET" });
}

export async function createTransferRecipient(input) {
  return paystackRequest("/transferrecipient", { method: "POST", body: input });
}

export async function initiateTransfer(input) {
  return paystackRequest("/transfer", { method: "POST", body: input });
}

export async function verifyTransfer(reference) {
  const encoded = encodeURIComponent(String(reference));
  return paystackRequest(`/transfer/verify/${encoded}`, { method: "GET" });
}

export async function finalizeTransfer(input) {
  return paystackRequest("/transfer/finalize_transfer", {
    method: "POST",
    body: input,
  });
}

export async function resendTransferOtp(input) {
  return paystackRequest("/transfer/resend_otp", {
    method: "POST",
    body: input,
  });
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
