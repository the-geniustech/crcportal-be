export function normalizeNigerianPhone(input) {
  const raw = String(input || "").trim();
  if (!raw) return null;

  const digits = raw.replace(/\D/g, "");
  let local = "";

  if (digits.startsWith("234") && digits.length === 13) {
    local = digits.slice(3);
  } else if (digits.length === 11 && digits.startsWith("0")) {
    local = digits.slice(1);
  } else if (digits.length === 10) {
    local = digits;
  } else {
    return null;
  }

  if (!/^[789]\d{9}$/.test(local)) return null;

  return `+234 ${local.slice(0, 3)} ${local.slice(3, 6)} ${local.slice(6)}`;
}

export function normalizeNigerianPhoneValue(value) {
  if (value === null || value === undefined) return value;
  const raw = String(value || "").trim();
  if (!raw) return raw;
  const normalized = normalizeNigerianPhone(raw);
  return normalized || raw;
}

export function isNormalizedNigerianPhone(value) {
  if (value === null || value === undefined) return true;
  const raw = String(value || "").trim();
  if (!raw) return true;
  return normalizeNigerianPhone(raw) === raw;
}

export function toNigerianE164(input) {
  const normalized = normalizeNigerianPhone(input);
  if (!normalized) return null;
  const digits = normalized.replace(/\D/g, "");
  return `+${digits}`;
}
