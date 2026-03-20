import crypto from "node:crypto";

export function sha256(input) {
  return crypto.createHash("sha256").update(input).digest("hex");
}

export function randomId(bytes = 32) {
  return crypto.randomBytes(bytes).toString("hex");
}

function resolveEncryptionKey() {
  const raw =
    process.env.TWOFA_ENCRYPTION_KEY ||
    process.env.JWT_SECRET ||
    process.env.JWT_ACCESS_SECRET;
  if (!raw) return null;
  return crypto.createHash("sha256").update(raw).digest();
}

export function encryptSecret(value) {
  if (!value) return value;
  const key = resolveEncryptionKey();
  if (!key) return value;

  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);
  const encrypted = Buffer.concat([
    cipher.update(String(value), "utf8"),
    cipher.final(),
  ]);
  const tag = cipher.getAuthTag();

  return `${iv.toString("hex")}.${tag.toString("hex")}.${encrypted.toString(
    "hex",
  )}`;
}

export function decryptSecret(payload) {
  if (!payload) return payload;
  const key = resolveEncryptionKey();
  if (!key) return payload;

  const parts = String(payload).split(".");
  if (parts.length !== 3) return payload;

  const [ivHex, tagHex, encryptedHex] = parts;
  const iv = Buffer.from(ivHex, "hex");
  const tag = Buffer.from(tagHex, "hex");
  const encrypted = Buffer.from(encryptedHex, "hex");

  const decipher = crypto.createDecipheriv("aes-256-gcm", key, iv);
  decipher.setAuthTag(tag);
  const decrypted = Buffer.concat([
    decipher.update(encrypted),
    decipher.final(),
  ]);

  return decrypted.toString("utf8");
}
