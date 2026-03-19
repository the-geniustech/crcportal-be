import crypto from "node:crypto";

export function sha256(input) {
  return crypto.createHash("sha256").update(input).digest("hex");
}

export function randomId(bytes = 32) {
  return crypto.randomBytes(bytes).toString("hex");
}
