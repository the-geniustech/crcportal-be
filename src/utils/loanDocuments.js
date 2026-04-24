import AppError from "./AppError.js";

export const LoanDocumentTypes = Object.freeze([
  "valid_id_card",
  "proof_of_address",
  "passport_photograph",
]);

export const LoanDocumentCatalog = Object.freeze([
  {
    id: "valid_id_card",
    label: "Valid ID Card",
    description:
      "National ID, Voter's Card, Driver's License, or International Passport",
    acceptedMimeTypes: ["image/jpeg", "image/png", "application/pdf"],
  },
  {
    id: "proof_of_address",
    label: "Proof of Address",
    description:
      "Utility bill, bank statement, or tenancy agreement (not older than 3 months)",
    acceptedMimeTypes: ["image/jpeg", "image/png", "application/pdf"],
  },
  {
    id: "passport_photograph",
    label: "Passport Photograph",
    description:
      "Recent passport-sized photograph with white background",
    acceptedMimeTypes: ["image/jpeg", "image/png"],
  },
]);

const LoanDocumentTypeAliases = Object.freeze({
  valid_id_card: "valid_id_card",
  id_card: "valid_id_card",
  proof_of_address: "proof_of_address",
  passport_photograph: "passport_photograph",
  passport_photo: "passport_photograph",
});

const LoanDocumentLabelByType = new Map(
  LoanDocumentCatalog.map((item) => [item.id, item.label]),
);

export function normalizeLoanDocumentType(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_");

  if (!normalized) return null;
  return LoanDocumentTypeAliases[normalized] || null;
}

export function getLoanDocumentLabel(documentType) {
  const normalized = normalizeLoanDocumentType(documentType);
  if (!normalized) return null;
  return LoanDocumentLabelByType.get(normalized) || null;
}

export function inferLoanDocumentType(raw) {
  if (!raw || typeof raw !== "object") return null;

  const fromExplicit = normalizeLoanDocumentType(
    raw.documentType || raw.docType || raw.id,
  );
  if (fromExplicit) return fromExplicit;

  const label = String(raw.label || raw.name || "")
    .trim()
    .toLowerCase();

  if (label.startsWith("valid id card")) return "valid_id_card";
  if (label.startsWith("proof of address")) return "proof_of_address";
  if (label.startsWith("passport photograph")) return "passport_photograph";

  return null;
}

export function sanitizeLoanDocument(raw) {
  if (!raw || typeof raw !== "object") return null;

  const documentType = inferLoanDocumentType(raw);
  if (!documentType) {
    throw new AppError(
      "Each loan document must be assigned one of the supported document types.",
      400,
    );
  }

  const label = getLoanDocumentLabel(documentType);
  const mimeType = String(raw.type || "application/octet-stream").trim();
  const size = Number(raw.size || 0);
  const status = String(raw.status || "uploaded").trim() || "uploaded";

  return {
    documentType,
    name: label,
    type: mimeType,
    size: Number.isFinite(size) && size >= 0 ? size : 0,
    status,
    url: raw.url ? String(raw.url) : null,
  };
}

export function sanitizeLoanDocumentList(rawDocuments, { requireAll = false } = {}) {
  const docs = Array.isArray(rawDocuments) ? rawDocuments : [];
  const seen = new Set();

  const normalized = docs
    .map((doc) => sanitizeLoanDocument(doc))
    .filter(Boolean)
    .map((doc) => {
      if (seen.has(doc.documentType)) {
        throw new AppError(
          "Duplicate loan document types are not allowed.",
          400,
        );
      }
      seen.add(doc.documentType);
      return doc;
    });

  if (normalized.length > LoanDocumentTypes.length) {
    throw new AppError(
      "Only the supported loan document types can be attached to this application.",
      400,
    );
  }

  if (requireAll) {
    const missing = LoanDocumentCatalog.filter(
      (item) => !seen.has(item.id),
    ).map((item) => item.label);

    if (missing.length > 0) {
      throw new AppError(
        `Missing required loan documents: ${missing.join(", ")}`,
        400,
      );
    }
  }

  return normalized;
}
