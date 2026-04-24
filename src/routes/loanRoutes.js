import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import AppError from "../utils/AppError.js";
import sendSuccess from "../utils/sendSuccess.js";
import {
  cancelManualLoanDisbursement,
  createLoanDraft,
  createLoanApplication,
  createLoanEditRequest,
  deleteLoanDraft,
  disburseLoan,
  finalizeLoanDisbursementOtp,
  finalizeManualLoanDisbursement,
  getLoanEligibility,
  getLoanApplication,
  initiateManualLoanDisbursement,
  listLoanApplications,
  listLoanSchedule,
  listLoanBorrowerBankAccounts,
  listMyLoanApplications,
  recordLoanRepayment,
  resendLoanDisbursementOtp,
  resendManualLoanDisbursementOtp,
  reviewLoanApplication,
  verifyLoanDisbursementTransfer,
  updateLoanDraft,
} from "../controllers/loanController.js";
import {
  listMyGuarantorCommitments,
  listMyGuarantorNotifications,
  listMyGuarantorRequests,
  markGuarantorNotificationRead,
  respondToGuarantorRequest,
} from "../controllers/loanGuarantorController.js";
import { cloudinaryUploadMultiple } from "../middlewares/cloudinaryUpload.js";
import {
  loadLoanApplication,
  loadLoanGuarantor,
  requireGuarantorOwnerOrAdmin,
  requireLoanOwnerOrAdmin,
} from "../middlewares/loanContext.js";
import { uploadMultiple, uploadSingle } from "../middlewares/upload.js";
import { cloudinaryUploadSingle } from "../middlewares/cloudinaryUpload.js";
import {
  getLoanDocumentLabel,
  normalizeLoanDocumentType,
  sanitizeLoanDocumentList,
} from "../utils/loanDocuments.js";

const router = express.Router();

const loanDocumentFileFilter = (req, file, cb) => {
  if (
    file.mimetype?.startsWith("image/") ||
    file.mimetype === "application/pdf"
  ) {
    return cb(null, true);
  }
  return cb(new AppError("Only image or PDF uploads are allowed", 400));
};

const shouldHandleMultipart = (req) => req.is("multipart/form-data");

const loanDocumentsUpload = uploadMultiple("documents", 3, {
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: loanDocumentFileFilter,
});

const maybeUploadLoanDocuments = (req, res, next) => {
  if (!shouldHandleMultipart(req)) return next();
  return loanDocumentsUpload(req, res, next);
};

const loanDocumentsCloudinary = cloudinaryUploadMultiple({
  fileField: "documents",
  bodyField: "documentUploads",
  folder: "crc/loans/documents",
  resourceType: "auto",
});

const maybeCloudinaryLoanDocuments = (req, res, next) => {
  if (!shouldHandleMultipart(req)) return next();
  return loanDocumentsCloudinary(req, res, next);
};

const normalizeLoanDocuments = (req, res, next) => {
  const parseJsonField = (value) => {
    if (typeof value !== "string") return value;
    try {
      return JSON.parse(value);
    } catch {
      return value;
    }
  };

  req.body = req.body || {};
  req.body.documents = parseJsonField(req.body.documents);
  req.body.guarantors = parseJsonField(req.body.guarantors);

  const existingDocs = Array.isArray(req.body?.documents)
    ? req.body.documents
    : [];
  const requestedDocumentTypesRaw = parseJsonField(
    req.body.documentTypes ?? req.body.documentType,
  );
  const requestedDocumentTypes = Array.isArray(requestedDocumentTypesRaw)
    ? requestedDocumentTypesRaw
    : requestedDocumentTypesRaw
      ? [requestedDocumentTypesRaw]
      : [];
  const uploads = Array.isArray(req.body?.documentUploads)
    ? req.body.documentUploads
    : req.body?.documentUploads
      ? [req.body.documentUploads]
      : [];
  const files = Array.isArray(req.files) ? req.files : [];

  if (!uploads.length) {
    return next();
  }

  const mapped = uploads.map((upload, index) => {
    const file = files[index];
    const normalizedDocumentType = normalizeLoanDocumentType(
      requestedDocumentTypes[index],
    );
    if (!normalizedDocumentType) {
      throw new AppError(
        "documentType is required for each uploaded loan document",
        400,
      );
    }
    const inferredType =
      file?.mimetype ??
      (upload?.resourceType === "raw"
        ? "application/pdf"
        : upload?.format
          ? `image/${upload.format}`
          : "application/octet-stream");

    return {
      documentType: normalizedDocumentType,
      name: getLoanDocumentLabel(normalizedDocumentType) || "Document",
      type: inferredType,
      size: file?.size ?? upload?.bytes ?? 0,
      status: "uploaded",
      url: upload?.url ?? null,
    };
  });

  req.body.documents = sanitizeLoanDocumentList([...existingDocs, ...mapped]);

  return next();
};

router.use(protect);

router.get("/eligibility", getLoanEligibility);
router.post(
  "/documents",
  loanDocumentsUpload,
  loanDocumentsCloudinary,
  normalizeLoanDocuments,
  (req, res) => {
    const documents = Array.isArray(req.body?.documents)
      ? req.body.documents
      : [];
    return sendSuccess(res, {
      statusCode: 201,
      results: documents.length,
      data: { documents },
    });
  },
);

const signatureUpload = uploadSingle("signature", {
  limits: { fileSize: 2 * 1024 * 1024 },
});
const signatureCloudinary = cloudinaryUploadSingle({
  fileField: "signature",
  bodyField: "signatureUpload",
  folder: "crc/loans/signatures",
  resourceType: "image",
});

router.post("/signatures", signatureUpload, signatureCloudinary, (req, res) => {
  const signature = req.body?.signatureUpload || null;
  return sendSuccess(res, {
    statusCode: 201,
    data: { signature },
  });
});

// Borrower (member) flows
router.get("/applications/me", listMyLoanApplications);
router.post(
  "/applications/draft",
  normalizeLoanDocuments,
  createLoanDraft,
);
router.patch(
  "/applications/:applicationId/draft",
  normalizeLoanDocuments,
  loadLoanApplication,
  requireLoanOwnerOrAdmin(),
  updateLoanDraft,
);
router.delete(
  "/applications/:applicationId/draft",
  loadLoanApplication,
  requireLoanOwnerOrAdmin(),
  deleteLoanDraft,
);
router.post(
  "/applications",
  maybeUploadLoanDocuments,
  maybeCloudinaryLoanDocuments,
  normalizeLoanDocuments,
  createLoanApplication,
);
router.post(
  "/applications/:applicationId/edit-requests",
  normalizeLoanDocuments,
  loadLoanApplication,
  requireLoanOwnerOrAdmin(),
  createLoanEditRequest,
);
router.get(
  "/applications/:applicationId",
  loadLoanApplication,
  requireLoanOwnerOrAdmin(),
  getLoanApplication,
);
router.get(
  "/:applicationId/schedule",
  loadLoanApplication,
  requireLoanOwnerOrAdmin(),
  listLoanSchedule,
);
router.post(
  "/:applicationId/repayments",
  loadLoanApplication,
  requireLoanOwnerOrAdmin(),
  recordLoanRepayment,
);

// Admin flows
router.get("/applications", restrictTo("admin"), listLoanApplications);
router.patch(
  "/applications/:applicationId/review",
  restrictTo("admin"),
  loadLoanApplication,
  reviewLoanApplication,
);
router.post(
  "/applications/:applicationId/disburse",
  restrictTo("admin"),
  loadLoanApplication,
  disburseLoan,
);
router.post(
  "/applications/:applicationId/manual-disbursement",
  restrictTo("admin"),
  loadLoanApplication,
  initiateManualLoanDisbursement,
);
router.patch(
  "/applications/:applicationId/finalize-otp",
  restrictTo("admin"),
  loadLoanApplication,
  finalizeLoanDisbursementOtp,
);
router.patch(
  "/applications/:applicationId/manual-disbursement/finalize",
  restrictTo("admin"),
  loadLoanApplication,
  finalizeManualLoanDisbursement,
);
router.patch(
  "/applications/:applicationId/resend-otp",
  restrictTo("admin"),
  loadLoanApplication,
  resendLoanDisbursementOtp,
);
router.patch(
  "/applications/:applicationId/manual-disbursement/resend-otp",
  restrictTo("admin"),
  loadLoanApplication,
  resendManualLoanDisbursementOtp,
);
router.patch(
  "/applications/:applicationId/manual-disbursement/cancel",
  restrictTo("admin"),
  loadLoanApplication,
  cancelManualLoanDisbursement,
);
router.get(
  "/applications/:applicationId/bank-accounts",
  restrictTo("admin"),
  loadLoanApplication,
  listLoanBorrowerBankAccounts,
);
router.patch(
  "/applications/:applicationId/verify-transfer",
  restrictTo("admin"),
  loadLoanApplication,
  verifyLoanDisbursementTransfer,
);

// Guarantor flows (member)
router.get("/guarantor/requests", listMyGuarantorRequests);
router.patch(
  "/guarantor/requests/:guarantorId/respond",
  loadLoanGuarantor,
  requireGuarantorOwnerOrAdmin(),
  respondToGuarantorRequest,
);
router.get("/guarantor/commitments", listMyGuarantorCommitments);
router.get("/guarantor/notifications", listMyGuarantorNotifications);
router.patch(
  "/guarantor/notifications/:notificationId/read",
  markGuarantorNotificationRead,
);

export default router;
