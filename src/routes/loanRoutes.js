import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import AppError from "../utils/AppError.js";
import sendSuccess from "../utils/sendSuccess.js";
import {
  createLoanApplication,
  disburseLoan,
  getLoanEligibility,
  getLoanApplication,
  listLoanApplications,
  listLoanSchedule,
  listMyLoanApplications,
  recordLoanRepayment,
  reviewLoanApplication,
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
import { uploadMultiple } from "../middlewares/upload.js";

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

const loanDocumentsUpload = uploadMultiple("documents", 10, {
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
    const inferredType =
      file?.mimetype ??
      (upload?.resourceType === "raw"
        ? "application/pdf"
        : upload?.format
          ? `image/${upload.format}`
          : "application/octet-stream");

    return {
      name: file?.originalname ?? upload?.originalFilename ?? "document",
      type: inferredType,
      size: file?.size ?? upload?.bytes ?? 0,
      status: "uploaded",
      url: upload?.url ?? null,
    };
  });

  req.body.documents = [...existingDocs, ...mapped];

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

// Borrower (member) flows
router.get("/applications/me", listMyLoanApplications);
router.post(
  "/applications",
  maybeUploadLoanDocuments,
  maybeCloudinaryLoanDocuments,
  normalizeLoanDocuments,
  createLoanApplication,
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
