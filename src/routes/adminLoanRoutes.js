import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import AppError from "../utils/AppError.js";
import {
  downloadAdminLoanRepaymentReceiptPdf,
  ensureAdminLoanAccess,
  emailAdminLoanRepaymentReceipt,
  downloadAdminLoanApplicationPdf,
  emailAdminLoanApplicationPdf,
  exportAdminLoanRepaymentHistory,
  exportAdminLoanApplications,
  listAdminLoanApplications,
  listAdminLoanRepayments,
  listAdminLoanTracker,
  recordAdminLoanRepayment,
  reconcileAdminLoanApplication,
  reviewAdminLoanApplication,
  reviewLoanEditRequest,
  uploadAdminLoanRepaymentReceipt,
} from "../controllers/adminLoanController.js";
import {
  cancelManualLoanDisbursement,
  disburseLoan as disburseLoanController,
  finalizeLoanDisbursementOtp,
  finalizeManualLoanDisbursement,
  initiateManualLoanDisbursement,
  listLoanBorrowerBankAccounts,
  resendLoanDisbursementOtp,
  resendManualLoanDisbursementOtp,
  verifyLoanDisbursementTransfer,
} from "../controllers/loanController.js";
import { cloudinaryUploadSingle } from "../middlewares/cloudinaryUpload.js";
import { loadLoanApplication } from "../middlewares/loanContext.js";
import { uploadSingle } from "../middlewares/upload.js";

const router = express.Router();

const repaymentReceiptFileFilter = (req, file, cb) => {
  if (
    file.mimetype?.startsWith("image/") ||
    file.mimetype === "application/pdf"
  ) {
    return cb(null, true);
  }
  return cb(new AppError("Only image or PDF uploads are allowed", 400));
};

const repaymentReceiptUpload = uploadSingle("receipt", {
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: repaymentReceiptFileFilter,
});

const repaymentReceiptCloudinary = cloudinaryUploadSingle({
  fileField: "receipt",
  bodyField: "receiptUpload",
  folder: "crc/loans/repayment-receipts",
  resourceType: "auto",
});

router.use(protect);
router.use(restrictTo("admin", "groupCoordinator"));

router.get("/applications", listAdminLoanApplications);
router.get("/applications/export", exportAdminLoanApplications);
router.get("/tracker", listAdminLoanTracker);
router.post(
  "/repayment-receipts",
  repaymentReceiptUpload,
  repaymentReceiptCloudinary,
  uploadAdminLoanRepaymentReceipt,
);
router.get(
  "/applications/:applicationId/repayments",
  loadLoanApplication,
  ensureAdminLoanAccess,
  listAdminLoanRepayments,
);
router.get(
  "/applications/:applicationId/repayments/export",
  loadLoanApplication,
  ensureAdminLoanAccess,
  exportAdminLoanRepaymentHistory,
);
router.get(
  "/applications/:applicationId/repayments/:repaymentId/receipt/pdf",
  loadLoanApplication,
  ensureAdminLoanAccess,
  downloadAdminLoanRepaymentReceiptPdf,
);
router.post(
  "/applications/:applicationId/repayments/:repaymentId/receipt/email",
  loadLoanApplication,
  ensureAdminLoanAccess,
  emailAdminLoanRepaymentReceipt,
);
router.get(
  "/applications/:applicationId/pdf",
  loadLoanApplication,
  ensureAdminLoanAccess,
  downloadAdminLoanApplicationPdf,
);
router.get(
  "/applications/:applicationId/bank-accounts",
  loadLoanApplication,
  ensureAdminLoanAccess,
  listLoanBorrowerBankAccounts,
);
router.post(
  "/applications/:applicationId/email",
  loadLoanApplication,
  ensureAdminLoanAccess,
  emailAdminLoanApplicationPdf,
);
router.patch("/applications/:applicationId/review", reviewAdminLoanApplication);
router.patch(
  "/applications/:applicationId/reconcile",
  loadLoanApplication,
  ensureAdminLoanAccess,
  reconcileAdminLoanApplication,
);
router.patch(
  "/applications/:applicationId/edit-requests/:requestId",
  loadLoanApplication,
  ensureAdminLoanAccess,
  reviewLoanEditRequest,
);
router.post(
  "/applications/:applicationId/disburse",
  restrictTo("admin"),
  loadLoanApplication,
  ensureAdminLoanAccess,
  disburseLoanController,
);
router.post(
  "/applications/:applicationId/manual-disbursement",
  restrictTo("admin"),
  loadLoanApplication,
  ensureAdminLoanAccess,
  initiateManualLoanDisbursement,
);
router.patch(
  "/applications/:applicationId/finalize-otp",
  restrictTo("admin"),
  loadLoanApplication,
  ensureAdminLoanAccess,
  finalizeLoanDisbursementOtp,
);
router.patch(
  "/applications/:applicationId/manual-disbursement/finalize",
  restrictTo("admin"),
  loadLoanApplication,
  ensureAdminLoanAccess,
  finalizeManualLoanDisbursement,
);
router.patch(
  "/applications/:applicationId/resend-otp",
  restrictTo("admin"),
  loadLoanApplication,
  ensureAdminLoanAccess,
  resendLoanDisbursementOtp,
);
router.patch(
  "/applications/:applicationId/manual-disbursement/resend-otp",
  restrictTo("admin"),
  loadLoanApplication,
  ensureAdminLoanAccess,
  resendManualLoanDisbursementOtp,
);
router.patch(
  "/applications/:applicationId/manual-disbursement/cancel",
  restrictTo("admin"),
  loadLoanApplication,
  ensureAdminLoanAccess,
  cancelManualLoanDisbursement,
);
router.patch(
  "/applications/:applicationId/verify-transfer",
  loadLoanApplication,
  ensureAdminLoanAccess,
  verifyLoanDisbursementTransfer,
);
router.post(
  "/applications/:applicationId/manual-repayment",
  loadLoanApplication,
  ensureAdminLoanAccess,
  recordAdminLoanRepayment,
);

export default router;

/*
Act as a senior full-stack engineer with 30+ years of experience building scalable SaaS platforms, so let's continue creating/building professional tailored scripts for the project.
- Create a script that takes in this type attached loan tracking (per group) file and seed the members loan applications into the database, ensuring all the necessary validations and error handling are in place.

Notes about the attached loan tracking file (.xlsx):
- The file

Ensure all the requirements are consistent with backend <=> frontend flows and implemented in an highly professional way and industry standard of doing things.

You can improvice whenever needed, just ensure you don't digress from the context of this project and you're always highly professional as much as possible.
*/
