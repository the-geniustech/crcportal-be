import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import {
  ensureAdminLoanAccess,
  downloadAdminLoanApplicationPdf,
  emailAdminLoanApplicationPdf,
  listAdminLoanApplications,
  reviewAdminLoanApplication,
} from "../controllers/adminLoanController.js";
import {
  disburseLoan as disburseLoanController,
  finalizeLoanDisbursementOtp,
  listLoanBorrowerBankAccounts,
  resendLoanDisbursementOtp,
  verifyLoanDisbursementTransfer,
} from "../controllers/loanController.js";
import { loadLoanApplication } from "../middlewares/loanContext.js";

const router = express.Router();

router.use(protect);
router.use(restrictTo("admin", "groupCoordinator"));

router.get("/applications", listAdminLoanApplications);
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
router.post(
  "/applications/:applicationId/disburse",
  restrictTo("admin"),
  loadLoanApplication,
  ensureAdminLoanAccess,
  disburseLoanController,
);
router.patch(
  "/applications/:applicationId/finalize-otp",
  restrictTo("admin"),
  loadLoanApplication,
  ensureAdminLoanAccess,
  finalizeLoanDisbursementOtp,
);
router.patch(
  "/applications/:applicationId/resend-otp",
  restrictTo("admin"),
  loadLoanApplication,
  ensureAdminLoanAccess,
  resendLoanDisbursementOtp,
);
router.patch(
  "/applications/:applicationId/verify-transfer",
  loadLoanApplication,
  ensureAdminLoanAccess,
  verifyLoanDisbursementTransfer,
);

export default router;
