import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
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
import {
  loadLoanApplication,
  loadLoanGuarantor,
  requireGuarantorOwnerOrAdmin,
  requireLoanOwnerOrAdmin,
} from "../middlewares/loanContext.js";

const router = express.Router();

router.use(protect);

router.get("/eligibility", getLoanEligibility);

// Borrower (member) flows
router.get("/applications/me", listMyLoanApplications);
router.post("/applications", createLoanApplication);
router.get("/applications/:applicationId", loadLoanApplication, requireLoanOwnerOrAdmin(), getLoanApplication);
router.get("/:applicationId/schedule", loadLoanApplication, requireLoanOwnerOrAdmin(), listLoanSchedule);
router.post("/:applicationId/repayments", loadLoanApplication, requireLoanOwnerOrAdmin(), recordLoanRepayment);

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
