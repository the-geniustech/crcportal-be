import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import {
  ensureAdminLoanAccess,
  listAdminLoanApplications,
  reviewAdminLoanApplication,
} from "../controllers/adminLoanController.js";
import { disburseLoan as disburseLoanController } from "../controllers/loanController.js";
import { loadLoanApplication } from "../middlewares/loanContext.js";

const router = express.Router();

router.use(protect);
router.use(restrictTo("admin", "groupCoordinator"));

router.get("/applications", listAdminLoanApplications);
router.patch("/applications/:applicationId/review", reviewAdminLoanApplication);
router.post(
  "/applications/:applicationId/disburse",
  loadLoanApplication,
  ensureAdminLoanAccess,
  disburseLoanController,
);

export default router;
