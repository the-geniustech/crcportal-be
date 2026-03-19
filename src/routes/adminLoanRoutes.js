import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import {
  listAdminLoanApplications,
  reviewAdminLoanApplication,
} from "../controllers/adminLoanController.js";

const router = express.Router();

router.use(protect);
router.use(restrictTo("admin", "groupCoordinator"));

router.get("/applications", listAdminLoanApplications);
router.patch("/applications/:applicationId/review", reviewAdminLoanApplication);

export default router;

