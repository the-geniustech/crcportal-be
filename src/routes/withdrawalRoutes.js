import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import {
  approveWithdrawal,
  completeWithdrawal,
  createWithdrawalRequest,
  finalizeWithdrawalOtp,
  resendWithdrawalOtp,
  getMyWithdrawalBalance,
  listMyWithdrawals,
  listWithdrawals,
  markWithdrawalProcessing,
  rejectWithdrawal,
} from "../controllers/withdrawalController.js";

const router = express.Router();

router.use(protect);

router.get("/me/balance", getMyWithdrawalBalance);
router.get("/me", listMyWithdrawals);
router.post("/me", createWithdrawalRequest);

router.get("/", restrictTo("admin", "groupCoordinator"), listWithdrawals);
router.patch(
  "/:id/approve",
  restrictTo("admin", "groupCoordinator"),
  approveWithdrawal,
);
router.patch(
  "/:id/reject",
  restrictTo("admin", "groupCoordinator"),
  rejectWithdrawal,
);
router.patch(
  "/:id/processing",
  restrictTo("admin", "groupCoordinator"),
  markWithdrawalProcessing,
);
router.patch("/:id/complete", restrictTo("admin"), completeWithdrawal);
router.patch("/:id/finalize-otp", restrictTo("admin"), finalizeWithdrawalOtp);
router.patch("/:id/resend-otp", restrictTo("admin"), resendWithdrawalOtp);

export default router;
