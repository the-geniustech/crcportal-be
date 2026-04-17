import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import {
  approveWithdrawal,
  completeWithdrawal,
  createWithdrawalRequest,
  finalizeManualWithdrawalPayout,
  finalizeWithdrawalOtp,
  resendWithdrawalOtp,
  getMyWithdrawalBalance,
  initiateManualWithdrawalPayout,
  listMyWithdrawals,
  listWithdrawals,
  markWithdrawalProcessing,
  cancelManualWithdrawalPayout,
  rejectWithdrawal,
  resendManualWithdrawalPayoutOtp,
  verifyWithdrawalTransfer,
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
router.patch("/:id/verify-transfer", restrictTo("admin"), verifyWithdrawalTransfer);
router.patch("/:id/finalize-otp", restrictTo("admin"), finalizeWithdrawalOtp);
router.patch("/:id/resend-otp", restrictTo("admin"), resendWithdrawalOtp);
router.patch(
  "/:id/initiate-manual-payout",
  restrictTo("admin"),
  initiateManualWithdrawalPayout,
);
router.patch(
  "/:id/finalize-manual-otp",
  restrictTo("admin"),
  finalizeManualWithdrawalPayout,
);
router.patch(
  "/:id/resend-manual-otp",
  restrictTo("admin"),
  resendManualWithdrawalPayoutOtp,
);
router.patch(
  "/:id/cancel-manual-otp",
  restrictTo("admin"),
  cancelManualWithdrawalPayout,
);

export default router;
