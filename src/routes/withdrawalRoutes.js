import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import {
  approveWithdrawal,
  completeWithdrawal,
  createWithdrawalRequest,
  listMyWithdrawals,
  listWithdrawals,
  markWithdrawalProcessing,
  rejectWithdrawal,
} from "../controllers/withdrawalController.js";

const router = express.Router();

router.use(protect);

router.get("/me", listMyWithdrawals);
router.post("/me", createWithdrawalRequest);

router.get("/", restrictTo("groupCoordinator"), listWithdrawals);
router.patch("/:id/approve", restrictTo("groupCoordinator"), approveWithdrawal);
router.patch("/:id/reject", restrictTo("groupCoordinator"), rejectWithdrawal);
router.patch("/:id/processing", restrictTo("groupCoordinator"), markWithdrawalProcessing);
router.patch("/:id/complete", restrictTo("groupCoordinator"), completeWithdrawal);

export default router;
