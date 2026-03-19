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

router.get("/", restrictTo("admin"), listWithdrawals);
router.patch("/:id/approve", restrictTo("admin"), approveWithdrawal);
router.patch("/:id/reject", restrictTo("admin"), rejectWithdrawal);
router.patch("/:id/processing", restrictTo("admin"), markWithdrawalProcessing);
router.patch("/:id/complete", restrictTo("admin"), completeWithdrawal);

export default router;

