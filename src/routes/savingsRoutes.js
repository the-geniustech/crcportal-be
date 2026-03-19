import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import {
  applyMonthlyInterest,
  confirmDeposit,
  createDeposit,
  getMySavingsSummary,
  verifyMyDeposit,
} from "../controllers/savingsController.js";

const router = express.Router();

router.use(protect);

router.get("/me/summary", getMySavingsSummary);

router.post("/deposits", createDeposit);
router.patch("/deposits/confirm", restrictTo("admin"), confirmDeposit);
router.post("/deposits/verify", verifyMyDeposit);

router.post("/interest/apply-monthly", applyMonthlyInterest);

export default router;
