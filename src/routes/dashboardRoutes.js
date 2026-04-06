import express from "express";

import { protect } from "../controllers/authController.js";
import {
  getDashboardSummary,
  getContributionTrend,
} from "../controllers/dashboardController.js";

const router = express.Router();

router.use(protect);

router.get("/summary", getDashboardSummary);
router.get("/contribution-trend", getContributionTrend);

export default router;
