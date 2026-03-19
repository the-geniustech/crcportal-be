import express from "express";

import { protect } from "../controllers/authController.js";
import { listMyPaymentReminders } from "../controllers/paymentReminderController.js";

const router = express.Router();

router.use(protect);

router.get("/me", listMyPaymentReminders);

export default router;
