import express from "express";

import { protect } from "../controllers/authController.js";
import {
  createRecurringPayment,
  deleteRecurringPayment,
  listMyRecurringPayments,
  updateRecurringPayment,
} from "../controllers/recurringPaymentController.js";

const router = express.Router();

router.use(protect);

router.get("/me", listMyRecurringPayments);
router.post("/me", createRecurringPayment);
router.patch("/me/:id", updateRecurringPayment);
router.delete("/me/:id", deleteRecurringPayment);

export default router;

