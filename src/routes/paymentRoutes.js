import express from "express";

import { protect } from "../controllers/authController.js";
import {
  initializePaystackPayment,
  initializePaystackBulkPayment,
  paystackWebhook,
  verifyPaystackPayment,
} from "../controllers/paymentController.js";

const router = express.Router();

router.post("/paystack/webhook", paystackWebhook);

router.use(protect);
router.post("/paystack/initialize", initializePaystackPayment);
router.post("/paystack/initialize-bulk", initializePaystackBulkPayment);
router.post("/paystack/verify", verifyPaystackPayment);

export default router;
