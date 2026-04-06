import express from "express";

import { protect, restrictTo } from "../controllers/authController.js";
import {
  downloadMyStatement,
  downloadMyTransactionReceiptPdf,
  emailMyTransactionReceipt,
  getMyTransaction,
  listMyTransactions,
  listTransactions,
} from "../controllers/transactionController.js";

const router = express.Router();

router.use(protect);

router.get("/me", listMyTransactions);
router.get("/me/statement", downloadMyStatement);
router.get("/me/:id", getMyTransaction);
router.get("/me/:id/receipt/pdf", downloadMyTransactionReceiptPdf);
router.post("/me/:id/receipt", emailMyTransactionReceipt);

router.get("/", restrictTo("admin"), listTransactions);

export default router;
