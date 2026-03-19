import express from "express";

import { protect } from "../controllers/authController.js";
import {
  createMyBankAccount,
  deleteMyBankAccount,
  listMyBankAccounts,
  updateMyBankAccount,
} from "../controllers/bankAccountController.js";

const router = express.Router();

router.use(protect);

router.get("/me", listMyBankAccounts);
router.post("/me", createMyBankAccount);
router.patch("/me/:id", updateMyBankAccount);
router.delete("/me/:id", deleteMyBankAccount);

export default router;

