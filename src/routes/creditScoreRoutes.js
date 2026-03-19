import express from "express";

import { protect } from "../controllers/authController.js";
import { getMyCreditScore } from "../controllers/creditScoreController.js";

const router = express.Router();

router.use(protect);

router.get("/me", getMyCreditScore);

export default router;

