import express from "express";
import cors from "cors";
import morgan from "morgan";
import dotenv from "dotenv";

dotenv.config();

import AppError from "./utils/AppError.js";
import globalErrorHandler from "./controllers/errorController.js";
import authRoutes from "./routes/authRoutes.js";
import userRoutes from "./routes/userRoutes.js";
import groupRoutes from "./routes/groupRoutes.js";
import loanRoutes from "./routes/loanRoutes.js";
import savingsRoutes from "./routes/savingsRoutes.js";
import transactionRoutes from "./routes/transactionRoutes.js";
import withdrawalRoutes from "./routes/withdrawalRoutes.js";
import bankAccountRoutes from "./routes/bankAccountRoutes.js";
import recurringPaymentRoutes from "./routes/recurringPaymentRoutes.js";
import paymentRoutes from "./routes/paymentRoutes.js";
import paymentReminderRoutes from "./routes/paymentReminderRoutes.js";
import meetingRoutes from "./routes/meetingRoutes.js";
import creditScoreRoutes from "./routes/creditScoreRoutes.js";
import adminRoutes from "./routes/adminRoutes.js";
import notificationRoutes from "./routes/notificationRoutes.js";
import dashboardRoutes from "./routes/dashboardRoutes.js";

const app = express();

app.set("trust proxy", true);

app.use(
  cors({
    origin: process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(",") : true,
    credentials: true,
  }),
);

if (process.env.NODE_ENV === "development") {
  app.use(morgan("dev"));
}

app.use(
  express.json({
    limit: "10kb",
    verify: (req, _res, buf) => {
      req.rawBody = buf;
    },
  }),
);

app.get("/health", (req, res) => {
  res.status(200).json({ status: "success", data: { ok: true } });
});

app.use("/api/v1/auth", authRoutes);
app.use("/api/v1/users", userRoutes);
app.use("/api/v1/groups", groupRoutes);
app.use("/api/v1/loans", loanRoutes);
app.use("/api/v1/savings", savingsRoutes);
app.use("/api/v1/transactions", transactionRoutes);
app.use("/api/v1/withdrawals", withdrawalRoutes);
app.use("/api/v1/bank-accounts", bankAccountRoutes);
app.use("/api/v1/recurring-payments", recurringPaymentRoutes);
app.use("/api/v1/payments", paymentRoutes);
app.use("/api/v1/payment-reminders", paymentReminderRoutes);
app.use("/api/v1/meetings", meetingRoutes);
app.use("/api/v1/credit-scores", creditScoreRoutes);
app.use("/api/v1/admin", adminRoutes);
app.use("/api/v1/notifications", notificationRoutes);
app.use("/api/v1/dashboard", dashboardRoutes);

app.all("*", (req, res, next) => {
  next(new AppError(`Can't find ${req.originalUrl} on this server`, 404));
});

app.use(globalErrorHandler);

export default app;
