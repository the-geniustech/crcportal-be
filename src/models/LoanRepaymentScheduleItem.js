import { Schema, ObjectId, model } from "./_shared.js";

export const LoanScheduleStatuses = ["paid", "pending", "upcoming", "overdue"];

export const LoanRepaymentScheduleItemSchema = new Schema(
  {
    loanApplicationId: {
      type: ObjectId,
      ref: "LoanApplication",
      required: true,
      index: true,
    },
    installmentNumber: { type: Number, required: true, min: 1 },

    dueDate: { type: Date, required: true, index: true },

    principalAmount: { type: Number, required: true, min: 0 },
    interestAmount: { type: Number, required: true, min: 0 },
    totalAmount: { type: Number, required: true, min: 0 },

    status: {
      type: String,
      enum: LoanScheduleStatuses,
      default: "upcoming",
      index: true,
    },

    paidAt: { type: Date, default: null },
    paidAmount: { type: Number, default: null, min: 0 },
    transactionId: { type: ObjectId, ref: "Transaction", default: null },
    reference: { type: String, default: null, trim: true },
  },
  { timestamps: true },
);

LoanRepaymentScheduleItemSchema.index(
  { loanApplicationId: 1, installmentNumber: 1 },
  { unique: true },
);
LoanRepaymentScheduleItemSchema.index({ loanApplicationId: 1, dueDate: 1 });
LoanRepaymentScheduleItemSchema.index({ loanApplicationId: 1, status: 1, dueDate: 1 });

export const LoanRepaymentScheduleItemModel = model(
  "LoanRepaymentScheduleItem",
  LoanRepaymentScheduleItemSchema,
);

