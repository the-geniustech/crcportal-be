import { Schema, ObjectId, model } from "./_shared.js";

export const TransactionTypes = [
  "deposit",
  "loan_disbursement",
  "loan_repayment",
  "group_contribution",
  "withdrawal",
  "interest",
];

export const TransactionStatuses = ["success", "pending", "failed"];

export const TransactionSchema = new Schema(
  {
    userId: { type: ObjectId, ref: "Profile", required: true, index: true },

    reference: { type: String, required: true, trim: true },
    amount: { type: Number, required: true, min: 0 },
    type: {
      type: String,
      enum: TransactionTypes,
      required: true,
      index: true,
    },
    status: {
      type: String,
      enum: TransactionStatuses,
      required: true,
      index: true,
    },

    description: { type: String, default: "", trim: true },
    channel: { type: String, default: null, trim: true },

    groupId: { type: ObjectId, ref: "Group", default: null, index: true },
    groupName: { type: String, default: null, trim: true },

    loanId: {
      type: ObjectId,
      ref: "LoanApplication",
      default: null,
      index: true,
    },
    loanName: { type: String, default: null, trim: true },

    metadata: { type: Schema.Types.Mixed, default: null },
    gateway: { type: String, default: "paystack", trim: true, index: true },
  },
  {
    timestamps: { createdAt: "date", updatedAt: "updatedAt" },
  },
);

TransactionSchema.index({ reference: 1 }, { unique: true });
TransactionSchema.index({ userId: 1, date: -1 });
TransactionSchema.index({ type: 1, status: 1, date: -1 });

export const TransactionModel = model("Transaction", TransactionSchema);
