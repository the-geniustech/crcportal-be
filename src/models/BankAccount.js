import { Schema, ObjectId, model } from "./_shared.js";

export const BankAccountSchema = new Schema(
  {
    // Matches `banking_details` usage in `src/components/withdrawals/WithdrawalRequestForm.tsx`
    userId: { type: ObjectId, ref: "Profile", required: true, index: true },
    bankName: { type: String, required: true, trim: true },
    accountNumber: { type: String, required: true, trim: true },
    accountName: { type: String, required: true, trim: true },
    isPrimary: { type: Boolean, default: false, index: true },
  },
  { timestamps: true },
);

BankAccountSchema.index({ userId: 1, isPrimary: 1 });
// Prevent duplicate account numbers per user (not globally unique).
BankAccountSchema.index({ userId: 1, accountNumber: 1 }, { unique: true });

export const BankAccountModel = model("BankAccount", BankAccountSchema);
