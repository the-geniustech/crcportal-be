import { Schema, ObjectId, model } from "./_shared.js";

export const RecurringPaymentTypes = [
  "deposit",
  "loan_repayment",
  "group_contribution",
];

export const RecurringFrequencies = ["weekly", "bi-weekly", "monthly"];

export const RecurringPaymentSchema = new Schema(
  {
    // Backend should always attach the current user.
    userId: { type: ObjectId, ref: "Profile", required: true, index: true },

    paymentType: {
      type: String,
      enum: RecurringPaymentTypes,
      required: true,
      index: true,
    },
    amount: { type: Number, required: true, min: 0 },
    frequency: {
      type: String,
      enum: RecurringFrequencies,
      required: true,
      index: true,
    },

    startDate: { type: Date, required: true },
    nextPaymentDate: { type: Date, required: true, index: true },
    endDate: { type: Date, default: null },

    groupId: { type: ObjectId, ref: "Group", default: null, index: true },
    groupName: { type: String, default: null, trim: true },
    loanId: {
      type: ObjectId,
      ref: "LoanApplication",
      default: null,
      index: true,
    },
    loanName: { type: String, default: null, trim: true },

    description: { type: String, default: null, trim: true },

    isActive: { type: Boolean, default: true, index: true },

    totalPaymentsMade: { type: Number, default: 0, min: 0 },
    totalAmountPaid: { type: Number, default: 0, min: 0 },
    lastPaymentDate: { type: Date, default: null },
    lastPaymentStatus: { type: String, default: null, trim: true },
  },
  { timestamps: true },
);

RecurringPaymentSchema.index({ userId: 1, isActive: 1, nextPaymentDate: 1 });
RecurringPaymentSchema.index({ paymentType: 1, isActive: 1 });

export const RecurringPaymentModel = model(
  "RecurringPayment",
  RecurringPaymentSchema,
);
