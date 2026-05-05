import { Schema, ObjectId, model } from "./_shared.js";

export const FormPaymentTypes = [
  "membership_registration",
  "revolving_loan",
  "bridging_loan",
  "soft_loan",
  "special_loan",
];

export const FormPaymentStatuses = ["pending", "paid", "defaulted"];

export const FormPaymentSources = ["GroupMembership", "LoanApplication"];

export const FormPaymentSchema = new Schema(
  {
    userId: { type: ObjectId, ref: "Profile", required: true, index: true },
    userAccountId: { type: ObjectId, ref: "User", default: null, index: true },
    groupId: { type: ObjectId, ref: "Group", default: null, index: true },
    groupName: { type: String, default: null, trim: true },

    memberName: { type: String, default: null, trim: true },
    memberEmail: { type: String, default: null, trim: true, lowercase: true },
    memberPhone: { type: String, default: null, trim: true },

    formType: {
      type: String,
      enum: FormPaymentTypes,
      required: true,
      index: true,
    },
    formCategory: {
      type: String,
      enum: ["membership", "loan"],
      required: true,
      index: true,
    },
    formLabel: { type: String, required: true, trim: true },
    amount: { type: Number, required: true, min: 0 },
    currency: { type: String, default: "NGN", trim: true },

    paymentStatus: {
      type: String,
      enum: FormPaymentStatuses,
      default: "pending",
      index: true,
    },

    sourceModel: {
      type: String,
      enum: FormPaymentSources,
      required: true,
      index: true,
    },
    sourceId: { type: ObjectId, required: true, index: true },
    sourceReference: { type: String, default: null, trim: true },

    transactionId: { type: ObjectId, ref: "Transaction", default: null },
    transactionReference: {
      type: String,
      default: null,
      trim: true,
      index: true,
    },

    submittedAt: { type: Date, default: () => new Date(), index: true },
    reviewedAt: { type: Date, default: null, index: true },
    reviewedBy: { type: ObjectId, ref: "Profile", default: null },
    notes: { type: String, default: null, trim: true },

    formDetails: { type: Schema.Types.Mixed, default: {} },
  },
  { timestamps: true },
);

FormPaymentSchema.index(
  { sourceModel: 1, sourceId: 1, formType: 1 },
  { unique: true },
);
FormPaymentSchema.index({ paymentStatus: 1, submittedAt: -1 });
FormPaymentSchema.index({ formType: 1, submittedAt: -1 });
FormPaymentSchema.index({ groupId: 1, submittedAt: -1 });

export const FormPaymentModel = model("FormPayment", FormPaymentSchema);
