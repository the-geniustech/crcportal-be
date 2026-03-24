import { Schema, ObjectId, model } from "./_shared.js";
import {
  LoanFacilityTypes,
  LoanInterestRateTypes,
} from "../utils/loanPolicy.js";

export const LoanApplicationStatuses = [
  "pending",
  "under_review",
  "approved",
  "rejected",
  "disbursed",
  "completed",
  "defaulted",
  "cancelled",
];

const LoanDocumentSchema = new Schema(
  {
    name: { type: String, required: true, trim: true },
    type: { type: String, required: true, trim: true },
    size: { type: Number, required: true, min: 0 },
    status: { type: String, required: true, trim: true },
    url: { type: String, default: null, trim: true },
  },
  { _id: false },
);

const LoanGuarantorInfoSchema = new Schema(
  {
    type: { type: String, enum: ["member", "external"], required: true },
    profileId: { type: ObjectId, ref: "Profile", default: null },
    name: { type: String, required: true, trim: true },
    email: { type: String, default: "", trim: true, lowercase: true },
    phone: { type: String, default: "", trim: true },
    relationship: { type: String, default: "", trim: true },
    occupation: { type: String, default: "", trim: true },
    address: { type: String, default: "", trim: true },
    memberSince: { type: String, default: "", trim: true },
    savingsBalance: { type: Number, default: null, min: 0 },
    liabilityPercentage: { type: Number, default: null, min: 1, max: 100 },
  },
  { _id: false },
);

export const LoanApplicationSchema = new Schema(
  {
    userId: { type: ObjectId, ref: "Profile", required: true, index: true },
    groupId: { type: ObjectId, ref: "Group", default: null, index: true },
    groupName: { type: String, default: null, trim: true },

    loanNumber: { type: Number, default: null },
    loanCode: { type: String, default: null, trim: true, index: true },

    loanType: {
      type: String,
      enum: LoanFacilityTypes,
      default: "revolving",
      index: true,
    },

    loanAmount: { type: Number, required: true, min: 0 },
    loanPurpose: { type: String, required: true, trim: true },
    purposeDescription: { type: String, default: "", trim: true },

    repaymentPeriod: { type: Number, required: true, min: 1 },
    interestRate: { type: Number, default: null, min: 0 },
    interestRateType: {
      type: String,
      enum: LoanInterestRateTypes,
      default: "annual",
    },
    monthlyIncome: { type: Number, default: null, min: 0 },

    documents: { type: [LoanDocumentSchema], default: [] },
    guarantors: { type: [LoanGuarantorInfoSchema], default: [] },

    status: {
      type: String,
      enum: LoanApplicationStatuses,
      default: "pending",
      index: true,
    },

    approvedAmount: { type: Number, default: null, min: 0 },
    approvedInterestRate: { type: Number, default: null, min: 0 },
    approvedAt: { type: Date, default: null },

    disbursedAt: { type: Date, default: null, index: true },
    disbursedBy: { type: ObjectId, ref: "Profile", default: null },
    repaymentStartDate: { type: Date, default: null },
    monthlyPayment: { type: Number, default: null, min: 0 },
    totalRepayable: { type: Number, default: null, min: 0 },
    remainingBalance: { type: Number, default: 0, min: 0 },

    reviewNotes: { type: String, default: null, trim: true },
    reviewedBy: { type: ObjectId, ref: "Profile", default: null },
    reviewedAt: { type: Date, default: null },
  },
  { timestamps: true },
);

LoanApplicationSchema.index({ status: 1, createdAt: -1 });
LoanApplicationSchema.index({ userId: 1, createdAt: -1 });
LoanApplicationSchema.index({ loanNumber: 1 }, { unique: true, sparse: true });

export const LoanApplicationModel = model("LoanApplication", LoanApplicationSchema);
