import { Schema, ObjectId, model } from "./_shared.js";
import {
  LoanFacilityTypes,
  LoanInterestRateTypes,
} from "../utils/loanPolicy.js";
import { LoanDocumentTypes } from "../utils/loanDocuments.js";
import {
  normalizeNigerianPhoneValue,
  isNormalizedNigerianPhone,
} from "../utils/phone.js";

const phoneValidator = {
  validator: isNormalizedNigerianPhone,
  message: "Phone number must be in +234 803 123 4567 format.",
};

export const LoanApplicationStatuses = [
  "draft",
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
    documentType: {
      type: String,
      enum: LoanDocumentTypes,
      default: null,
      trim: true,
    },
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
    type: {
      type: String,
      enum: ["member", "external"],
      required: function requiredType() {
        const parent = typeof this.ownerDocument === "function" ? this.ownerDocument() : null;
        return parent?.status !== "draft";
      },
    },
    profileId: { type: ObjectId, ref: "Profile", default: null },
    name: {
      type: String,
      required: function requiredName() {
        const parent = typeof this.ownerDocument === "function" ? this.ownerDocument() : null;
        return parent?.status !== "draft";
      },
      trim: true,
    },
    email: { type: String, default: "", trim: true, lowercase: true },
    phone: {
      type: String,
      default: "",
      trim: true,
      set: normalizeNigerianPhoneValue,
      validate: phoneValidator,
    },
    relationship: { type: String, default: "", trim: true },
    occupation: { type: String, default: "", trim: true },
    address: { type: String, default: "", trim: true },
    memberSince: { type: String, default: "", trim: true },
    savingsBalance: { type: Number, default: null, min: 0 },
    liabilityPercentage: { type: Number, default: null, min: 1, max: 100 },
    signature: {
      method: {
        type: String,
        enum: ["text", "draw", "upload"],
        default: null,
      },
      text: { type: String, default: "", trim: true },
      font: { type: String, default: "", trim: true },
      imageUrl: { type: String, default: null, trim: true },
      imagePublicId: { type: String, default: null, trim: true },
      signedAt: { type: Date, default: null },
    },
  },
  { _id: false },
);

const ManualLoanDisbursementSchema = new Schema(
  {
    status: {
      type: String,
      enum: ["pending_otp", "completed"],
      default: null,
    },
    method: {
      type: String,
      enum: [
        "cash",
        "bank_transfer",
        "bank_settlement",
        "cheque",
        "pos",
        "other",
      ],
      default: null,
    },
    amount: { type: Number, default: null, min: 0 },
    externalReference: { type: String, default: null, trim: true },
    occurredAt: { type: Date, default: null },
    repaymentStartDate: { type: Date, default: null },
    notes: { type: String, default: null, trim: true },
    initiatedByUserId: { type: ObjectId, ref: "User", default: null },
    initiatedBy: { type: ObjectId, ref: "Profile", default: null },
    authorizedBy: { type: ObjectId, ref: "Profile", default: null },
    initiatedAt: { type: Date, default: null },
    completedAt: { type: Date, default: null },
    otpChannel: {
      type: String,
      enum: ["phone", "email"],
      default: null,
    },
    otpRecipient: { type: String, default: null, trim: true },
    otpBackupChannels: {
      type: [{ type: String, enum: ["phone", "email"] }],
      default: [],
    },
    otpSentAt: { type: Date, default: null },
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

    loanAmount: {
      type: Number,
      required: function requiredLoanAmount() {
        return this.status !== "draft";
      },
      min: 0,
    },
    loanPurpose: {
      type: String,
      required: function requiredLoanPurpose() {
        return this.status !== "draft";
      },
      trim: true,
    },
    purposeDescription: { type: String, default: "", trim: true },

    repaymentPeriod: {
      type: Number,
      required: function requiredRepayment() {
        return this.status !== "draft";
      },
      min: 1,
    },
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

    draftStep: { type: Number, default: 0 },
    draftLastSavedAt: { type: Date, default: null },

    approvedAmount: { type: Number, default: null, min: 0 },
    approvedInterestRate: { type: Number, default: null, min: 0 },
    approvedAt: { type: Date, default: null },

    disbursementBankAccountId: {
      type: ObjectId,
      ref: "BankAccount",
      default: null,
      index: true,
    },
    disbursementBankName: { type: String, default: null, trim: true },
    disbursementBankCode: { type: String, default: null, trim: true },
    disbursementAccountNumber: { type: String, default: null, trim: true },
    disbursementAccountName: { type: String, default: null, trim: true },

    disbursedAt: { type: Date, default: null, index: true },
    disbursedBy: { type: ObjectId, ref: "Profile", default: null },
    repaymentStartDate: { type: Date, default: null },
    monthlyPayment: { type: Number, default: null, min: 0 },
    totalRepayable: { type: Number, default: null, min: 0 },
    remainingBalance: { type: Number, default: 0, min: 0 },

    payoutReference: { type: String, default: null, trim: true },
    payoutGateway: { type: String, default: null, trim: true },
    payoutTransferCode: { type: String, default: null, trim: true },
    payoutStatus: { type: String, default: null, trim: true },
    payoutOtpResentAt: { type: Date, default: null },
    manualDisbursement: { type: ManualLoanDisbursementSchema, default: null },
    manualDisbursementOtpHash: {
      type: String,
      default: null,
      select: false,
    },
    manualDisbursementOtpExpiresAt: {
      type: Date,
      default: null,
      select: false,
    },

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
