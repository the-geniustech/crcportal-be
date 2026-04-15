// backend/src/models/Profile.js
import { Schema, model } from "./_shared.js";
import {
  normalizeNigerianPhoneValue,
  isNormalizedNigerianPhone,
} from "../utils/phone.js";

const phoneValidator = {
  validator: isNormalizedNigerianPhone,
  message: "Phone number must be in +234 803 123 4567 format.",
};

export const MembershipStatuses = [
  "pending",
  "active",
  "suspended",
  "inactive",
];

const AvatarSchema = new Schema(
  {
    url: { type: String, required: true, trim: true },
    publicId: { type: String, required: true, trim: true, index: true },
    width: { type: Number, default: null },
    height: { type: Number, default: null },
    format: { type: String, default: null, trim: true },
    bytes: { type: Number, default: null },
    originalFilename: { type: String, default: null, trim: true },
  },
  { _id: false },
);

const ContributionSettingsSchema = new Schema(
  {
    year: { type: Number, default: null },
    units: { type: Schema.Types.Mixed, default: null },
    updatedAt: { type: Date, default: null },
  },
  { _id: false },
);

export const ProfileSchema = new Schema(
  {
    email: { type: String, trim: true, lowercase: true },
    fullName: { type: String, default: null, trim: true },
    phone: {
      type: String,
      trim: true,
      set: normalizeNigerianPhoneValue,
      validate: phoneValidator,
    },
    dateOfBirth: { type: Date, default: null },
    address: { type: String, default: null, trim: true },
    city: { type: String, default: null, trim: true },
    state: { type: String, default: null, trim: true },
    occupation: { type: String, default: null, trim: true },
    employer: { type: String, default: null, trim: true },
    nextOfKinName: { type: String, default: null, trim: true },
    nextOfKinPhone: {
      type: String,
      default: null,
      trim: true,
      set: normalizeNigerianPhoneValue,
      validate: phoneValidator,
    },
    nextOfKinRelationship: { type: String, default: null, trim: true },
    membershipStatus: {
      type: String,
      enum: MembershipStatuses,
      default: "pending",
    },
    avatar: { type: AvatarSchema, default: null },
    contributionSettings: { type: ContributionSettingsSchema, default: null },
  },
  { timestamps: true },
);

// ProfileSchema.index(
//   { email: 1 },
//   {
//     unique: true,
//     partialFilterExpression: { email: { $type: "string", $ne: "" } },
//   },
// );
// ProfileSchema.index(
//   { phone: 1 },
//   {
//     unique: true,
//     partialFilterExpression: { phone: { $type: "string", $ne: "" } },
//   },
// );
ProfileSchema.index({ membershipStatus: 1 });

export const ProfileModel = model("Profile", ProfileSchema);
