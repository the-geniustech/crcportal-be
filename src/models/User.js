// backend/src/models/User.js
import bcrypt from "bcryptjs";
import { Schema, ObjectId, model } from "./_shared.js";

export const UserRoles = [
  "member",
  "groupCoordinator",
  "groupGuarantor",
  "admin",
];

export const UserSchema = new Schema(
  {
    email: {
      type: String,
      trim: true,
      lowercase: true,
    },
    phone: {
      type: String,
      trim: true,
    },
    pendingEmail: {
      type: String,
      trim: true,
      lowercase: true,
      default: null,
    },
    pendingPhone: {
      type: String,
      trim: true,
      default: null,
    },
    password: {
      type: String,
      required: [true, "Password is required"],
      minlength: 8,
      select: false,
    },
    role: {
      type: String,
      enum: UserRoles,
      default: "member",
      index: true,
    },
    profileId: {
      type: ObjectId,
      ref: "Profile",
      required: true,
      unique: true,
      index: true,
    },

    emailVerifiedAt: {
      type: Date,
      default: null,
    },
    phoneVerifiedAt: {
      type: Date,
      default: null,
    },

    emailVerificationTokenHash: {
      type: String,
      default: null,
      select: false,
    },
    emailVerificationExpiresAt: {
      type: Date,
      default: null,
      select: false,
    },
    emailVerificationSentAt: {
      type: Date,
      default: null,
      select: false,
    },

    phoneOtpHash: {
      type: String,
      default: null,
      select: false,
    },
    phoneOtpExpiresAt: {
      type: Date,
      default: null,
      select: false,
    },
    phoneOtpSentAt: {
      type: Date,
      default: null,
      select: false,
    },

    emailChangeOtpHash: {
      type: String,
      default: null,
      select: false,
    },
    emailChangeOtpExpiresAt: {
      type: Date,
      default: null,
      select: false,
    },
    emailChangeOtpSentAt: {
      type: Date,
      default: null,
      select: false,
    },

    phoneChangeOtpHash: {
      type: String,
      default: null,
      select: false,
    },
    phoneChangeOtpExpiresAt: {
      type: Date,
      default: null,
      select: false,
    },
    phoneChangeOtpSentAt: {
      type: Date,
      default: null,
      select: false,
    },

    twoFactorEnabled: {
      type: Boolean,
      default: false,
      index: true,
    },
    twoFactorSecret: {
      type: String,
      default: null,
      select: false,
    },
    twoFactorEnabledAt: {
      type: Date,
      default: null,
    },

    passwordResetTokenHash: {
      type: String,
      default: null,
      select: false,
    },
    passwordResetExpiresAt: {
      type: Date,
      default: null,
      select: false,
    },
    passwordResetSentAt: {
      type: Date,
      default: null,
      select: false,
    },

    passwordResetPhoneOtpHash: {
      type: String,
      default: null,
      select: false,
    },
    passwordResetPhoneOtpExpiresAt: {
      type: Date,
      default: null,
      select: false,
    },
    passwordResetPhoneOtpSentAt: {
      type: Date,
      default: null,
      select: false,
    },

    active: {
      type: Boolean,
      default: true,
      select: false,
    },
    deletionRequestedAt: {
      type: Date,
      default: null,
    },
    deletionScheduledFor: {
      type: Date,
      default: null,
    },
    deletionCancelledAt: {
      type: Date,
      default: null,
    },
    deletedAt: {
      type: Date,
      default: null,
    },
    passwordChangedAt: {
      type: Date,
      default: null,
      select: false,
    },
  },
  { timestamps: true },
);

UserSchema.pre("validate", function (next) {
  if (!this.email && !this.phone) {
    return next(new Error("Either email or phone is required"));
  }
  return next();
});

UserSchema.pre("save", async function (next) {
  if (!this.isModified("password")) return next();

  this.password = await bcrypt.hash(this.password, 12);

  if (!this.isNew) {
    this.passwordChangedAt = new Date(Date.now() - 1000);
  }

  return next();
});

UserSchema.methods.correctPassword = async function (candidatePassword) {
  return bcrypt.compare(candidatePassword, this.password);
};

UserSchema.methods.changedPasswordAfter = function (jwtIatSeconds) {
  if (!this.passwordChangedAt) return false;
  const changedTimestamp = Math.floor(this.passwordChangedAt.getTime() / 1000);
  return changedTimestamp > jwtIatSeconds;
};

// UserSchema.index(
//   { email: 1 },
//   {
//     unique: true,
//     partialFilterExpression: { email: { $type: "string", $ne: "" } },
//   },
// );
// UserSchema.index(
//   { phone: 1 },
//   {
//     unique: true,
//     partialFilterExpression: { phone: { $type: "string", $ne: "" } },
//   },
// );

export const UserModel = model("User", UserSchema);
