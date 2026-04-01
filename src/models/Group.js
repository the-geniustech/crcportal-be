import { Schema, ObjectId, model } from "./_shared.js";

export const GroupSchema = new Schema(
  {
    // Matches `groups` usage in `src/pages/ContributionGroups.tsx`
    groupNumber: { type: Number, required: true, index: true, unique: true },
    groupName: { type: String, required: true, trim: true },
    description: { type: String, default: "", trim: true },

    coordinatorId: { type: ObjectId, ref: "Profile", default: null },
    coordinatorName: { type: String, default: null, trim: true },
    coordinatorPhone: { type: String, default: null, trim: true },
    coordinatorEmail: {
      type: String,
      default: null,
      trim: true,
      lowercase: true,
    },

    // Matches group creation/details UIs in `src/components/groups/*`
    category: { type: String, default: null, trim: true },
    location: { type: String, default: null, trim: true },
    meetingFrequency: { type: String, default: null, trim: true },
    meetingDay: { type: String, default: null, trim: true },
    rules: { type: String, default: null, trim: true },
    imageUrl: { type: String, default: null, trim: true },
    isOpen: { type: Boolean, default: true, index: true },

    monthlyContribution: { type: Number, required: true, min: 0 },
    totalSavings: { type: Number, default: 0, min: 0 },
    memberCount: { type: Number, default: 0, min: 0 },
    memberSequence: { type: Number, default: 0, min: 0 },
    maxMembers: { type: Number, required: true, min: 1 },

    isSpecial: { type: Boolean, default: false, index: true },
    status: { type: String, default: "active", index: true, trim: true },
  },
  { timestamps: true },
);

GroupSchema.index({ groupName: 1 });
GroupSchema.index({ status: 1, isOpen: 1 });

export const GroupModel = model("Group", GroupSchema);
