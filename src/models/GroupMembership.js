import { Schema, ObjectId, model } from "./_shared.js";

export const GroupRoles = [
  "member",
  "coordinator",
  "treasurer",
  "secretary",
  "admin",
];

export const GroupMembershipStatuses = [
  "pending",
  "active",
  "rejected",
  "inactive",
  "suspended",
];

export const GroupMembershipSchema = new Schema(
  {
    userId: { type: ObjectId, ref: "Profile", required: true, index: true },
    groupId: { type: ObjectId, ref: "Group", required: true, index: true },

    role: { type: String, enum: GroupRoles, default: "member", index: true },
    status: {
      type: String,
      enum: GroupMembershipStatuses,
      default: "active",
      index: true,
      trim: true,
    },

    joinedAt: { type: Date, default: () => new Date(), index: true },
    totalContributed: { type: Number, default: 0, min: 0 },
    memberNumber: { type: Number, default: null, min: 1 },
    memberSerial: { type: String, default: null, trim: true },

    requestedAt: { type: Date, default: null, index: true },
    reviewedBy: { type: ObjectId, ref: "Profile", default: null },
    reviewedAt: { type: Date, default: null, index: true },
    reviewNotes: { type: String, default: null, trim: true },
  },
  { timestamps: true },
);

GroupMembershipSchema.index({ userId: 1, groupId: 1 }, { unique: true });
GroupMembershipSchema.index({ groupId: 1, role: 1 });
GroupMembershipSchema.index(
  { groupId: 1, memberNumber: 1 },
  {
    unique: true,
    partialFilterExpression: { memberNumber: { $type: "number" } },
  },
);
GroupMembershipSchema.index(
  { memberSerial: 1 },
  {
    unique: true,
    partialFilterExpression: { memberSerial: { $type: "string" } },
  },
);

export const GroupMembershipModel = model(
  "GroupMembership",
  GroupMembershipSchema,
);
