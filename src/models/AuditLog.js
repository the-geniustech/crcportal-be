import { Schema, ObjectId, model } from "./_shared.js";

export const AuditLogSchema = new Schema(
  {
    action: { type: String, required: true, trim: true, index: true },
    entityType: { type: String, required: true, trim: true, index: true },
    entityId: { type: String, required: true, trim: true, index: true },

    actorUserId: { type: ObjectId, ref: "User", default: null, index: true },
    actorProfileId: {
      type: ObjectId,
      ref: "Profile",
      default: null,
      index: true,
    },
    actorRoles: { type: [String], default: [] },

    targetUserId: { type: ObjectId, ref: "User", default: null, index: true },
    targetProfileId: {
      type: ObjectId,
      ref: "Profile",
      default: null,
      index: true,
    },
    groupId: { type: ObjectId, ref: "Group", default: null, index: true },
    membershipId: {
      type: ObjectId,
      ref: "GroupMembership",
      default: null,
      index: true,
    },

    summary: { type: String, default: null, trim: true },
    metadata: { type: Schema.Types.Mixed, default: null },

    ip: { type: String, default: null },
    userAgent: { type: String, default: null },
    requestMethod: { type: String, default: null, trim: true },
    requestPath: { type: String, default: null, trim: true },
  },
  { timestamps: true },
);

AuditLogSchema.index({ createdAt: -1 });
AuditLogSchema.index({ action: 1, createdAt: -1 });
AuditLogSchema.index({ entityType: 1, entityId: 1, createdAt: -1 });
AuditLogSchema.index({ targetUserId: 1, createdAt: -1 });
AuditLogSchema.index({ groupId: 1, createdAt: -1 });

export const AuditLogModel = model("AuditLog", AuditLogSchema);
