import { Schema, ObjectId, model } from "./_shared.js";

export const GroupVoteStatuses = ["active", "closed"];

export const GroupVoteSchema = new Schema(
  {
    groupId: { type: ObjectId, ref: "Group", required: true, index: true },
    title: { type: String, required: true, trim: true },
    description: { type: String, default: "", trim: true },
    status: {
      type: String,
      enum: GroupVoteStatuses,
      default: "active",
      index: true,
      trim: true,
    },
    endsAt: { type: Date, default: null, index: true },
    yesVotes: { type: Number, default: 0, min: 0 },
    noVotes: { type: Number, default: 0, min: 0 },
    totalVoters: { type: Number, default: 0, min: 0 },
    createdBy: { type: ObjectId, ref: "Profile", default: null },
  },
  { timestamps: true },
);

GroupVoteSchema.index({ groupId: 1, status: 1, endsAt: -1 });

export const GroupVoteModel = model("GroupVote", GroupVoteSchema);
