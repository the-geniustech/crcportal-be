import { Schema, ObjectId, model } from "./_shared.js";

export const GroupVoteResponseChoices = ["yes", "no"];

export const GroupVoteResponseSchema = new Schema(
  {
    groupId: { type: ObjectId, ref: "Group", required: true, index: true },
    voteId: { type: ObjectId, ref: "GroupVote", required: true, index: true },
    userId: { type: ObjectId, ref: "Profile", required: true, index: true },
    choice: { type: String, enum: GroupVoteResponseChoices, required: true },
  },
  { timestamps: true },
);

GroupVoteResponseSchema.index({ voteId: 1, userId: 1 }, { unique: true });
GroupVoteResponseSchema.index({ groupId: 1, voteId: 1, choice: 1 });

export const GroupVoteResponseModel = model(
  "GroupVoteResponse",
  GroupVoteResponseSchema,
);
