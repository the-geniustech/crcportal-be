import { Schema, model } from "../models/_shared.js";
import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";

const MemberSequenceSchema = new Schema(
  {
    key: { type: String, required: true, unique: true, trim: true },
    value: { type: Number, default: 0, min: 0 },
  },
  { timestamps: true },
);

const MemberSequenceModel = model("MemberSequence", MemberSequenceSchema);
const MEMBER_SEQUENCE_KEY = "group_member";

export function formatGroupMemberSerial({
  groupNumber,
  memberNumber,
}) {
  const groupPart = String(groupNumber ?? "").trim() || "0";
  const memberPart = String(memberNumber ?? 0).padStart(4, "0");
  return `CRC/G${groupPart}/${memberPart}`;
}

export async function ensureGroupMemberSequence(groupId) {
  const maxMember = await GroupMembershipModel.findOne(
    { memberNumber: { $type: "number" } },
    { memberNumber: 1 },
  )
    .sort({ memberNumber: -1 })
    .lean();
  const maxNumber = Number(maxMember?.memberNumber ?? 0);
  const update = maxNumber > 0 ? { $set: { value: maxNumber } } : { $setOnInsert: { value: 0 } };
  await MemberSequenceModel.updateOne(
    {
      key: MEMBER_SEQUENCE_KEY,
      ...(maxNumber > 0
        ? { $or: [{ value: { $exists: false } }, { value: { $lt: maxNumber } }] }
        : {}),
    },
    update,
    { upsert: true },
  );
  return maxNumber;
}

export async function reserveGroupMemberNumbers(groupId, count) {
  const total = Number(count ?? 0);
  if (!Number.isFinite(total) || total <= 0) {
    const group = groupId
      ? await GroupModel.findById(groupId, { groupNumber: 1 }).lean()
      : null;
    return {
      groupNumber: Number(group?.groupNumber ?? 0),
      start: null,
      end: null,
    };
  }

  await ensureGroupMemberSequence(groupId);
  const sequence = await MemberSequenceModel.findOneAndUpdate(
    { key: MEMBER_SEQUENCE_KEY },
    { $inc: { value: total } },
    { new: true, upsert: true, setDefaultsOnInsert: true },
  ).lean();

  const group = groupId
    ? await GroupModel.findById(groupId, { groupNumber: 1 }).lean()
    : null;

  if (!sequence) return { groupNumber: null, start: null, end: null };

  const end = Number(sequence.value ?? 0);
  const start = end - total + 1;
  return { groupNumber: Number(group?.groupNumber ?? 0), start, end };
}

export async function assignGroupMemberSerial({ membership, group }) {
  if (!membership || !group) return membership;
  if (membership.status !== "active") return membership;

  let memberNumber = Number(membership.memberNumber ?? 0) || null;
  let groupNumber = Number(group.groupNumber ?? 0) || null;
  const joinedAt = membership.joinedAt || new Date();

  if (!memberNumber) {
    const reserved = await reserveGroupMemberNumbers(group._id, 1);
    if (reserved.start) memberNumber = reserved.start;
    if (!groupNumber && reserved.groupNumber) {
      groupNumber = reserved.groupNumber;
    }
  }

  if (!memberNumber || !groupNumber) return membership;

  const serial = formatGroupMemberSerial({
    groupNumber,
    memberNumber,
  });

  const needsUpdate =
    membership.memberNumber !== memberNumber ||
    membership.memberSerial !== serial ||
    !membership.joinedAt;

  if (!needsUpdate) return membership;

  membership.memberNumber = memberNumber;
  membership.memberSerial = serial;
  if (!membership.joinedAt) membership.joinedAt = joinedAt;

  await membership.save({ validateBeforeSave: true });
  return membership;
}
