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

function isMemberSequenceDuplicateKeyError(error) {
  return (
    error?.code === 11000 &&
    String(error?.message || "").includes(MEMBER_SEQUENCE_KEY)
  );
}

export function formatGroupMemberSerial({
  groupNumber,
  memberNumber,
}) {
  const groupPart = String(groupNumber ?? "").trim() || "0";
  const memberPart = String(memberNumber ?? 0).padStart(4, "0");
  return `CRC/G${groupPart}/${memberPart}`;
}

export async function ensureGroupMemberSequence(groupId, { session = null } = {}) {
  let maxMemberQuery = GroupMembershipModel.findOne(
    { memberNumber: { $type: "number" } },
    { memberNumber: 1 },
  )
    .sort({ memberNumber: -1 });
  if (session) {
    maxMemberQuery = maxMemberQuery.session(session);
  }
  const maxMember = await maxMemberQuery.lean();
  const maxNumber = Number(maxMember?.memberNumber ?? 0);
  const options = { upsert: true, ...(session ? { session } : {}) };
  try {
    await MemberSequenceModel.updateOne(
      { key: MEMBER_SEQUENCE_KEY },
      { $max: { value: maxNumber } },
      options,
    );
  } catch (error) {
    if (!isMemberSequenceDuplicateKeyError(error)) {
      throw error;
    }
    await MemberSequenceModel.updateOne(
      { key: MEMBER_SEQUENCE_KEY },
      { $max: { value: maxNumber } },
      session ? { session } : {},
    );
  }
  return maxNumber;
}

export async function reserveGroupMemberNumbers(
  groupId,
  count,
  { session = null } = {},
) {
  const total = Number(count ?? 0);
  if (!Number.isFinite(total) || total <= 0) {
    let groupQuery =
      groupId
        ? GroupModel.findById(groupId, { groupNumber: 1 })
        : null;
    if (groupQuery && session) {
      groupQuery = groupQuery.session(session);
    }
    const group = groupQuery ? await groupQuery.lean() : null;
    return {
      groupNumber: Number(group?.groupNumber ?? 0),
      start: null,
      end: null,
    };
  }

  await ensureGroupMemberSequence(groupId, { session });
  const sequenceUpdate = { $inc: { value: total } };
  const baseOptions = {
    new: true,
    upsert: true,
    setDefaultsOnInsert: true,
    ...(session ? { session } : {}),
  };
  let sequence = null;
  try {
    let sequenceQuery = MemberSequenceModel.findOneAndUpdate(
      { key: MEMBER_SEQUENCE_KEY },
      sequenceUpdate,
      baseOptions,
    );
    if (session) {
      sequenceQuery = sequenceQuery.session(session);
    }
    sequence = await sequenceQuery.lean();
  } catch (error) {
    if (!isMemberSequenceDuplicateKeyError(error)) {
      throw error;
    }
    let retryQuery = MemberSequenceModel.findOneAndUpdate(
      { key: MEMBER_SEQUENCE_KEY },
      sequenceUpdate,
      { new: true, ...(session ? { session } : {}) },
    );
    if (session) {
      retryQuery = retryQuery.session(session);
    }
    sequence = await retryQuery.lean();
  }

  let groupQuery =
    groupId
      ? GroupModel.findById(groupId, { groupNumber: 1 })
      : null;
  if (groupQuery && session) {
    groupQuery = groupQuery.session(session);
  }
  const group = groupQuery ? await groupQuery.lean() : null;

  if (!sequence) return { groupNumber: null, start: null, end: null };

  const end = Number(sequence.value ?? 0);
  const start = end - total + 1;
  return { groupNumber: Number(group?.groupNumber ?? 0), start, end };
}

export async function assignGroupMemberSerial({
  membership,
  group,
  session = null,
}) {
  if (!membership || !group) return membership;
  if (membership.status !== "active") return membership;

  let memberNumber = Number(membership.memberNumber ?? 0) || null;
  let groupNumber = Number(group.groupNumber ?? 0) || null;
  const joinedAt = membership.joinedAt || new Date();

  if (!memberNumber) {
    const reserved = await reserveGroupMemberNumbers(group._id, 1, { session });
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

  await membership.save({
    validateBeforeSave: true,
    ...(session ? { session } : {}),
  });
  return membership;
}
