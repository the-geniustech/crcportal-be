import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";

function resolveSerialYear(joinedAt) {
  const date = joinedAt ? new Date(joinedAt) : new Date();
  const resolved = Number.isNaN(date.getTime()) ? new Date() : date;
  return String(resolved.getFullYear()).slice(-2);
}

export function formatGroupMemberSerial({
  joinedAt,
  groupNumber,
  memberNumber,
}) {
  const year = resolveSerialYear(joinedAt);
  const groupPart = String(groupNumber ?? "").trim() || "0";
  const memberPart = String(memberNumber ?? 0).padStart(4, "0");
  return `${year}/${groupPart}/${memberPart}`;
}

export async function ensureGroupMemberSequence(groupId) {
  if (!groupId) return 0;
  const maxMember = await GroupMembershipModel.findOne(
    { groupId, memberNumber: { $type: "number" } },
    { memberNumber: 1 },
  )
    .sort({ memberNumber: -1 })
    .lean();
  const maxNumber = Number(maxMember?.memberNumber ?? 0);
  if (maxNumber > 0) {
    await GroupModel.updateOne(
      {
        _id: groupId,
        $or: [
          { memberSequence: { $exists: false } },
          { memberSequence: { $lt: maxNumber } },
        ],
      },
      { $set: { memberSequence: maxNumber } },
    );
  }
  return maxNumber;
}

export async function reserveGroupMemberNumbers(groupId, count) {
  if (!groupId) {
    return { groupNumber: null, start: null, end: null };
  }
  const total = Number(count ?? 0);
  if (!Number.isFinite(total) || total <= 0) {
    const group = await GroupModel.findById(groupId, {
      groupNumber: 1,
      memberSequence: 1,
    }).lean();
    return {
      groupNumber: Number(group?.groupNumber ?? 0),
      start: null,
      end: null,
    };
  }

  await ensureGroupMemberSequence(groupId);
  const group = await GroupModel.findByIdAndUpdate(
    groupId,
    { $inc: { memberSequence: total } },
    { new: true, select: { groupNumber: 1, memberSequence: 1 } },
  ).lean();

  if (!group) {
    return { groupNumber: null, start: null, end: null };
  }

  const end = Number(group.memberSequence ?? 0);
  const start = end - total + 1;
  return { groupNumber: Number(group.groupNumber ?? 0), start, end };
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
    joinedAt,
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
