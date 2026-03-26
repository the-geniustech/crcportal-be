import { GroupMembershipModel } from "../models/GroupMembership.js";
import { GroupModel } from "../models/Group.js";

export const BLOCKING_MEMBERSHIP_STATUSES = [
  "active",
  "pending",
  "suspended",
];

export function isGeneralGroup(group) {
  return Number(group?.groupNumber) === 0;
}

export async function findUsersWithNonZeroGroupMembership(
  userIds,
  excludeGroupId,
) {
  const normalizedIds = Array.isArray(userIds)
    ? userIds.map((id) => String(id)).filter(Boolean)
    : [];

  if (normalizedIds.length === 0) return new Set();

  const membershipFilter = {
    userId: { $in: normalizedIds },
    status: { $in: BLOCKING_MEMBERSHIP_STATUSES },
  };
  if (excludeGroupId) {
    membershipFilter.groupId = { $ne: excludeGroupId };
  }

  const memberships = await GroupMembershipModel.find(membershipFilter, {
    userId: 1,
    groupId: 1,
  }).lean();

  if (memberships.length === 0) return new Set();

  const groupIds = [
    ...new Set(memberships.map((m) => String(m.groupId))),
  ];

  const groups = await GroupModel.find(
    { _id: { $in: groupIds } },
    { groupNumber: 1 },
  ).lean();

  const nonZeroGroupIds = new Set(
    groups
      .filter((g) => Number(g.groupNumber) !== 0)
      .map((g) => String(g._id)),
  );

  const blocked = new Set();
  for (const membership of memberships) {
    if (nonZeroGroupIds.has(String(membership.groupId))) {
      blocked.add(String(membership.userId));
    }
  }

  return blocked;
}

export async function hasNonZeroGroupMembership(userId, excludeGroupId) {
  if (!userId) return false;
  const blocked = await findUsersWithNonZeroGroupMembership(
    [String(userId)],
    excludeGroupId,
  );
  return blocked.has(String(userId));
}
