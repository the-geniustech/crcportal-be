import { hasUserRole } from "./roles.js";

export function canViewFullGroupData({ user, groupMembership }) {
  if (hasUserRole(user, "admin", "groupCoordinator")) {
    return true;
  }
  const membershipRole = String(groupMembership?.role || "");
  const membershipStatus = String(groupMembership?.status || "");
  if (membershipStatus !== "active") return false;
  if (["coordinator", "treasurer", "secretary", "admin"].includes(membershipRole)) {
    return true;
  }
  return false;
}

export function resolveScopedGroupUserId(req) {
  if (canViewFullGroupData(req)) return null;
  return req.user?.profileId ? String(req.user.profileId) : null;
}
