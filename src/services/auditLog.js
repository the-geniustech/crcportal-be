import { AuditLogModel } from "../models/AuditLog.js";
import { normalizeUserRoles } from "../utils/roles.js";

export const AuditActions = {
  ADMIN_MEMBER_CREATE: "admin.member.create",
  ADMIN_MEMBER_UPDATE: "admin.member.update",
  ADMIN_MEMBER_DELETE: "admin.member.delete",
  ADMIN_USER_PROMOTE_ADMIN: "admin.user.promote_admin",
  ADMIN_USER_ROLE_UPDATE: "admin.user.role.update",
};

export const AuditEntityTypes = {
  GROUP_MEMBERSHIP: "groupMembership",
  USER: "user",
};

function resolveClientIp(req) {
  const forwarded = req?.headers?.["x-forwarded-for"];
  if (typeof forwarded === "string" && forwarded.length > 0) {
    return forwarded.split(",")[0].trim();
  }
  return req?.ip || null;
}

function compactValue(value) {
  if (value === undefined) return undefined;
  if (value === null) return null;
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) {
    return value
      .map((entry) => compactValue(entry))
      .filter((entry) => entry !== undefined);
  }
  if (value && typeof value === "object") {
    const out = {};
    for (const [key, entry] of Object.entries(value)) {
      const normalized = compactValue(entry);
      if (normalized !== undefined) {
        out[key] = normalized;
      }
    }
    return out;
  }
  return value;
}

export async function createAuditLog(
  {
    req,
    action,
    entityType,
    entityId,
    targetUserId = null,
    targetProfileId = null,
    groupId = null,
    membershipId = null,
    summary = null,
    metadata = null,
  },
  session = null,
) {
  const payload = {
    action,
    entityType,
    entityId: String(entityId),
    actorUserId: req?.user?._id ?? null,
    actorProfileId: req?.user?.profileId ?? null,
    actorRoles: normalizeUserRoles(req?.user || {}),
    targetUserId: targetUserId || null,
    targetProfileId: targetProfileId || null,
    groupId: groupId || null,
    membershipId: membershipId || null,
    summary: summary || null,
    metadata: compactValue(metadata),
    ip: resolveClientIp(req),
    userAgent: req?.get?.("user-agent") || req?.headers?.["user-agent"] || null,
    requestMethod: req?.method || null,
    requestPath: req?.originalUrl || req?.url || null,
  };

  const log = new AuditLogModel(payload);
  await log.save(session ? { session } : undefined);
  return log;
}
