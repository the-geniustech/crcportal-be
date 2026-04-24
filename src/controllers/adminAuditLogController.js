import mongoose from "mongoose";

import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";

import { AuditLogModel } from "../models/AuditLog.js";
import { GroupModel } from "../models/Group.js";
import { ProfileModel } from "../models/Profile.js";
import { AuditActions, AuditEntityTypes } from "../services/auditLog.js";

const DEFAULT_LIMIT = 15;
const MAX_LIMIT = 100;
const CSV_BOM = "\uFEFF";

const AUDIT_ACTION_OPTIONS = [
  {
    value: AuditActions.ADMIN_MEMBER_CREATE,
    label: "Member Created",
  },
  {
    value: AuditActions.ADMIN_MEMBER_UPDATE,
    label: "Member Updated",
  },
  {
    value: AuditActions.ADMIN_MEMBER_DELETE,
    label: "Member Deleted",
  },
  {
    value: AuditActions.ADMIN_USER_PROMOTE_ADMIN,
    label: "Admin Promotion",
  },
  {
    value: AuditActions.ADMIN_USER_ROLE_UPDATE,
    label: "User Role Updated",
  },
];

const AUDIT_ENTITY_OPTIONS = [
  { value: AuditEntityTypes.GROUP_MEMBERSHIP, label: "Member Record" },
  { value: AuditEntityTypes.USER, label: "User Account" },
];

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function parsePagination(req) {
  const page = Math.max(1, parseInt(String(req.query?.page ?? "1"), 10) || 1);
  const limit = clamp(
    parseInt(String(req.query?.limit ?? String(DEFAULT_LIMIT)), 10) ||
      DEFAULT_LIMIT,
    1,
    MAX_LIMIT,
  );

  return {
    page,
    limit,
    skip: (page - 1) * limit,
  };
}

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function optionalObjectIdString(fieldPath) {
  return {
    $cond: [
      { $ifNull: [fieldPath, false] },
      { $toString: fieldPath },
      null,
    ],
  };
}

function parseDateFilter(value, label, { endOfDay = false } = {}) {
  if (!value) return null;
  const raw = String(value).trim();
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    throw new AppError(`Invalid ${label} date`, 400);
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    if (endOfDay) {
      parsed.setHours(23, 59, 59, 999);
    } else {
      parsed.setHours(0, 0, 0, 0);
    }
  }
  return parsed;
}

function parseAuditLogFilters(req) {
  const search = String(req.query?.search ?? "").trim();
  const action = String(req.query?.action ?? "all").trim();
  const entityType = String(req.query?.entityType ?? "all").trim();
  const groupId = String(req.query?.groupId ?? "").trim();
  const from = parseDateFilter(req.query?.from, "from");
  const to = parseDateFilter(req.query?.to, "to", { endOfDay: true });

  if (
    action !== "all" &&
    !AUDIT_ACTION_OPTIONS.some((option) => option.value === action)
  ) {
    throw new AppError("Invalid audit action filter", 400);
  }

  if (
    entityType !== "all" &&
    !AUDIT_ENTITY_OPTIONS.some((option) => option.value === entityType)
  ) {
    throw new AppError("Invalid audit entity filter", 400);
  }

  if (groupId && !mongoose.Types.ObjectId.isValid(groupId)) {
    throw new AppError("Invalid group filter", 400);
  }

  if (from && to && from.getTime() > to.getTime()) {
    throw new AppError("The from date cannot be later than the to date", 400);
  }

  return {
    search,
    action,
    entityType,
    groupId,
    from,
    to,
  };
}

function csvEscape(value) {
  const raw = String(value ?? "");
  if (/[",\n]/.test(raw)) {
    return `"${raw.replace(/"/g, '""')}"`;
  }
  return raw;
}

function buildCsv(rows) {
  return rows.map((row) => row.map((value) => csvEscape(value)).join(",")).join("\n");
}

function formatDateValue(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return new Intl.DateTimeFormat("en-NG", {
    day: "numeric",
    month: "short",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function getAuditActionLabel(action) {
  return (
    AUDIT_ACTION_OPTIONS.find((option) => option.value === action)?.label ||
    String(action || "Unknown")
  );
}

function getAuditEntityLabel(entityType) {
  return (
    AUDIT_ENTITY_OPTIONS.find((option) => option.value === entityType)?.label ||
    String(entityType || "Unknown")
  );
}

function buildAuditLogProjection() {
  return {
    $project: {
      _id: 0,
      id: { $toString: "$_id" },
      action: "$action",
      entityType: "$entityType",
      entityId: "$entityId",
      summary: "$summary",
      createdAt: "$createdAt",
      actor: {
        userId: optionalObjectIdString("$actorUserId"),
        profileId: optionalObjectIdString("$actorProfileId"),
        fullName: { $ifNull: ["$actorProfile.fullName", "System"] },
        email: { $ifNull: ["$actorProfile.email", null] },
        phone: { $ifNull: ["$actorProfile.phone", null] },
        roles: { $ifNull: ["$actorRoles", []] },
      },
      target: {
        userId: optionalObjectIdString("$targetUserId"),
        profileId: optionalObjectIdString("$targetProfileId"),
        fullName: { $ifNull: ["$targetProfile.fullName", null] },
        email: { $ifNull: ["$targetProfile.email", null] },
        phone: { $ifNull: ["$targetProfile.phone", null] },
      },
      group: {
        id: optionalObjectIdString("$groupId"),
        name: { $ifNull: ["$group.groupName", null] },
        number: {
          $cond: [
            { $ne: ["$group.groupNumber", null] },
            "$group.groupNumber",
            null,
          ],
        },
      },
      request: {
        method: { $ifNull: ["$requestMethod", null] },
        path: { $ifNull: ["$requestPath", null] },
        ip: { $ifNull: ["$ip", null] },
        userAgent: { $ifNull: ["$userAgent", null] },
      },
      metadata: { $ifNull: ["$metadata", null] },
    },
  };
}

function buildAuditLogPipeline(filters) {
  const match = {};

  if (filters.action !== "all") {
    match.action = filters.action;
  }
  if (filters.entityType !== "all") {
    match.entityType = filters.entityType;
  }
  if (filters.groupId) {
    match.groupId = new mongoose.Types.ObjectId(filters.groupId);
  }
  if (filters.from || filters.to) {
    match.createdAt = {};
    if (filters.from) match.createdAt.$gte = filters.from;
    if (filters.to) match.createdAt.$lte = filters.to;
  }

  const profileCollection = ProfileModel.collection.name;
  const groupCollection = GroupModel.collection.name;

  const pipeline = [
    { $match: match },
    {
      $lookup: {
        from: profileCollection,
        localField: "actorProfileId",
        foreignField: "_id",
        as: "actorProfile",
      },
    },
    { $unwind: { path: "$actorProfile", preserveNullAndEmptyArrays: true } },
    {
      $lookup: {
        from: profileCollection,
        localField: "targetProfileId",
        foreignField: "_id",
        as: "targetProfile",
      },
    },
    { $unwind: { path: "$targetProfile", preserveNullAndEmptyArrays: true } },
    {
      $lookup: {
        from: groupCollection,
        localField: "groupId",
        foreignField: "_id",
        as: "group",
      },
    },
    { $unwind: { path: "$group", preserveNullAndEmptyArrays: true } },
  ];

  if (filters.search) {
    const pattern = escapeRegex(filters.search);
    pipeline.push({
      $match: {
        $or: [
          { action: { $regex: pattern, $options: "i" } },
          { entityType: { $regex: pattern, $options: "i" } },
          { entityId: { $regex: pattern, $options: "i" } },
          { summary: { $regex: pattern, $options: "i" } },
          { "actorProfile.fullName": { $regex: pattern, $options: "i" } },
          { "actorProfile.email": { $regex: pattern, $options: "i" } },
          { "actorProfile.phone": { $regex: pattern, $options: "i" } },
          { "targetProfile.fullName": { $regex: pattern, $options: "i" } },
          { "targetProfile.email": { $regex: pattern, $options: "i" } },
          { "targetProfile.phone": { $regex: pattern, $options: "i" } },
          { "group.groupName": { $regex: pattern, $options: "i" } },
          { requestPath: { $regex: pattern, $options: "i" } },
          { requestMethod: { $regex: pattern, $options: "i" } },
          { ip: { $regex: pattern, $options: "i" } },
        ],
      },
    });
  }

  return pipeline;
}

function buildAuditLogRecordPipeline({ skip = 0, limit = DEFAULT_LIMIT, paginate = true } = {}) {
  const pipeline = [{ $sort: { createdAt: -1, _id: -1 } }];
  if (paginate) {
    pipeline.push({ $skip: skip }, { $limit: limit });
  }
  pipeline.push(buildAuditLogProjection());
  return pipeline;
}

function buildAuditLogExportRows(logs) {
  return logs.map((log) => {
    const metadata =
      log?.metadata && typeof log.metadata === "object" ? log.metadata : {};
    const member =
      metadata?.member && typeof metadata.member === "object"
        ? metadata.member
        : {};
    const changedFields = Array.isArray(metadata?.changedFields)
      ? metadata.changedFields.map((entry) => String(entry))
      : [];
    const addedRoles = Array.isArray(metadata?.addedRoles)
      ? metadata.addedRoles.map((entry) => String(entry))
      : [];
    const removedRoles = Array.isArray(metadata?.removedRoles)
      ? metadata.removedRoles.map((entry) => String(entry))
      : [];

    return [
      formatDateValue(log.createdAt),
      getAuditActionLabel(log.action),
      getAuditEntityLabel(log.entityType),
      log.entityId,
      log.summary || "-",
      log.actor?.fullName || "System",
      log.actor?.email || "-",
      log.actor?.phone || "-",
      Array.isArray(log.actor?.roles) ? log.actor.roles.join(", ") : "-",
      log.target?.fullName || "-",
      log.target?.email || "-",
      log.target?.phone || "-",
      log.group?.name || "System-wide",
      log.group?.number ?? "-",
      member?.memberSerial || "-",
      member?.groupRole || "-",
      changedFields.length > 0 ? changedFields.join(", ") : "-",
      addedRoles.length > 0 ? addedRoles.join(", ") : "-",
      removedRoles.length > 0 ? removedRoles.join(", ") : "-",
      log.request?.method || "-",
      log.request?.path || "-",
      log.request?.ip || "-",
    ];
  });
}

export const listAdminAuditLogs = catchAsync(async (req, res) => {
  const filters = parseAuditLogFilters(req);
  const { page, limit, skip } = parsePagination(req);
  const last24Hours = new Date(Date.now() - 24 * 60 * 60 * 1000);

  const [result] = await AuditLogModel.aggregate([
    ...buildAuditLogPipeline(filters),
    {
      $facet: {
        records: buildAuditLogRecordPipeline({ skip, limit, paginate: true }),
        totals: [{ $count: "total" }],
        actionCounts: [
          {
            $group: {
              _id: "$action",
              count: { $sum: 1 },
            },
          },
        ],
        recent24Hours: [
          {
            $match: {
              createdAt: { $gte: last24Hours },
            },
          },
          { $count: "count" },
        ],
        affectedGroups: [
          {
            $match: {
              groupId: { $ne: null },
            },
          },
          {
            $group: {
              _id: "$groupId",
            },
          },
          { $count: "count" },
        ],
      },
    },
  ]);

  const logs = result?.records ?? [];
  const total = Number(result?.totals?.[0]?.total ?? 0);
  const actionCounts = Array.isArray(result?.actionCounts)
    ? result.actionCounts.map((entry) => ({
        action: String(entry._id || ""),
        count: Number(entry.count || 0),
      }))
    : [];
  const actionCountMap = new Map(
    actionCounts.map((entry) => [entry.action, entry.count]),
  );

  return sendSuccess(res, {
    statusCode: 200,
    results: logs.length,
    total,
    page,
    limit,
    data: {
      logs,
      summary: {
        totalLogs: total,
        last24Hours: Number(result?.recent24Hours?.[0]?.count ?? 0),
        memberCrudCount:
          Number(actionCountMap.get(AuditActions.ADMIN_MEMBER_CREATE) ?? 0) +
          Number(actionCountMap.get(AuditActions.ADMIN_MEMBER_UPDATE) ?? 0) +
          Number(actionCountMap.get(AuditActions.ADMIN_MEMBER_DELETE) ?? 0),
        adminPromotionCount: Number(
          actionCountMap.get(AuditActions.ADMIN_USER_PROMOTE_ADMIN) ?? 0,
        ),
        affectedGroups: Number(result?.affectedGroups?.[0]?.count ?? 0),
        actionCounts,
      },
      filterOptions: {
        actions: AUDIT_ACTION_OPTIONS,
        entityTypes: AUDIT_ENTITY_OPTIONS,
      },
    },
  });
});

export const exportAdminAuditLogs = catchAsync(async (req, res) => {
  const filters = parseAuditLogFilters(req);
  const logs = await AuditLogModel.aggregate([
    ...buildAuditLogPipeline(filters),
    ...buildAuditLogRecordPipeline({ paginate: false }),
  ]);

  if (logs.length === 0) {
    throw new AppError("No audit events matched the current export filters.", 400);
  }

  const csv = CSV_BOM + buildCsv([
    [
      "Logged At",
      "Action",
      "Entity Type",
      "Entity Id",
      "Summary",
      "Actor",
      "Actor Email",
      "Actor Phone",
      "Actor Roles",
      "Target",
      "Target Email",
      "Target Phone",
      "Group",
      "Group Number",
      "Member Serial",
      "Group Role",
      "Changed Fields",
      "Added Roles",
      "Removed Roles",
      "Request Method",
      "Request Path",
      "IP Address",
    ],
    ...buildAuditLogExportRows(logs),
  ]);

  const filename = `audit-trail-${new Date().toISOString().slice(0, 10)}.csv`;
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
  return res.status(200).send(csv);
});
