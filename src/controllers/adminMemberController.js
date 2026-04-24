import mongoose from "mongoose";
import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel, GroupMembershipStatuses, GroupRoles } from "../models/GroupMembership.js";
import { MembershipStatuses, ProfileModel } from "../models/Profile.js";
import { UserModel } from "../models/User.js";
import { ContributionModel } from "../models/Contribution.js";
import { ContributionSettingModel } from "../models/ContributionSetting.js";
import { NotificationModel } from "../models/Notification.js";
import { NotificationPreferenceModel } from "../models/NotificationPreference.js";
import { RecurringPaymentModel } from "../models/RecurringPayment.js";
import { TransactionModel } from "../models/Transaction.js";
import { WithdrawalRequestModel } from "../models/WithdrawalRequest.js";
import { BankAccountModel } from "../models/BankAccount.js";
import { MeetingAttendanceModel } from "../models/MeetingAttendance.js";
import { MeetingModel } from "../models/Meeting.js";
import { MeetingRsvpModel } from "../models/MeetingRsvp.js";
import { LoanApplicationModel } from "../models/LoanApplication.js";
import { LoanApplicationEditRequestModel } from "../models/LoanApplicationEditRequest.js";
import { LoanGuarantorModel } from "../models/LoanGuarantor.js";
import { LoanRepaymentScheduleItemModel } from "../models/LoanRepaymentScheduleItem.js";
import { GuarantorNotificationModel } from "../models/GuarantorNotification.js";
import { GroupVoteModel } from "../models/GroupVote.js";
import { GroupVoteResponseModel } from "../models/GroupVoteResponse.js";
import { RefreshTokenModel } from "../models/RefreshToken.js";
import { LoginHistoryModel } from "../models/LoginHistory.js";
import {
  assignGroupMemberSerial,
  formatGroupMemberSerial,
} from "../utils/groupMemberSerial.js";
import { hasNonZeroGroupMembership, isGeneralGroup } from "../utils/groupMembershipPolicy.js";
import { normalizeNigerianPhone } from "../utils/phone.js";
import {
  hasUserRole,
  normalizeUserRoles,
  pickPrimaryRole,
} from "../utils/roles.js";
import { generateAdminMembersWorkbookBuffer } from "../services/adminMemberWorkbook.js";
import { generateAdminMembersDirectoryPdfBuffer } from "../services/pdf/adminMembersDirectoryPdf.js";
import {
  AuditActions,
  AuditEntityTypes,
  createAuditLog,
} from "../services/auditLog.js";

const COUNTED_CONTRIBUTION_STATUSES = ["completed", "verified"];
const CSV_BOM = "\uFEFF";
const DEFAULT_LIMIT = 10;
const MAX_LIMIT = 100;
const MEMBER_SORT_OPTIONS = new Set([
  "newest",
  "oldest",
  "name-asc",
  "name-desc",
  "serial-asc",
  "serial-desc",
  "joined-desc",
  "joined-asc",
]);
const MEMBER_EXPORT_FORMATS = new Set(["pdf", "csv", "xlsx"]);
const DEFAULT_MEMBER_SUMMARY = {
  totalRecords: 0,
  uniqueMembers: 0,
  groupsCovered: 0,
  activeMembers: 0,
  pendingMembers: 0,
  suspendedMembers: 0,
  inactiveMembers: 0,
  rejectedMembers: 0,
  newThisMonth: 0,
};

function hasOwn(object, key) {
  return Object.prototype.hasOwnProperty.call(object || {}, key);
}

function sanitizeString(value, { lowercase = false } = {}) {
  if (value === undefined) return undefined;
  if (value === null) return null;
  const normalized = String(value).trim();
  if (!normalized) return "";
  return lowercase ? normalized.toLowerCase() : normalized;
}

function sanitizeOptionalString(value, options = {}) {
  const normalized = sanitizeString(value, options);
  if (normalized === undefined) return undefined;
  if (normalized === null || normalized === "") return null;
  return normalized;
}

function parsePagination(req) {
  const page = Math.max(1, parseInt(String(req.query?.page ?? "1"), 10) || 1);
  const limit = Math.min(
    MAX_LIMIT,
    Math.max(1, parseInt(String(req.query?.limit ?? String(DEFAULT_LIMIT)), 10) || DEFAULT_LIMIT),
  );
  return {
    page,
    limit,
    skip: (page - 1) * limit,
  };
}

function resolveMemberSort(sortKey) {
  switch (sortKey) {
    case "oldest":
      return { createdAt: 1, _id: 1 };
    case "name-asc":
      return { "profile.fullName": 1, memberNumber: 1, createdAt: -1 };
    case "name-desc":
      return { "profile.fullName": -1, memberNumber: 1, createdAt: -1 };
    case "serial-asc":
      return { memberNumber: 1, memberSerial: 1, createdAt: -1 };
    case "serial-desc":
      return { memberNumber: -1, memberSerial: -1, createdAt: -1 };
    case "joined-asc":
      return { joinedAt: 1, createdAt: 1 };
    case "joined-desc":
      return { joinedAt: -1, createdAt: -1 };
    case "newest":
    default:
      return { createdAt: -1, _id: -1 };
  }
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
  }).format(date);
}

function formatRoleLabel(role) {
  switch (String(role || "").trim().toLowerCase()) {
    case "coordinator":
      return "Coordinator";
    case "treasurer":
      return "Treasurer";
    case "secretary":
      return "Secretary";
    case "admin":
      return "Group Admin";
    case "member":
    default:
      return "Member";
  }
}

function formatStatusLabel(status) {
  const value = String(status || "").trim().toLowerCase();
  if (!value) return "Unknown";
  return value.replace(/[-_]+/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

function formatSortLabel(sortKey) {
  switch (sortKey) {
    case "oldest":
      return "Oldest first";
    case "name-asc":
      return "Name A-Z";
    case "name-desc":
      return "Name Z-A";
    case "serial-asc":
      return "Serial ascending";
    case "serial-desc":
      return "Serial descending";
    case "joined-desc":
      return "Recently joined";
    case "joined-asc":
      return "Earliest joined";
    case "newest":
    default:
      return "Newest first";
  }
}

function parseAdminMemberFilters(req) {
  const search = String(req.query?.search ?? "").trim();
  const status = String(req.query?.status ?? "all").trim().toLowerCase();
  const profileStatus = String(req.query?.profileStatus ?? "all").trim().toLowerCase();
  const role = String(req.query?.role ?? "all").trim().toLowerCase();
  const sort = String(req.query?.sort ?? "newest").trim().toLowerCase();
  const groupId = String(req.query?.groupId ?? "").trim();

  if (status !== "all" && !GroupMembershipStatuses.includes(status)) {
    throw new AppError("Invalid member status filter", 400);
  }
  if (profileStatus !== "all" && !MembershipStatuses.includes(profileStatus)) {
    throw new AppError("Invalid profile status filter", 400);
  }
  if (role !== "all" && !GroupRoles.includes(role)) {
    throw new AppError("Invalid member role filter", 400);
  }
  if (!MEMBER_SORT_OPTIONS.has(sort)) {
    throw new AppError("Invalid member sort option", 400);
  }
  if (groupId && !mongoose.Types.ObjectId.isValid(groupId)) {
    throw new AppError("Invalid group filter", 400);
  }

  return {
    search,
    status,
    profileStatus,
    role,
    sort,
    groupId,
  };
}

function toObjectIdList(values = []) {
  return values.map((value) => new mongoose.Types.ObjectId(String(value)));
}

async function getManageableGroupIds(req) {
  if (!req.user) {
    throw new AppError("Not authenticated", 401);
  }
  if (!req.user.profileId) {
    throw new AppError("User profile not found", 400);
  }
  if (hasUserRole(req.user, "admin")) {
    return null;
  }
  if (!hasUserRole(req.user, "groupCoordinator")) {
    throw new AppError(
      "You do not have permission to manage member records.",
      403,
    );
  }

  const memberships = await GroupMembershipModel.find(
    {
      userId: req.user.profileId,
      role: "coordinator",
      status: "active",
    },
    { groupId: 1 },
  ).lean();

  return [
    ...new Set(
      memberships
        .map((membership) => String(membership.groupId || ""))
        .filter(Boolean),
    ),
  ];
}

function ensureManageableGroupAccess(
  groupId,
  manageableGroupIds,
  {
    message = "You can only manage members in groups assigned to you.",
    statusCode = 403,
  } = {},
) {
  if (!manageableGroupIds) return;
  const targetGroupId = String(groupId || "");
  if (!targetGroupId || !manageableGroupIds.includes(targetGroupId)) {
    throw new AppError(message, statusCode);
  }
}

function normalizeMemberSerialInput(value, groupNumber) {
  const normalized = sanitizeOptionalString(value);
  if (!normalized) return null;

  const match = /^CRC\/G(\d+)\/(\d+)$/i.exec(String(normalized).trim());
  if (!match) {
    throw new AppError(
      "Member serial must use the format CRC/G{groupNumber}/{memberNumber}.",
      400,
    );
  }

  const serialGroupNumber = Number(match[1]);
  const memberNumber = Number(match[2]);
  if (
    !Number.isFinite(serialGroupNumber) ||
    serialGroupNumber <= 0 ||
    !Number.isFinite(memberNumber) ||
    memberNumber <= 0
  ) {
    throw new AppError("Member serial contains an invalid member number.", 400);
  }

  const expectedGroupNumber = Number(groupNumber ?? 0);
  if (expectedGroupNumber > 0 && serialGroupNumber !== expectedGroupNumber) {
    throw new AppError("Member serial must match the selected group number.", 400);
  }

  return {
    memberNumber,
    memberSerial: formatGroupMemberSerial({
      groupNumber: serialGroupNumber,
      memberNumber,
    }),
  };
}

function buildAdminMemberBasePipeline(filters, manageableGroupIds = null) {
  const match = {};
  if (manageableGroupIds) {
    if (filters.groupId) {
      ensureManageableGroupAccess(filters.groupId, manageableGroupIds);
      match.groupId = new mongoose.Types.ObjectId(filters.groupId);
    } else {
      match.groupId = { $in: toObjectIdList(manageableGroupIds) };
    }
  } else if (filters.groupId) {
    match.groupId = new mongoose.Types.ObjectId(filters.groupId);
  }
  if (filters.status !== "all") {
    match.status = filters.status;
  }
  if (filters.role !== "all") {
    match.role = filters.role;
  }

  const profileCollection = ProfileModel.collection.name;
  const groupCollection = GroupModel.collection.name;
  const userCollection = UserModel.collection.name;

  const pipeline = [
    { $match: match },
    {
      $lookup: {
        from: profileCollection,
        localField: "userId",
        foreignField: "_id",
        as: "profile",
      },
    },
    { $unwind: { path: "$profile", preserveNullAndEmptyArrays: false } },
    {
      $lookup: {
        from: groupCollection,
        localField: "groupId",
        foreignField: "_id",
        as: "group",
      },
    },
    { $unwind: { path: "$group", preserveNullAndEmptyArrays: false } },
    {
      $lookup: {
        from: userCollection,
        let: { profileId: "$userId" },
        pipeline: [
          { $match: { $expr: { $eq: ["$profileId", "$$profileId"] } } },
          {
            $project: {
              email: 1,
              phone: 1,
              roles: 1,
              role: 1,
              active: 1,
              emailVerifiedAt: 1,
              phoneVerifiedAt: 1,
            },
          },
        ],
        as: "user",
      },
    },
    { $unwind: { path: "$user", preserveNullAndEmptyArrays: true } },
  ];

  if (filters.profileStatus !== "all") {
    pipeline.push({
      $match: {
        "profile.membershipStatus": filters.profileStatus,
      },
    });
  }

  if (filters.search) {
    pipeline.push({
      $match: {
        $or: [
          { memberSerial: { $regex: filters.search, $options: "i" } },
          { "profile.fullName": { $regex: filters.search, $options: "i" } },
          { "profile.email": { $regex: filters.search, $options: "i" } },
          { "profile.phone": { $regex: filters.search, $options: "i" } },
          { "user.email": { $regex: filters.search, $options: "i" } },
          { "user.phone": { $regex: filters.search, $options: "i" } },
          { "group.groupName": { $regex: filters.search, $options: "i" } },
        ],
      },
    });
  }

  return pipeline;
}

function buildAdminMemberProjection() {
  return {
    $project: {
      _id: 0,
      membershipId: { $toString: "$_id" },
      profileId: { $toString: "$userId" },
      userId: {
        $cond: [
          { $ifNull: ["$user._id", false] },
          { $toString: "$user._id" },
          null,
        ],
      },
      memberSerial: "$memberSerial",
      memberNumber: "$memberNumber",
      fullName: { $ifNull: ["$profile.fullName", "Member"] },
      email: {
        $ifNull: [
          "$user.email",
          { $ifNull: ["$profile.email", ""] },
        ],
      },
      phone: {
        $ifNull: [
          "$user.phone",
          { $ifNull: ["$profile.phone", ""] },
        ],
      },
      groupId: { $toString: "$group._id" },
      groupName: { $ifNull: ["$group.groupName", "Group"] },
      groupNumber: "$group.groupNumber",
      role: "$role",
      status: "$status",
      joinedAt: "$joinedAt",
      requestedAt: "$requestedAt",
      reviewedAt: "$reviewedAt",
      createdAt: "$createdAt",
      updatedAt: "$updatedAt",
      profileMembershipStatus: "$profile.membershipStatus",
      userRoles: {
        $ifNull: [
          "$user.roles",
          {
            $cond: [
              { $ifNull: ["$user.role", false] },
              ["$user.role"],
              [],
            ],
          },
        ],
      },
      accountActive: {
        $cond: [{ $eq: ["$user.active", false] }, false, true],
      },
      emailVerifiedAt: "$user.emailVerifiedAt",
      phoneVerifiedAt: "$user.phoneVerifiedAt",
    },
  };
}

function buildAdminMemberSummaryPipeline(monthStart) {
  return [
    {
      $group: {
        _id: null,
        totalRecords: { $sum: 1 },
        profileIds: { $addToSet: "$userId" },
        groupIds: { $addToSet: "$groupId" },
        activeMembers: {
          $sum: { $cond: [{ $eq: ["$status", "active"] }, 1, 0] },
        },
        pendingMembers: {
          $sum: { $cond: [{ $eq: ["$status", "pending"] }, 1, 0] },
        },
        suspendedMembers: {
          $sum: { $cond: [{ $eq: ["$status", "suspended"] }, 1, 0] },
        },
        inactiveMembers: {
          $sum: { $cond: [{ $eq: ["$status", "inactive"] }, 1, 0] },
        },
        rejectedMembers: {
          $sum: { $cond: [{ $eq: ["$status", "rejected"] }, 1, 0] },
        },
        newThisMonth: {
          $sum: {
            $cond: [{ $gte: ["$createdAt", monthStart] }, 1, 0],
          },
        },
      },
    },
    {
      $project: {
        _id: 0,
        totalRecords: 1,
        uniqueMembers: { $size: "$profileIds" },
        groupsCovered: { $size: "$groupIds" },
        activeMembers: 1,
        pendingMembers: 1,
        suspendedMembers: 1,
        inactiveMembers: 1,
        rejectedMembers: 1,
        newThisMonth: 1,
      },
    },
  ];
}

async function getAdminMemberListing(
  filters,
  manageableGroupIds = null,
  { page = 1, limit = DEFAULT_LIMIT, paginate = true } = {},
) {
  const safePage = Math.max(1, Number(page) || 1);
  const safeLimit = Math.min(MAX_LIMIT, Math.max(1, Number(limit) || DEFAULT_LIMIT));
  const skip = (safePage - 1) * safeLimit;

  const monthStart = new Date();
  monthStart.setDate(1);
  monthStart.setHours(0, 0, 0, 0);

  const recordPipeline = [{ $sort: resolveMemberSort(filters.sort) }];
  if (paginate) {
    recordPipeline.push({ $skip: skip }, { $limit: safeLimit });
  }
  recordPipeline.push(buildAdminMemberProjection());

  const [result] = await GroupMembershipModel.aggregate([
    ...buildAdminMemberBasePipeline(filters, manageableGroupIds),
    {
      $facet: {
        records: recordPipeline,
        totals: [{ $count: "total" }],
        summary: buildAdminMemberSummaryPipeline(monthStart),
      },
    },
  ]);

  return {
    members: result?.records ?? [],
    total: Number(result?.totals?.[0]?.total ?? 0),
    summary: result?.summary?.[0] ?? DEFAULT_MEMBER_SUMMARY,
  };
}

function buildAdminMemberExportRows(members) {
  return members.map((member) => ({
    memberSerial: member.memberSerial || "Pending assignment",
    fullName: member.fullName || "Member",
    email: member.email || "-",
    phone: member.phone || "-",
    groupName: member.groupName || "-",
    role: formatRoleLabel(member.role),
    memberStatus: formatStatusLabel(member.status),
    profileStatus: formatStatusLabel(member.profileMembershipStatus),
    joinedAt: formatDateValue(member.joinedAt || member.createdAt),
  }));
}

function buildAdminMemberExportScope(
  filters,
  members,
  manageableGroupIds = null,
) {
  const groupLabel = filters.groupId
    ? members[0]?.groupName || "Selected group"
    : manageableGroupIds
      ? "Managed groups"
      : "All groups";

  return [
    `Search: ${filters.search || "All members"}`,
    `Group: ${groupLabel}`,
    `Membership status: ${
      filters.status === "all" ? "All statuses" : formatStatusLabel(filters.status)
    }`,
    `Profile status: ${
      filters.profileStatus === "all"
        ? "All profile statuses"
        : formatStatusLabel(filters.profileStatus)
    }`,
    `Role: ${filters.role === "all" ? "All roles" : formatRoleLabel(filters.role)}`,
    `Sort: ${formatSortLabel(filters.sort)}`,
  ];
}

function buildMemberRow({ membership, profile, user, group }) {
  const userRoles = normalizeUserRoles(user || {});
  return {
    membershipId: String(membership._id),
    profileId: String(profile?._id || membership.userId),
    userId: user?._id ? String(user._id) : null,
    memberSerial: membership.memberSerial ?? null,
    memberNumber:
      typeof membership.memberNumber === "number"
        ? membership.memberNumber
        : null,
    fullName: profile?.fullName ?? "Member",
    email: user?.email ?? profile?.email ?? "",
    phone: user?.phone ?? profile?.phone ?? "",
    address: profile?.address ?? null,
    city: profile?.city ?? null,
    state: profile?.state ?? null,
    occupation: profile?.occupation ?? null,
    employer: profile?.employer ?? null,
    nextOfKinName: profile?.nextOfKinName ?? null,
    nextOfKinPhone: profile?.nextOfKinPhone ?? null,
    nextOfKinRelationship: profile?.nextOfKinRelationship ?? null,
    groupId: String(group?._id || membership.groupId),
    groupName: group?.groupName ?? "Group",
    groupNumber:
      typeof group?.groupNumber === "number" ? group.groupNumber : null,
    role: membership.role,
    status: membership.status,
    joinedAt: membership.joinedAt ? membership.joinedAt.toISOString() : null,
    requestedAt: membership.requestedAt
      ? membership.requestedAt.toISOString()
      : null,
    reviewedAt: membership.reviewedAt ? membership.reviewedAt.toISOString() : null,
    reviewNotes: membership.reviewNotes ?? null,
    createdAt: membership.createdAt ? membership.createdAt.toISOString() : null,
    updatedAt: membership.updatedAt ? membership.updatedAt.toISOString() : null,
    profileMembershipStatus: profile?.membershipStatus ?? null,
    userRoles,
    accountActive: Boolean(user?.active ?? true),
    emailVerifiedAt: user?.emailVerifiedAt
      ? new Date(user.emailVerifiedAt).toISOString()
      : null,
    phoneVerifiedAt: user?.phoneVerifiedAt
      ? new Date(user.phoneVerifiedAt).toISOString()
      : null,
  };
}

function buildMemberAuditState({ membership, profile, user, group }) {
  return {
    membershipId: membership?._id ? String(membership._id) : null,
    userId: user?._id ? String(user._id) : null,
    profileId: profile?._id ? String(profile._id) : null,
    groupId: group?._id ? String(group._id) : null,
    groupName: group?.groupName ?? null,
    groupNumber:
      typeof group?.groupNumber === "number" ? Number(group.groupNumber) : null,
    fullName: profile?.fullName ?? null,
    email: user?.email ?? profile?.email ?? null,
    phone: user?.phone ?? profile?.phone ?? null,
    groupRole: membership?.role ?? null,
    membershipStatus: membership?.status ?? null,
    profileMembershipStatus: profile?.membershipStatus ?? null,
    memberSerial: membership?.memberSerial ?? null,
    memberNumber:
      typeof membership?.memberNumber === "number"
        ? Number(membership.memberNumber)
        : null,
    userRoles: [...normalizeUserRoles(user || {})].sort(),
    accountActive: Boolean(user?.active ?? true),
    addressPresent: Boolean(profile?.address),
    cityPresent: Boolean(profile?.city),
    statePresent: Boolean(profile?.state),
    occupationPresent: Boolean(profile?.occupation),
    employerPresent: Boolean(profile?.employer),
    nextOfKinPresent: Boolean(
      profile?.nextOfKinName ||
        profile?.nextOfKinPhone ||
        profile?.nextOfKinRelationship,
    ),
    reviewNotesPresent: Boolean(membership?.reviewNotes),
  };
}

function buildMemberAuditStateFromDetail(member) {
  return {
    membershipId: member?.membershipId ? String(member.membershipId) : null,
    userId: member?.userId ? String(member.userId) : null,
    profileId: member?.profileId ? String(member.profileId) : null,
    groupId: member?.groupId ? String(member.groupId) : null,
    groupName: member?.groupName ?? null,
    groupNumber:
      typeof member?.groupNumber === "number" ? Number(member.groupNumber) : null,
    fullName: member?.fullName ?? null,
    email: member?.email ?? null,
    phone: member?.phone ?? null,
    groupRole: member?.role ?? null,
    membershipStatus: member?.status ?? null,
    profileMembershipStatus: member?.profileMembershipStatus ?? null,
    memberSerial: member?.memberSerial ?? null,
    memberNumber:
      typeof member?.memberNumber === "number" ? Number(member.memberNumber) : null,
    userRoles: Array.isArray(member?.userRoles)
      ? [...member.userRoles].map(String).sort()
      : [],
    accountActive: Boolean(member?.accountActive ?? true),
    addressPresent: Boolean(member?.address),
    cityPresent: Boolean(member?.city),
    statePresent: Boolean(member?.state),
    occupationPresent: Boolean(member?.occupation),
    employerPresent: Boolean(member?.employer),
    nextOfKinPresent: Boolean(
      member?.nextOfKinName ||
        member?.nextOfKinPhone ||
        member?.nextOfKinRelationship,
    ),
    reviewNotesPresent: Boolean(member?.reviewNotes),
  };
}

function areAuditValuesEqual(left, right) {
  if (Array.isArray(left) || Array.isArray(right)) {
    return JSON.stringify(left ?? []) === JSON.stringify(right ?? []);
  }
  return left === right;
}

function buildMemberAuditChanges(beforeState, afterState) {
  const labels = {
    fullName: "fullName",
    email: "email",
    phone: "phone",
    groupRole: "groupRole",
    membershipStatus: "membershipStatus",
    profileMembershipStatus: "profileMembershipStatus",
    memberSerial: "memberSerial",
    memberNumber: "memberNumber",
    userRoles: "userRoles",
    accountActive: "accountActive",
    addressPresent: "addressPresent",
    cityPresent: "cityPresent",
    statePresent: "statePresent",
    occupationPresent: "occupationPresent",
    employerPresent: "employerPresent",
    nextOfKinPresent: "nextOfKinPresent",
    reviewNotesPresent: "reviewNotesPresent",
  };
  const detailedKeys = new Set([
    "fullName",
    "email",
    "phone",
    "groupRole",
    "membershipStatus",
    "profileMembershipStatus",
    "memberSerial",
    "memberNumber",
    "userRoles",
    "accountActive",
  ]);

  const changedFields = [];
  const changes = {};

  for (const [key, label] of Object.entries(labels)) {
    if (areAuditValuesEqual(beforeState?.[key], afterState?.[key])) continue;
    changedFields.push(label);
    if (detailedKeys.has(key)) {
      changes[label] = {
        before: beforeState?.[key] ?? null,
        after: afterState?.[key] ?? null,
      };
    }
  }

  return {
    changedFields,
    changes,
  };
}

async function syncUserCoordinatorRole(profileId, session = null) {
  if (!profileId) return;

  let userQuery = UserModel.findOne({ profileId }).select("+active roles role");
  let coordinatorQuery = GroupMembershipModel.exists({
    userId: profileId,
    role: "coordinator",
    status: "active",
  });
  if (session) {
    userQuery = userQuery.session(session);
    coordinatorQuery = coordinatorQuery.session(session);
  }

  const [user, hasCoordinatorMembership] = await Promise.all([
    userQuery.lean(),
    coordinatorQuery,
  ]);

  if (!user) return;

  const currentRoles = normalizeUserRoles(user);
  const nextRoles = new Set(currentRoles);
  nextRoles.add("member");

  if (hasCoordinatorMembership) {
    nextRoles.add("groupCoordinator");
  } else {
    nextRoles.delete("groupCoordinator");
  }

  const resolvedRoles = Array.from(nextRoles);
  await UserModel.updateOne(
    { profileId },
    {
      $set: {
        roles: resolvedRoles,
        role: pickPrimaryRole(resolvedRoles),
      },
    },
    session ? { session } : {},
  );
}

async function syncProfileMembershipStatus(profileId, session = null) {
  if (!profileId) return;

  let membershipsQuery = GroupMembershipModel.find(
    { userId: profileId },
    { status: 1 },
  );
  if (session) {
    membershipsQuery = membershipsQuery.session(session);
  }

  const memberships = await membershipsQuery.lean();
  const statuses = memberships.map((membership) => String(membership.status || ""));

  let membershipStatus = "inactive";
  if (statuses.includes("active")) {
    membershipStatus = "active";
  } else if (statuses.includes("pending")) {
    membershipStatus = "pending";
  } else if (statuses.includes("suspended")) {
    membershipStatus = "suspended";
  }

  await ProfileModel.updateOne(
    { _id: profileId },
    { $set: { membershipStatus } },
    session ? { session } : {},
  );
}

async function recomputeGroupSnapshot(groupId, session = null) {
  if (!groupId) return;

  const activeCountPromise = GroupMembershipModel.countDocuments({
    groupId,
    status: "active",
  });

  const savingsAggPromise = ContributionModel.aggregate([
    {
      $match: {
        groupId,
        status: { $in: COUNTED_CONTRIBUTION_STATUSES },
      },
    },
    {
      $group: {
        _id: null,
        totalSavings: { $sum: "$amount" },
      },
    },
  ]);

  if (session) {
    activeCountPromise.session(session);
    savingsAggPromise.session(session);
  }

  const [memberCount, savingsAgg] = await Promise.all([
    activeCountPromise,
    savingsAggPromise,
  ]);

  const totalSavings = Number(savingsAgg?.[0]?.totalSavings ?? 0);
  await GroupModel.updateOne(
    { _id: groupId },
    { $set: { memberCount, totalSavings } },
    session ? { session } : {},
  );
}

async function loadMemberContext(
  membershipId,
  session = null,
  manageableGroupIds = null,
) {
  if (!mongoose.Types.ObjectId.isValid(String(membershipId || ""))) {
    throw new AppError("Invalid member record", 400);
  }

  let membershipQuery = GroupMembershipModel.findById(membershipId);
  if (session) membershipQuery = membershipQuery.session(session);
  const membership = await membershipQuery;
  if (!membership) {
    throw new AppError("Member record not found", 404);
  }
  ensureManageableGroupAccess(membership.groupId, manageableGroupIds, {
    message: "Member record not found",
    statusCode: 404,
  });

  let profileQuery = ProfileModel.findById(membership.userId);
  let groupQuery = GroupModel.findById(membership.groupId);
  let userQuery = UserModel.findOne({ profileId: membership.userId }).select(
    "email phone roles role emailVerifiedAt phoneVerifiedAt +active",
  );
  if (session) {
    profileQuery = profileQuery.session(session);
    groupQuery = groupQuery.session(session);
    userQuery = userQuery.session(session);
  }

  const [profile, group, user] = await Promise.all([
    profileQuery,
    groupQuery,
    userQuery,
  ]);

  if (!profile) throw new AppError("Profile not found", 404);
  if (!group) throw new AppError("Group not found", 404);
  if (!user) throw new AppError("User account not found", 404);

  return { membership, profile, group, user };
}

async function buildMemberDetail(
  membershipId,
  session = null,
  manageableGroupIds = null,
) {
  const { membership, profile, group, user } = await loadMemberContext(
    membershipId,
    session,
    manageableGroupIds,
  );
  const scopedGroupIds = manageableGroupIds
    ? toObjectIdList(manageableGroupIds)
    : null;

  let membershipsQuery = GroupMembershipModel.find(
    {
      userId: profile._id,
      ...(scopedGroupIds
        ? { groupId: { $in: scopedGroupIds } }
        : {}),
    },
    {
      groupId: 1,
      role: 1,
      status: 1,
      joinedAt: 1,
      requestedAt: 1,
      reviewedAt: 1,
      memberSerial: 1,
      memberNumber: 1,
      reviewNotes: 1,
      createdAt: 1,
      updatedAt: 1,
    },
  ).sort({ createdAt: -1 });

  let contributionsAgg = ContributionModel.aggregate([
    {
      $match: {
        userId: profile._id,
        ...(scopedGroupIds ? { groupId: { $in: scopedGroupIds } } : {}),
        status: { $in: COUNTED_CONTRIBUTION_STATUSES },
      },
    },
    {
      $group: {
        _id: null,
        total: { $sum: "$amount" },
        count: { $sum: 1 },
      },
    },
  ]);

  let withdrawalsAgg = WithdrawalRequestModel.aggregate([
    {
      $match: {
        userId: profile._id,
        ...(scopedGroupIds ? { groupId: { $in: scopedGroupIds } } : {}),
      },
    },
    {
      $group: {
        _id: null,
        total: { $sum: "$amount" },
        count: { $sum: 1 },
      },
    },
  ]);

  let loansAgg = LoanApplicationModel.aggregate([
    {
      $match: {
        $or: [{ userId: profile._id }, { profileId: profile._id }],
        ...(scopedGroupIds ? { groupId: { $in: scopedGroupIds } } : {}),
      },
    },
    {
      $group: {
        _id: null,
        count: { $sum: 1 },
        totalBorrowed: { $sum: "$loanAmount" },
      },
    },
  ]);

  let attendanceMetricQuery = scopedGroupIds
    ? MeetingAttendanceModel.aggregate([
        {
          $match: {
            userId: profile._id,
            status: "present",
          },
        },
        {
          $lookup: {
            from: MeetingModel.collection.name,
            localField: "meetingId",
            foreignField: "_id",
            as: "meeting",
          },
        },
        { $unwind: { path: "$meeting", preserveNullAndEmptyArrays: false } },
        {
          $match: {
            "meeting.groupId": { $in: scopedGroupIds },
          },
        },
        { $count: "total" },
      ])
    : MeetingAttendanceModel.countDocuments({
        userId: profile._id,
        status: "present",
      });

  if (session) {
    membershipsQuery = membershipsQuery.session(session);
    contributionsAgg = contributionsAgg.session(session);
    withdrawalsAgg = withdrawalsAgg.session(session);
    loansAgg = loansAgg.session(session);
    attendanceMetricQuery = attendanceMetricQuery.session(session);
  }

  const [
    allMemberships,
    contributionRows,
    withdrawalRows,
    loanRows,
    attendanceMetricRows,
  ] = await Promise.all([
      membershipsQuery.lean(),
      contributionsAgg,
      withdrawalsAgg,
      loansAgg,
      attendanceMetricQuery,
    ]);

  const membershipGroupIds = [
    ...new Set(allMemberships.map((entry) => String(entry.groupId))),
  ];
  let groupsQuery = GroupModel.find(
    { _id: { $in: membershipGroupIds } },
    { groupName: 1, groupNumber: 1 },
  );
  if (session) groupsQuery = groupsQuery.session(session);
  const groups = await groupsQuery.lean();
  const groupById = new Map(groups.map((entry) => [String(entry._id), entry]));

  const memberships = allMemberships.map((entry) => {
    const membershipGroup = groupById.get(String(entry.groupId));
    return {
      membershipId: String(entry._id),
      groupId: String(entry.groupId),
      groupName: membershipGroup?.groupName ?? "Group",
      groupNumber:
        typeof membershipGroup?.groupNumber === "number"
          ? membershipGroup.groupNumber
          : null,
      role: entry.role,
      status: entry.status,
      memberSerial: entry.memberSerial ?? null,
      memberNumber:
        typeof entry.memberNumber === "number" ? entry.memberNumber : null,
      joinedAt: entry.joinedAt ? entry.joinedAt.toISOString() : null,
      requestedAt: entry.requestedAt ? entry.requestedAt.toISOString() : null,
      reviewedAt: entry.reviewedAt ? entry.reviewedAt.toISOString() : null,
      reviewNotes: entry.reviewNotes ?? null,
      createdAt: entry.createdAt ? entry.createdAt.toISOString() : null,
      updatedAt: entry.updatedAt ? entry.updatedAt.toISOString() : null,
    };
  });

  const contributionSummary = contributionRows?.[0] ?? {};
  const withdrawalSummary = withdrawalRows?.[0] ?? {};
  const loanSummary = loanRows?.[0] ?? {};
  const meetingsAttended = Array.isArray(attendanceMetricRows)
    ? Number(attendanceMetricRows?.[0]?.total ?? 0)
    : Number(attendanceMetricRows ?? 0);

  const baseRow = buildMemberRow({
    membership,
    profile,
    user,
    group,
  });

  return {
    ...baseRow,
    memberships,
    stats: {
      totalContributions: Number(contributionSummary.total ?? 0),
      contributionCount: Number(contributionSummary.count ?? 0),
      totalWithdrawals: Number(withdrawalSummary.total ?? 0),
      withdrawalCount: Number(withdrawalSummary.count ?? 0),
      loanApplications: Number(loanSummary.count ?? 0),
      totalBorrowed: Number(loanSummary.totalBorrowed ?? 0),
      meetingsAttended: Number(meetingsAttended ?? 0),
      activeMemberships: memberships.filter((entry) => entry.status === "active")
        .length,
    },
  };
}

export const listAdminMembers = catchAsync(async (req, res, next) => {
  const filters = parseAdminMemberFilters(req);
  const manageableGroupIds = await getManageableGroupIds(req);
  const { page, limit } = parsePagination(req);
  const { members, total, summary } = await getAdminMemberListing(
    filters,
    manageableGroupIds,
    {
      page,
      limit,
      paginate: true,
    },
  );

  return sendSuccess(res, {
    statusCode: 200,
    results: members.length,
    total,
    page,
    limit,
    data: {
      members,
      summary,
    },
  });
});

export const exportAdminMembers = catchAsync(async (req, res, next) => {
  const filters = parseAdminMemberFilters(req);
  const manageableGroupIds = await getManageableGroupIds(req);
  const format = String(req.query?.format ?? "pdf").trim().toLowerCase();

  if (!MEMBER_EXPORT_FORMATS.has(format)) {
    return next(new AppError("Invalid format. Use pdf, csv, or xlsx.", 400));
  }

  const now = new Date();
  const { members, summary } = await getAdminMemberListing(
    filters,
    manageableGroupIds,
    {
      paginate: false,
    },
  );

  if (members.length === 0) {
    return next(new AppError("No members matched the current export filters.", 400));
  }

  const filenameBase = `members-management-${now.toISOString().slice(0, 10)}`;
  const exportRows = buildAdminMemberExportRows(members);

  if (format === "csv") {
    const csv = CSV_BOM + buildCsv([
      [
        "Member Serial",
        "Full Name",
        "Email",
        "Phone",
        "Group",
        "Role",
        "Member Status",
        "Profile Status",
        "Joined",
      ],
      ...exportRows.map((row) => [
        row.memberSerial,
        row.fullName,
        row.email,
        row.phone,
        row.groupName,
        row.role,
        row.memberStatus,
        row.profileStatus,
        row.joinedAt,
      ]),
    ]);

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${filenameBase}.csv"`,
    );
    return res.status(200).send(csv);
  }

  if (format === "xlsx") {
    const workbookBuffer = await generateAdminMembersWorkbookBuffer({
      rows: exportRows,
    });
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    );
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${filenameBase}.xlsx"`,
    );
    return res.status(200).send(workbookBuffer);
  }

  const pdfBuffer = await generateAdminMembersDirectoryPdfBuffer({
    summary,
    rows: exportRows,
    generatedAt: now,
    scopeLines: buildAdminMemberExportScope(
      filters,
      members,
      manageableGroupIds,
    ),
  });

  res.setHeader("Content-Type", "application/pdf");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="${filenameBase}.pdf"`,
  );
  return res.status(200).send(pdfBuffer);
});

export const getAdminMemberDetails = catchAsync(async (req, res) => {
  const manageableGroupIds = await getManageableGroupIds(req);
  const member = await buildMemberDetail(
    req.params.membershipId,
    null,
    manageableGroupIds,
  );
  return sendSuccess(res, {
    statusCode: 200,
    data: { member },
  });
});

export const createAdminMember = catchAsync(async (req, res, next) => {
  const manageableGroupIds = await getManageableGroupIds(req);
  const fullName = sanitizeOptionalString(req.body?.fullName);
  const email = sanitizeOptionalString(req.body?.email, { lowercase: true });
  const phoneRaw = sanitizeOptionalString(req.body?.phone);
  const phone = phoneRaw ? normalizeNigerianPhone(phoneRaw) : null;
  const password = String(req.body?.password ?? "");
  const groupId = String(req.body?.groupId ?? "").trim();
  const role = String(req.body?.role ?? "member").trim().toLowerCase();
  const reviewNotes = sanitizeOptionalString(req.body?.reviewNotes);

  if (!fullName) return next(new AppError("Full name is required", 400));
  if (!email && !phoneRaw) {
    return next(new AppError("Either email or phone is required", 400));
  }
  if (phoneRaw && !phone) {
    return next(new AppError("Phone number must be a valid Nigerian number", 400));
  }
  if (!password || password.length < 8) {
    return next(new AppError("Password must be at least 8 characters long", 400));
  }
  if (!mongoose.Types.ObjectId.isValid(groupId)) {
    return next(new AppError("A valid group is required", 400));
  }
  if (!GroupRoles.includes(role)) {
    return next(new AppError("Invalid group role", 400));
  }
  ensureManageableGroupAccess(groupId, manageableGroupIds);

  const address = sanitizeOptionalString(req.body?.address);
  const city = sanitizeOptionalString(req.body?.city);
  const state = sanitizeOptionalString(req.body?.state);
  const occupation = sanitizeOptionalString(req.body?.occupation);
  const employer = sanitizeOptionalString(req.body?.employer);
  const nextOfKinName = sanitizeOptionalString(req.body?.nextOfKinName);
  const nextOfKinPhoneRaw = sanitizeOptionalString(req.body?.nextOfKinPhone);
  const nextOfKinPhone = nextOfKinPhoneRaw
    ? normalizeNigerianPhone(nextOfKinPhoneRaw)
    : null;
  const nextOfKinRelationship = sanitizeOptionalString(
    req.body?.nextOfKinRelationship,
  );

  if (nextOfKinPhoneRaw && !nextOfKinPhone) {
    return next(
      new AppError("Next of kin phone must be a valid Nigerian number", 400),
    );
  }

  const session = await mongoose.startSession();
  let member = null;

  try {
    await session.withTransaction(async () => {
      const group = await GroupModel.findById(groupId).session(session);
      if (!group) {
        throw new AppError("Group not found", 404);
      }

      const [existingEmail, existingPhone, activeMemberCount] = await Promise.all([
        email
          ? UserModel.findOne({ email }, { _id: 1 }).session(session).lean()
          : null,
        phone
          ? UserModel.findOne({ phone }, { _id: 1 }).session(session).lean()
          : null,
        GroupMembershipModel.countDocuments({
          groupId: group._id,
          status: "active",
        }).session(session),
      ]);

      if (existingEmail) {
        throw new AppError("An account with this email already exists", 409);
      }
      if (existingPhone) {
        throw new AppError("An account with this phone number already exists", 409);
      }
      if (activeMemberCount >= Number(group.maxMembers ?? 0)) {
        throw new AppError("This group has reached its member limit", 400);
      }

      const profile = await ProfileModel.create(
        [
          {
            email,
            phone,
            fullName,
            address,
            city,
            state,
            occupation,
            employer,
            nextOfKinName,
            nextOfKinPhone,
            nextOfKinRelationship,
            membershipStatus: "active",
            avatar: null,
          },
        ],
        { session },
      ).then((rows) => rows[0]);

      const userRoles = new Set(["member"]);
      if (role === "coordinator") {
        userRoles.add("groupCoordinator");
      }
      const resolvedRoles = Array.from(userRoles);

      const user = await UserModel.create(
        [
          {
            email,
            phone,
            password,
            profileId: profile._id,
            roles: resolvedRoles,
            role: pickPrimaryRole(resolvedRoles),
            active: true,
          },
        ],
        { session },
      ).then((rows) => rows[0]);

      const membership = await GroupMembershipModel.create(
        [
          {
            userId: profile._id,
            groupId: group._id,
            role,
            status: "active",
            joinedAt: new Date(),
            requestedAt: new Date(),
            reviewedAt: new Date(),
            reviewedBy: req.user?.profileId ?? null,
            reviewNotes,
          },
        ],
        { session },
      ).then((rows) => rows[0]);

      await assignGroupMemberSerial({ membership, group, session });
      await recomputeGroupSnapshot(group._id, session);
      member = await buildMemberDetail(membership._id, session, manageableGroupIds);

      await createAuditLog(
        {
          req,
          action: AuditActions.ADMIN_MEMBER_CREATE,
          entityType: AuditEntityTypes.GROUP_MEMBERSHIP,
          entityId: membership._id,
          membershipId: membership._id,
          targetUserId: user._id,
          targetProfileId: profile._id,
          groupId: group._id,
          summary: `Created member ${member.fullName} in ${member.groupName}.`,
          metadata: {
            member: {
              membershipId: member.membershipId,
              userId: member.userId,
              profileId: member.profileId,
              fullName: member.fullName,
              email: member.email || null,
              phone: member.phone || null,
              groupRole: member.role,
              membershipStatus: member.status,
              profileMembershipStatus: member.profileMembershipStatus,
              memberSerial: member.memberSerial,
              userRoles: member.userRoles,
            },
            group: {
              id: member.groupId,
              name: member.groupName,
              number: member.groupNumber,
            },
          },
        },
        session,
      );
    });
  } finally {
    await session.endSession();
  }

  return sendSuccess(res, {
    statusCode: 201,
    message: "Member created successfully",
    data: { member },
  });
});

export const updateAdminMember = catchAsync(async (req, res, next) => {
  const manageableGroupIds = await getManageableGroupIds(req);
  const session = await mongoose.startSession();
  let member = null;

  try {
    await session.withTransaction(async () => {
      const { membership, profile, group, user } = await loadMemberContext(
        req.params.membershipId,
        session,
        manageableGroupIds,
      );
      const beforeAuditState = buildMemberAuditState({
        membership,
        profile,
        user,
        group,
      });

      const currentEmail = user.email ?? profile.email ?? null;
      const currentPhoneRaw = user.phone ?? profile.phone ?? null;
      const nextEmail = hasOwn(req.body, "email")
        ? sanitizeOptionalString(req.body?.email, { lowercase: true })
        : currentEmail;
      const nextPhoneRaw = hasOwn(req.body, "phone")
        ? sanitizeOptionalString(req.body?.phone)
        : currentPhoneRaw;
      const nextPhone = nextPhoneRaw ? normalizeNigerianPhone(nextPhoneRaw) : null;
      const isUpdatingContact =
        hasOwn(req.body, "email") || hasOwn(req.body, "phone");

      if (isUpdatingContact && !nextEmail && !nextPhoneRaw) {
        throw new AppError("Either email or phone is required", 400);
      }
      if (hasOwn(req.body, "phone") && nextPhoneRaw && !nextPhone) {
        throw new AppError("Phone number must be a valid Nigerian number", 400);
      }

      const nextRole = hasOwn(req.body, "role")
        ? String(req.body?.role ?? "").trim().toLowerCase()
        : membership.role;
      const nextStatus = hasOwn(req.body, "status")
        ? String(req.body?.status ?? "").trim().toLowerCase()
        : membership.status;
      const nextProfileMembershipStatus = hasOwn(req.body, "profileMembershipStatus")
        ? String(req.body?.profileMembershipStatus ?? "").trim().toLowerCase()
        : undefined;
      const manualSerialInput = hasOwn(req.body, "memberSerial")
        ? sanitizeOptionalString(req.body?.memberSerial)
        : undefined;

      if (!GroupRoles.includes(nextRole)) {
        throw new AppError("Invalid group role", 400);
      }
      if (!GroupMembershipStatuses.includes(nextStatus)) {
        throw new AppError("Invalid membership status", 400);
      }
      if (
        nextProfileMembershipStatus !== undefined &&
        !MembershipStatuses.includes(nextProfileMembershipStatus)
      ) {
        throw new AppError("Invalid profile status", 400);
      }

      const nextOfKinPhoneRaw = hasOwn(req.body, "nextOfKinPhone")
        ? sanitizeOptionalString(req.body?.nextOfKinPhone)
        : profile.nextOfKinPhone ?? null;
      const nextOfKinPhone = nextOfKinPhoneRaw
        ? normalizeNigerianPhone(nextOfKinPhoneRaw)
        : null;
      if (nextOfKinPhoneRaw && !nextOfKinPhone) {
        throw new AppError(
          "Next of kin phone must be a valid Nigerian number",
          400,
        );
      }

      const nextManualSerial =
        manualSerialInput === undefined
          ? undefined
          : normalizeMemberSerialInput(manualSerialInput, group.groupNumber);

      const [existingEmail, existingPhone, existingSerial, existingMemberNumber] =
        await Promise.all([
        nextEmail && nextEmail !== user.email
          ? UserModel.findOne(
              { email: nextEmail, _id: { $ne: user._id } },
              { _id: 1 },
            )
              .session(session)
              .lean()
          : null,
        nextPhone && nextPhone !== user.phone
          ? UserModel.findOne(
              { phone: nextPhone, _id: { $ne: user._id } },
              { _id: 1 },
            )
              .session(session)
              .lean()
          : null,
        nextManualSerial &&
        nextManualSerial.memberSerial !== membership.memberSerial
          ? GroupMembershipModel.findOne(
              {
                memberSerial: nextManualSerial.memberSerial,
                _id: { $ne: membership._id },
              },
              { _id: 1 },
            )
              .session(session)
              .lean()
          : null,
        nextManualSerial &&
        nextManualSerial.memberNumber !== membership.memberNumber
          ? GroupMembershipModel.findOne(
              {
                groupId: group._id,
                memberNumber: nextManualSerial.memberNumber,
                _id: { $ne: membership._id },
              },
              { _id: 1 },
            )
              .session(session)
              .lean()
          : null,
        ]);

      if (existingEmail) {
        throw new AppError("Another user already uses this email", 409);
      }
      if (existingPhone) {
        throw new AppError("Another user already uses this phone number", 409);
      }
      if (existingSerial || existingMemberNumber) {
        throw new AppError("Another member already uses this serial number.", 409);
      }

      const activating = membership.status !== "active" && nextStatus === "active";
      if (activating) {
        if (!isGeneralGroup(group)) {
          const conflict = await hasNonZeroGroupMembership(profile._id, group._id);
          if (conflict) {
            throw new AppError(
              "Member already belongs to another non-general group.",
              400,
            );
          }
        }

        const activeMemberCount = await GroupMembershipModel.countDocuments({
          groupId: group._id,
          status: "active",
        }).session(session);
        if (activeMemberCount >= Number(group.maxMembers ?? 0)) {
          throw new AppError("This group has reached its member limit", 400);
        }
      }

      profile.fullName = hasOwn(req.body, "fullName")
        ? sanitizeOptionalString(req.body?.fullName) || profile.fullName
        : profile.fullName;
      profile.email = nextEmail;
      profile.phone = nextPhone;
      if (hasOwn(req.body, "address")) {
        profile.address = sanitizeOptionalString(req.body?.address);
      }
      if (hasOwn(req.body, "city")) {
        profile.city = sanitizeOptionalString(req.body?.city);
      }
      if (hasOwn(req.body, "state")) {
        profile.state = sanitizeOptionalString(req.body?.state);
      }
      if (hasOwn(req.body, "occupation")) {
        profile.occupation = sanitizeOptionalString(req.body?.occupation);
      }
      if (hasOwn(req.body, "employer")) {
        profile.employer = sanitizeOptionalString(req.body?.employer);
      }
      if (hasOwn(req.body, "nextOfKinName")) {
        profile.nextOfKinName = sanitizeOptionalString(req.body?.nextOfKinName);
      }
      if (hasOwn(req.body, "nextOfKinPhone")) {
        profile.nextOfKinPhone = nextOfKinPhone;
      }
      if (hasOwn(req.body, "nextOfKinRelationship")) {
        profile.nextOfKinRelationship = sanitizeOptionalString(
          req.body?.nextOfKinRelationship,
        );
      }
      if (nextProfileMembershipStatus !== undefined) {
        profile.membershipStatus = nextProfileMembershipStatus;
      }
      await profile.save({ session, validateBeforeSave: true });

      user.email = nextEmail;
      user.phone = nextPhone;
      await user.save({ session, validateBeforeSave: false });

      membership.role = nextRole;
      membership.status = nextStatus;
      if (activating && !membership.joinedAt) {
        membership.joinedAt = new Date();
      }
      if (hasOwn(req.body, "reviewNotes")) {
        membership.reviewNotes = sanitizeOptionalString(req.body?.reviewNotes);
      }
      if (manualSerialInput !== undefined) {
        if (nextManualSerial) {
          membership.memberSerial = nextManualSerial.memberSerial;
          membership.memberNumber = nextManualSerial.memberNumber;
        } else {
          membership.memberSerial = null;
          membership.memberNumber = null;
        }
      }
      await membership.save({ session, validateBeforeSave: true });

      await syncUserCoordinatorRole(profile._id, session);
      if (membership.status === "active") {
        await assignGroupMemberSerial({ membership, group, session });
      }
      await recomputeGroupSnapshot(group._id, session);

      member = await buildMemberDetail(
        membership._id,
        session,
        manageableGroupIds,
      );
      const afterAuditState = buildMemberAuditStateFromDetail(member);
      const auditDiff = buildMemberAuditChanges(beforeAuditState, afterAuditState);

      if (auditDiff.changedFields.length > 0) {
        await createAuditLog(
          {
            req,
            action: AuditActions.ADMIN_MEMBER_UPDATE,
            entityType: AuditEntityTypes.GROUP_MEMBERSHIP,
            entityId: membership._id,
            membershipId: membership._id,
            targetUserId: user._id,
            targetProfileId: profile._id,
            groupId: group._id,
            summary: `Updated member ${member.fullName} in ${member.groupName}.`,
            metadata: {
              member: {
                membershipId: member.membershipId,
                userId: member.userId,
                profileId: member.profileId,
                fullName: member.fullName,
                memberSerial: member.memberSerial,
              },
              changedFields: auditDiff.changedFields,
              changes: auditDiff.changes,
            },
          },
          session,
        );
      }
    });
  } finally {
    await session.endSession();
  }

  return sendSuccess(res, {
    statusCode: 200,
    message: "Member updated successfully",
    data: { member },
  });
});

export const deleteAdminMember = catchAsync(async (req, res, next) => {
  const manageableGroupIds = await getManageableGroupIds(req);
  const session = await mongoose.startSession();
  let responseSummary = null;

  try {
    await session.withTransaction(async () => {
      const { membership, profile, user, group } = await loadMemberContext(
        req.params.membershipId,
        session,
        manageableGroupIds,
      );
      const beforeAuditState = buildMemberAuditState({
        membership,
        profile,
        user,
        group,
      });

      const userRoles = normalizeUserRoles(user);
      if (userRoles.includes("admin") || userRoles.includes("groupCoordinator")) {
        throw new AppError(
          "Remove admin or coordinator privileges before deleting this member.",
          400,
        );
      }

      const confirmation = String(req.body?.confirmation ?? "").trim();
      const allowedConfirmations = [
        membership.memberSerial ? String(membership.memberSerial).trim() : null,
        profile.fullName ? String(profile.fullName).trim() : null,
      ].filter(Boolean);

      if (!confirmation || !allowedConfirmations.includes(confirmation)) {
        throw new AppError(
          "Confirmation text does not match this member record.",
          400,
        );
      }

      const memberships = await GroupMembershipModel.find(
        { userId: profile._id },
        { groupId: 1 },
      )
        .session(session)
        .lean();
      const affectedGroupIds = [
        ...new Set(memberships.map((entry) => String(entry.groupId))),
      ];

      const loanApplications = await LoanApplicationModel.find(
        { $or: [{ userId: profile._id }, { profileId: profile._id }] },
        { _id: 1 },
      )
        .session(session)
        .lean();
      const loanIds = loanApplications.map((entry) => entry._id);

      const guarantorRecords = await LoanGuarantorModel.find(
        {
          $or: [
            { guarantorUserId: profile._id },
            ...(loanIds.length > 0
              ? [{ loanApplicationId: { $in: loanIds } }]
              : []),
          ],
        },
        { _id: 1 },
      )
        .session(session)
        .lean();
      const guarantorIds = guarantorRecords.map((entry) => entry._id);

      await Promise.all([
        ContributionModel.deleteMany({ userId: profile._id }, { session }),
        ContributionSettingModel.deleteMany({ userId: profile._id }, { session }),
        GroupMembershipModel.deleteMany({ userId: profile._id }, { session }),
        NotificationModel.deleteMany({ userId: profile._id }, { session }),
        NotificationPreferenceModel.deleteMany(
          { userId: profile._id },
          { session },
        ),
        RecurringPaymentModel.deleteMany({ userId: profile._id }, { session }),
        TransactionModel.deleteMany({ userId: profile._id }, { session }),
        WithdrawalRequestModel.deleteMany({ userId: profile._id }, { session }),
        BankAccountModel.deleteMany({ userId: profile._id }, { session }),
        MeetingAttendanceModel.deleteMany({ userId: profile._id }, { session }),
        MeetingRsvpModel.deleteMany({ userId: profile._id }, { session }),
        GroupVoteResponseModel.deleteMany({ userId: profile._id }, { session }),
        GroupVoteModel.updateMany(
          { createdBy: profile._id },
          { $set: { createdBy: null } },
          { session },
        ),
        LoanApplicationEditRequestModel.deleteMany(
          {
            $or: [
              { userId: profile._id },
              ...(loanIds.length > 0
                ? [{ loanApplicationId: { $in: loanIds } }]
                : []),
            ],
          },
          { session },
        ),
        LoanRepaymentScheduleItemModel.deleteMany(
          loanIds.length > 0 ? { loanApplicationId: { $in: loanIds } } : { _id: null },
          { session },
        ),
        LoanApplicationModel.updateMany(
          { "guarantors.profileId": profile._id },
          { $pull: { guarantors: { profileId: profile._id } } },
          { session },
        ),
        LoanApplicationModel.deleteMany(
          {
            $or: [{ userId: profile._id }, { profileId: profile._id }],
          },
          { session },
        ),
        LoanGuarantorModel.deleteMany(
          {
            $or: [
              { guarantorUserId: profile._id },
              ...(loanIds.length > 0
                ? [{ loanApplicationId: { $in: loanIds } }]
                : []),
            ],
          },
          { session },
        ),
        GuarantorNotificationModel.deleteMany(
          guarantorIds.length > 0 ? { guarantorId: { $in: guarantorIds } } : { _id: null },
          { session },
        ),
        RefreshTokenModel.deleteMany({ userId: user._id }, { session }),
        LoginHistoryModel.deleteMany({ userId: user._id }, { session }),
        UserModel.deleteOne({ _id: user._id }, { session }),
        ProfileModel.deleteOne({ _id: profile._id }, { session }),
      ]);

      for (const groupId of affectedGroupIds) {
        await recomputeGroupSnapshot(groupId, session);
      }

      responseSummary = {
        membershipId: String(membership._id),
        profileId: String(profile._id),
        userId: String(user._id),
        deletedMemberships: memberships.length,
        affectedGroups: affectedGroupIds.length,
        deletedLoanApplications: loanIds.length,
      };

      await createAuditLog(
        {
          req,
          action: AuditActions.ADMIN_MEMBER_DELETE,
          entityType: AuditEntityTypes.GROUP_MEMBERSHIP,
          entityId: membership._id,
          membershipId: membership._id,
          targetUserId: user._id,
          targetProfileId: profile._id,
          groupId: membership.groupId,
          summary: `Deleted member ${beforeAuditState.fullName || "member"} from ${beforeAuditState.groupName || "group"}.`,
          metadata: {
            member: {
              membershipId: beforeAuditState.membershipId,
              userId: beforeAuditState.userId,
              profileId: beforeAuditState.profileId,
              fullName: beforeAuditState.fullName,
              email: beforeAuditState.email,
              phone: beforeAuditState.phone,
              memberSerial: beforeAuditState.memberSerial,
              groupRole: beforeAuditState.groupRole,
              membershipStatus: beforeAuditState.membershipStatus,
              profileMembershipStatus: beforeAuditState.profileMembershipStatus,
              userRoles: beforeAuditState.userRoles,
            },
            deletionSummary: responseSummary,
          },
        },
        session,
      );
    });
  } finally {
    await session.endSession();
  }

  return sendSuccess(res, {
    statusCode: 200,
    message: "Member deleted permanently",
    data: { summary: responseSummary },
  });
});
