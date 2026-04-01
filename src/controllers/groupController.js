import AppError from "../utils/AppError.js";
import catchAsync from "../utils/catchAsync.js";
import sendSuccess from "../utils/sendSuccess.js";
import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel, GroupRoles } from "../models/GroupMembership.js";
import { ProfileModel } from "../models/Profile.js";
import {
  assignGroupMemberSerial,
  formatGroupMemberSerial,
  reserveGroupMemberNumbers,
} from "../utils/groupMemberSerial.js";
import {
  findUsersWithNonZeroGroupMembership,
  hasNonZeroGroupMembership,
  isGeneralGroup,
} from "../utils/groupMembershipPolicy.js";
import { UserModel } from "../models/User.js";

function pick(obj, allowedKeys) {
  const out = {};
  for (const key of allowedKeys) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) out[key] = obj[key];
  }
  return out;
}

async function getNextGroupNumber() {
  const last = await GroupModel.findOne()
    .sort({ groupNumber: -1 })
    .select("groupNumber");
  const next = (last?.groupNumber ?? 0) + 1;
  return next;
}

export const listGroups = catchAsync(async (req, res) => {
  const filter = {};

  if (typeof req.query?.status === "string" && req.query.status.trim()) {
    filter.status = req.query.status.trim();
  }

  if (typeof req.query?.isOpen !== "undefined") {
    const v = String(req.query.isOpen).toLowerCase();
    if (v === "true" || v === "false") filter.isOpen = v === "true";
  }

  if (typeof req.query?.isSpecial !== "undefined") {
    const v = String(req.query.isSpecial).toLowerCase();
    if (v === "true" || v === "false") filter.isSpecial = v === "true";
  }

  if (typeof req.query?.category === "string" && req.query.category.trim()) {
    filter.category = req.query.category.trim();
  }

  if (typeof req.query?.location === "string" && req.query.location.trim()) {
    filter.location = req.query.location.trim();
  }

  const search =
    typeof req.query?.search === "string" ? req.query.search.trim() : "";
  if (search) {
    filter.groupName = { $regex: search, $options: "i" };
  }

  const page = Math.max(1, parseInt(String(req.query?.page ?? "1"), 10) || 1);
  const limit = Math.min(
    200,
    Math.max(1, parseInt(String(req.query?.limit ?? "50"), 10) || 50),
  );
  const skip = (page - 1) * limit;

  const sortKey = String(req.query?.sort ?? "groupNumber");
  const sortDir =
    String(req.query?.order ?? "asc").toLowerCase() === "desc" ? -1 : 1;
  const sort = (() => {
    switch (sortKey) {
      case "createdAt":
      case "totalSavings":
      case "memberCount":
        return { [sortKey]: sortDir };
      case "groupNumber":
      default:
        return { groupNumber: sortDir };
    }
  })();

  const [groups, total] = await Promise.all([
    GroupModel.find(filter).sort(sort).skip(skip).limit(limit),
    GroupModel.countDocuments(filter),
  ]);

  return sendSuccess(res, {
    statusCode: 200,
    results: groups.length,
    total,
    page,
    limit,
    data: { groups },
  });
});

export const getGroup = catchAsync(async (req, res) => {
  const group = req.group;
  const membership = req.groupMembership || null;

  return sendSuccess(res, {
    statusCode: 200,
    data: { group, membership },
  });
});

export const createGroup = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const allowed = [
    "groupNumber",
    "groupName",
    "description",
    "category",
    "location",
    "meetingFrequency",
    "meetingDay",
    "rules",
    "imageUrl",
    "isOpen",
    "monthlyContribution",
    "maxMembers",
    "isSpecial",
    "status",
    "coordinatorId",
    "coordinatorName",
    "coordinatorPhone",
    "coordinatorEmail",
  ];

  const input = pick(req.body || {}, allowed);

  if (!input.groupName || !String(input.groupName).trim()) {
    return next(new AppError("groupName is required", 400));
  }
  if (typeof input.monthlyContribution !== "number") {
    return next(new AppError("monthlyContribution must be a number", 400));
  }
  if (typeof input.maxMembers !== "number") {
    return next(new AppError("maxMembers must be a number", 400));
  }

  if (typeof input.groupNumber !== "number") {
    input.groupNumber = await getNextGroupNumber();
  }

  // Default coordinator to current user if not explicitly set
  const coordinatorProfileId = input.coordinatorId || req.user.profileId;
  const coordinatorProfile = await ProfileModel.findById(coordinatorProfileId);
  if (!coordinatorProfile)
    return next(new AppError("Coordinator profile not found", 400));

  input.coordinatorId = coordinatorProfile._id;
  input.coordinatorName =
    coordinatorProfile.fullName ?? input.coordinatorName ?? null;
  input.coordinatorPhone =
    coordinatorProfile.phone ?? input.coordinatorPhone ?? null;
  input.coordinatorEmail =
    coordinatorProfile.email ?? input.coordinatorEmail ?? null;

  const group = await GroupModel.create(input);

  // Ensure coordinator is also a member (best-effort)
  const coordinatorMembership = await GroupMembershipModel.findOneAndUpdate(
    { groupId: group._id, userId: coordinatorProfile._id },
    {
      groupId: group._id,
      userId: coordinatorProfile._id,
      role: "coordinator",
      status: "active",
    },
    { upsert: true, new: true, runValidators: true, setDefaultsOnInsert: true },
  );
  if (coordinatorMembership) {
    await assignGroupMemberSerial({ membership: coordinatorMembership, group });
  }

  // Update counters (best-effort)
  await GroupModel.findByIdAndUpdate(group._id, { $set: { memberCount: 1 } });

  return sendSuccess(res, {
    statusCode: 201,
    message: "Group created successfully",
    data: { group },
  });
});

export const updateGroup = catchAsync(async (req, res, next) => {
  const group = req.group;

  const updates = pick(req.body || {}, [
    "groupName",
    "description",
    "category",
    "location",
    "meetingFrequency",
    "meetingDay",
    "rules",
    "imageUrl",
    "isOpen",
    "monthlyContribution",
    "maxMembers",
    "isSpecial",
    "status",
  ]);

  if (Object.keys(updates).length === 0) {
    return next(new AppError("No updatable fields provided", 400));
  }

  const updated = await GroupModel.findByIdAndUpdate(group._id, updates, {
    new: true,
    runValidators: true,
  });

  return sendSuccess(res, { statusCode: 200, data: { group: updated } });
});

export const archiveGroup = catchAsync(async (req, res) => {
  const group = req.group;

  const updated = await GroupModel.findByIdAndUpdate(
    group._id,
    { status: "archived", isOpen: false },
    { new: true, runValidators: true },
  );

  return sendSuccess(res, {
    statusCode: 200,
    message: "Group archived successfully",
    data: { group: updated },
  });
});

export const setCoordinator = catchAsync(async (req, res, next) => {
  const group = req.group;
  const { coordinatorProfileId, removeCoordinator } = req.body || {};
  const wantsRemove =
    removeCoordinator === true ||
    coordinatorProfileId === null ||
    coordinatorProfileId === "";

  if (wantsRemove) {
    if (group.coordinatorId) {
      await GroupMembershipModel.updateOne(
        { groupId: group._id, userId: group.coordinatorId },
        { $set: { role: "member" } },
      );
    }

    const updated = await GroupModel.findByIdAndUpdate(
      group._id,
      {
        coordinatorId: null,
        coordinatorName: null,
        coordinatorPhone: null,
        coordinatorEmail: null,
      },
      { new: true, runValidators: true },
    );

    return sendSuccess(res, { statusCode: 200, data: { group: updated } });
  }

  if (!coordinatorProfileId) {
    return next(new AppError("Coordinator profile id is required", 400));
  }

  const coordinatorProfile = await ProfileModel.findById(coordinatorProfileId);
  if (!coordinatorProfile) {
    return next(new AppError("Coordinator profile not found", 400));
  }

  const membership = await GroupMembershipModel.findOne({
    groupId: group._id,
    userId: coordinatorProfile._id,
    status: "active",
  }).lean();
  if (!membership) {
    return next(
      new AppError("Coordinator must be an active group member", 400),
    );
  }

  if (
    group.coordinatorId &&
    String(group.coordinatorId) !== String(coordinatorProfile._id)
  ) {
    await GroupMembershipModel.updateOne(
      { groupId: group._id, userId: group.coordinatorId },
      { $set: { role: "member" } },
    );
  }

  const patch = {
    coordinatorId: coordinatorProfile._id,
    coordinatorName: coordinatorProfile.fullName ?? null,
    coordinatorPhone: coordinatorProfile.phone ?? null,
    coordinatorEmail: coordinatorProfile.email ?? null,
  };

  const updated = await GroupModel.findByIdAndUpdate(group._id, patch, {
    new: true,
    runValidators: true,
  });

  await GroupMembershipModel.updateOne(
    { groupId: group._id, userId: coordinatorProfile._id },
    { $set: { role: "coordinator", status: "active" } },
  );

  const user = await UserModel.findOne({ profileId: coordinatorProfile._id })
    .select("role")
    .lean();
  if (!user.role.includes("admin")) {
    await UserModel.updateOne(
      { profileId: coordinatorProfile._id },
      { $addToSet: { role: "groupCoordinator" } },
    );
  }

  return sendSuccess(res, { statusCode: 200, data: { group: updated } });
});

export const joinGroup = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const group = req.group;
  if (!group.isOpen)
    return next(new AppError("Group is not open to new members", 400));
  if (group.memberCount >= group.maxMembers) {
    return next(new AppError("Group is full", 400));
  }

  const userProfileId = req.user.profileId;

  const existing = await GroupMembershipModel.findOne({
    groupId: group._id,
    userId: userProfileId,
  });

  if (existing && existing.status === "active") {
    return next(new AppError("Already a member of this group", 400));
  }

  if (existing && existing.status === "pending") {
    return sendSuccess(res, {
      statusCode: 200,
      message: "Membership request already submitted",
      data: { membership: existing },
    });
  }

  if (!isGeneralGroup(group)) {
    const conflict = await hasNonZeroGroupMembership(userProfileId, group._id);
    if (conflict) {
      return next(
        new AppError(
          "You can only join one group. Group 0 is the only additional group allowed.",
          400,
        ),
      );
    }
  }

  const membership = await GroupMembershipModel.findOneAndUpdate(
    { groupId: group._id, userId: userProfileId },
    {
      groupId: group._id,
      userId: userProfileId,
      status: "pending",
      requestedAt: new Date(),
      reviewedAt: null,
      reviewedBy: null,
      reviewNotes: null,
    },
    { upsert: true, new: true, runValidators: true, setDefaultsOnInsert: true },
  );

  return sendSuccess(res, {
    statusCode: 200,
    message: "Membership request submitted",
    data: { membership },
  });
});

export const leaveGroup = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const group = req.group;
  const userProfileId = req.user.profileId;

  const membership = await GroupMembershipModel.findOne({
    groupId: group._id,
    userId: userProfileId,
  });

  if (!membership || membership.status !== "active") {
    return next(new AppError("Not an active member of this group", 400));
  }

  if (membership.role === "coordinator") {
    return next(
      new AppError("Coordinator cannot leave without reassignment", 400),
    );
  }

  membership.status = "inactive";
  await membership.save({ validateBeforeSave: true });

  await GroupModel.findByIdAndUpdate(
    group._id,
    { $inc: { memberCount: -1 } },
    { new: true },
  );

  return sendSuccess(res, {
    statusCode: 200,
    message: "Left group successfully",
  });
});

export const listGroupMembers = catchAsync(async (req, res) => {
  const group = req.group;
  const search =
    typeof req.query?.search === "string" ? req.query.search.trim() : "";
  const status =
    typeof req.query?.status === "string" && req.query.status.trim()
      ? String(req.query.status).trim()
      : null;

  const filter = { groupId: group._id };
  if (status) filter.status = status;

  const memberships = await GroupMembershipModel.find(filter)
    .sort({ joinedAt: -1 })
    .populate("userId");

  const normalizedSearch = search.toLowerCase();
  const filtered = search
    ? memberships.filter((m) => {
        const profile =
          m.userId && typeof m.userId === "object" ? m.userId : null;
        const name = profile?.fullName
          ? String(profile.fullName).toLowerCase()
          : "";
        const email = profile?.email ? String(profile.email).toLowerCase() : "";
        const phone = profile?.phone ? String(profile.phone).toLowerCase() : "";
        return (
          name.includes(normalizedSearch) ||
          email.includes(normalizedSearch) ||
          phone.includes(normalizedSearch)
        );
      })
    : memberships;

  return sendSuccess(res, {
    statusCode: 200,
    results: filtered.length,
    data: { members: filtered },
  });
});

export const listGroupMemberCandidates = catchAsync(async (req, res) => {
  const group = req.group;
  const search =
    typeof req.query?.search === "string" ? req.query.search.trim() : "";

  const activeMemberships = await GroupMembershipModel.find(
    { groupId: group._id, status: "active" },
    { userId: 1 },
  ).lean();
  const activeIds = activeMemberships.map((m) => m.userId).filter(Boolean);

  const filter = { _id: { $nin: activeIds } };
  if (search) {
    filter.$or = [
      { fullName: { $regex: search, $options: "i" } },
      { email: { $regex: search, $options: "i" } },
      { phone: { $regex: search, $options: "i" } },
    ];
  }

  const candidates = await ProfileModel.find(filter)
    .sort({ createdAt: -1 })
    .limit(50)
    .lean();

  let filteredCandidates = candidates;
  if (!isGeneralGroup(group) && candidates.length > 0) {
    const blockedIds = await findUsersWithNonZeroGroupMembership(
      candidates.map((profile) => profile._id),
      group._id,
    );
    if (blockedIds.size > 0) {
      filteredCandidates = candidates.filter(
        (profile) => !blockedIds.has(String(profile._id)),
      );
    }
  }

  return sendSuccess(res, {
    statusCode: 200,
    results: filteredCandidates.length,
    data: {
      candidates: filteredCandidates.map((profile) => ({
        id: profile._id,
        fullName: profile.fullName,
        email: profile.email,
        phone: profile.phone,
        avatarUrl: profile.avatar?.url ?? null,
      })),
    },
  });
});

export const addGroupMembers = catchAsync(async (req, res, next) => {
  if (!req.user) return next(new AppError("Not authenticated", 401));
  if (!req.user.profileId)
    return next(new AppError("User profile not found", 400));

  const group = req.group;
  const inputIds = Array.isArray(req.body?.userIds)
    ? req.body.userIds
    : req.body?.userId
      ? [req.body.userId]
      : [];
  const uniqueIds = [
    ...new Set(inputIds.map((id) => String(id)).filter(Boolean)),
  ];

  if (uniqueIds.length === 0) {
    return next(new AppError("userIds is required", 400));
  }

  const role = req.body?.role || "member";
  if (!GroupRoles.includes(role)) {
    return next(
      new AppError(`Invalid role. Allowed: ${GroupRoles.join(", ")}`, 400),
    );
  }

  const existingMemberships = await GroupMembershipModel.find({
    groupId: group._id,
    userId: { $in: uniqueIds },
  })
    .select("userId status memberNumber memberSerial joinedAt")
    .lean();

  const activeIds = new Set(
    existingMemberships
      .filter((m) => m.status === "active")
      .map((m) => String(m.userId)),
  );

  const targetIds = uniqueIds.filter((id) => !activeIds.has(id));

  if (targetIds.length === 0) {
    return sendSuccess(res, {
      statusCode: 200,
      data: { added: 0, skipped: uniqueIds.length, missing: 0 },
    });
  }

  const profiles = await ProfileModel.find({ _id: { $in: targetIds } }).lean();
  const profileIdSet = new Set(profiles.map((p) => String(p._id)));
  const missing = targetIds.filter((id) => !profileIdSet.has(id));
  const validIds = targetIds.filter((id) => profileIdSet.has(id));

  if (validIds.length === 0) {
    return sendSuccess(res, {
      statusCode: 200,
      data: { added: 0, skipped: activeIds.size, missing: missing.length },
    });
  }

  let conflictIds = new Set();
  let eligibleIds = validIds;
  if (!isGeneralGroup(group)) {
    conflictIds = await findUsersWithNonZeroGroupMembership(
      validIds,
      group._id,
    );
    if (conflictIds.size > 0) {
      eligibleIds = validIds.filter((id) => !conflictIds.has(String(id)));
    }
  }

  if (eligibleIds.length === 0) {
    return sendSuccess(res, {
      statusCode: 200,
      data: {
        added: 0,
        skipped: activeIds.size + conflictIds.size,
        missing: missing.length,
        conflicts: conflictIds.size,
      },
    });
  }

  const maxMembers = Number(group.maxMembers ?? 0);
  if (maxMembers > 0) {
    const available = Math.max(0, maxMembers - Number(group.memberCount ?? 0));
    if (eligibleIds.length > available) {
      return next(new AppError("Group is full", 400));
    }
  }

  const existingByUserId = new Map(
    existingMemberships.map((membership) => [
      String(membership.userId),
      membership,
    ]),
  );

  const normalizeMemberNumber = (value) => {
    const num = Number(value);
    return Number.isFinite(num) && num > 0 ? num : null;
  };

  const needsNumberIds = eligibleIds.filter((userId) => {
    const existing = existingByUserId.get(String(userId));
    return !normalizeMemberNumber(existing?.memberNumber);
  });

  const reservedRange =
    needsNumberIds.length > 0
      ? await reserveGroupMemberNumbers(group._id, needsNumberIds.length)
      : null;
  let nextMemberNumber = reservedRange?.start ?? null;

  const now = new Date();
  const ops = eligibleIds.map((userId) => {
    const existing = existingByUserId.get(String(userId));
    const existingNumber = normalizeMemberNumber(existing?.memberNumber);
    const memberNumber =
      existingNumber ?? (nextMemberNumber ? nextMemberNumber++ : null);
    const joinedAt =
      existing?.joinedAt && !Number.isNaN(new Date(existing.joinedAt).getTime())
        ? new Date(existing.joinedAt)
        : now;
    const memberSerial =
      existing?.memberSerial ||
      (memberNumber
        ? formatGroupMemberSerial({
            joinedAt,
            groupNumber: group.groupNumber,
            memberNumber,
          })
        : null);

    return {
      updateOne: {
        filter: { groupId: group._id, userId },
        update: {
          $set: {
            groupId: group._id,
            userId,
            role,
            status: "active",
            joinedAt,
            requestedAt: now,
            reviewedBy: req.user.profileId,
            reviewedAt: now,
            reviewNotes: "Added by admin/coordinator",
            memberNumber,
            memberSerial,
          },
          $setOnInsert: { totalContributed: 0 },
        },
        upsert: true,
      },
    };
  });

  await GroupMembershipModel.bulkWrite(ops, { ordered: false });

  if (eligibleIds.length > 0) {
    await GroupModel.findByIdAndUpdate(group._id, {
      $inc: { memberCount: eligibleIds.length },
    });
  }

  return sendSuccess(res, {
    statusCode: 200,
    data: {
      added: eligibleIds.length,
      skipped: activeIds.size + conflictIds.size,
      missing: missing.length,
      conflicts: conflictIds.size,
    },
  });
});

export const updateGroupMember = catchAsync(async (req, res, next) => {
  const group = req.group;
  const { memberId } = req.params;
  const { role, status } = req.body || {};

  const updates = {};
  if (typeof role !== "undefined") {
    if (!GroupRoles.includes(role)) {
      return next(
        new AppError(`Invalid role. Allowed: ${GroupRoles.join(", ")}`, 400),
      );
    }
    updates.role = role;
  }
  if (typeof status !== "undefined") {
    updates.status = String(status).trim();
  }

  if (Object.keys(updates).length === 0) {
    return next(new AppError("No updatable fields provided", 400));
  }

  if (updates.status === "active" && !isGeneralGroup(group)) {
    const existingMembership = await GroupMembershipModel.findOne({
      _id: memberId,
      groupId: group._id,
    });

    if (!existingMembership) {
      return next(new AppError("Group member not found", 404));
    }

    const conflict = await hasNonZeroGroupMembership(
      existingMembership.userId,
      group._id,
    );
    if (conflict) {
      return next(
        new AppError(
          "Member already belongs to another group. Group 0 is the only additional group allowed.",
          400,
        ),
      );
    }
  }

  const membership = await GroupMembershipModel.findOneAndUpdate(
    { _id: memberId, groupId: group._id },
    updates,
    { new: true, runValidators: true },
  );

  if (!membership) return next(new AppError("Group member not found", 404));

  if (membership.status === "active") {
    await assignGroupMemberSerial({ membership, group });
  }

  return sendSuccess(res, { statusCode: 200, data: { member: membership } });
});
