import fs from "fs/promises";
import path from "path";
import mongoose from "mongoose";
import bcrypt from "bcryptjs";
import dotenv from "dotenv";
import { connectMongo } from "../../../db.js";
import { UserModel } from "../../../models/User.js";
import { ProfileModel } from "../../../models/Profile.js";
import { GroupModel } from "../../../models/Group.js";
import { GroupMembershipModel } from "../../../models/GroupMembership.js";
import { ContributionModel } from "../../../models/Contribution.js";
import { ContributionSettingModel } from "../../../models/ContributionSetting.js";
import { TransactionModel } from "../../../models/Transaction.js";
import { formatGroupMemberSerial, ensureGroupMemberSequence } from "../../../utils/groupMemberSerial.js";
import {
  coerceUserRoles,
  normalizeUserRoles,
  pickPrimaryRole,
} from "../../../utils/roles.js";

dotenv.config();

const readJson = async (filePath) => {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
};

const fileExists = async (filePath) => {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
};

const asArray = (value) => (Array.isArray(value) ? value : []);

const requireFields = (item, fields) => {
  const missing = [];
  fields.forEach((field) => {
    if (
      item[field] === undefined ||
      item[field] === null ||
      item[field] === ""
    ) {
      missing.push(field);
    }
  });
  return missing;
};

const requireAnyField = (item, fields) => {
  if (!Array.isArray(fields) || fields.length === 0) return [];
  const hasAny = fields.some((field) => {
    const value = item[field];
    return value !== undefined && value !== null && value !== "";
  });
  return hasAny ? [] : [`one_of:${fields.join("|")}`];
};

const collectValues = (items, key) =>
  items
    .map((item) => item[key])
    .filter((value) => value !== null && value !== undefined);

const unique = (values) => Array.from(new Set(values));

const mapBy = (items, key) => {
  const map = new Map();
  items.forEach((item) => {
    const value = typeof key === "function" ? key(item) : item[key];
    if (!value) return;
    map.set(String(value), item);
  });
  return map;
};

const normalizeMemberNumber = (value) => {
  const num = Number(value);
  return Number.isFinite(num) && num > 0 ? num : null;
};

const stripFields = (item, fields) => {
  if (!item) return item;
  const copy = { ...item };
  fields.forEach((field) => {
    if (Object.prototype.hasOwnProperty.call(copy, field)) {
      delete copy[field];
    }
  });
  return copy;
};

const isBcryptHash = (value) => {
  if (!value) return false;
  const text = String(value);
  return /^\$2[aby]\$\d{2}\$/.test(text);
};

const hashPasswordIfNeeded = async (password) => {
  if (!password) return password;
  if (isBcryptHash(password)) return password;
  return bcrypt.hash(String(password), 12);
};

const resolveSeedUserRoles = (seedUser, existingUser = null) => {
  const rawRoles = Array.isArray(seedUser?.roles) ? seedUser.roles : seedUser?.role;
  let roles = coerceUserRoles(rawRoles);
  if (roles.length === 0 && existingUser) {
    roles = normalizeUserRoles(existingUser);
  }
  if (roles.length === 0) roles = ["member"];
  if (!roles.includes("member")) roles.push("member");
  return Array.from(new Set(roles));
};

export async function loadSeedFiles({
  inputDir,
  usersPath,
  profilesPath,
  groupsPath,
  groupMembersPath,
  contributionsPath,
  transactionsPath,
  contributionSettingsPath,
}) {
  const resolved = {
    usersPath,
    profilesPath,
    groupsPath,
    groupMembersPath,
    contributionsPath,
    transactionsPath,
    contributionSettingsPath,
  };

  const [
    users,
    profiles,
    groups,
    groupMembers,
    contributions,
    contributionSettings,
  ] = await Promise.all([
    readJson(resolved.usersPath),
    readJson(resolved.profilesPath),
    readJson(resolved.groupsPath),
    readJson(resolved.groupMembersPath),
    readJson(resolved.contributionsPath),
    readJson(resolved.contributionSettingsPath),
  ]);

  const transactions =
    resolved.transactionsPath && (await fileExists(resolved.transactionsPath))
      ? await readJson(resolved.transactionsPath)
      : [];

  return {
    inputDir,
    users: asArray(users),
    profiles: asArray(profiles),
    groups: asArray(groups),
    groupMembers: asArray(groupMembers),
    contributions: asArray(contributions),
    contributionSettings: asArray(contributionSettings),
    transactions: asArray(transactions),
    paths: resolved,
  };
}

export async function seedContributionData({
  inputDir,
  usersPath,
  profilesPath,
  groupsPath,
  groupMembersPath,
  contributionsPath,
  transactionsPath,
  contributionSettingsPath,
  dryRun = false,
  reset = false,
  reindex = false,
}) {
  const mongoUri = process.env.MONGO_URI;
  if (!mongoUri) {
    throw new Error("Missing MONGO_URI");
  }

  const seed = await loadSeedFiles({
    inputDir,
    usersPath,
    profilesPath,
    groupsPath,
    groupMembersPath,
    contributionsPath,
    contributionSettingsPath,
  });

  const stats = {
    users: {
      total: seed.users.length,
      inserted: 0,
      existing: 0,
      updated: 0,
      skipped: 0,
    },
    profiles: {
      total: seed.profiles.length,
      inserted: 0,
      existing: 0,
      skipped: 0,
    },
    groups: { total: seed.groups.length, upserted: 0, modified: 0 },
    groupMembers: { total: seed.groupMembers.length, upserted: 0, skipped: 0 },
    contributionSettings: {
      total: seed.contributionSettings.length,
      upserted: 0,
      skipped: 0,
    },
    contributions: {
      total: seed.contributions.length,
      upserted: 0,
      skipped: 0,
    },
    transactions: {
      total: seed.transactions.length,
      upserted: 0,
      skipped: 0,
    },
    reset,
    reindex,
    warnings: [],
    errors: [],
  };

  const profileIds = new Set(collectValues(seed.profiles, "_id").map(String));
  const groupIds = new Set(collectValues(seed.groups, "_id").map(String));

  const validateCollection = (
    items,
    requiredFields,
    name,
    referenceChecks = [],
    anyOfFields = [],
  ) => {
    const valid = [];
    items.forEach((item, index) => {
      const missing = [
        ...requireFields(item, requiredFields),
        ...requireAnyField(item, anyOfFields),
      ];
      if (missing.length) {
        stats.errors.push(`${name}[${index}] missing: ${missing.join(", ")}`);
        return;
      }
      for (const check of referenceChecks) {
        const value = item[check.field];
        if (value && !check.set.has(String(value))) {
          stats.errors.push(
            `${name}[${index}] references missing ${check.field}: ${value}`,
          );
          return;
        }
      }
      valid.push(item);
    });
    return valid;
  };

  const validUsers = validateCollection(
    seed.users,
    ["profileId", "password"],
    "users",
    [],
    ["email", "phone"],
  );
  const validProfiles = validateCollection(
    seed.profiles,
    ["_id"],
    "profiles",
    [],
    ["email", "phone"],
  );
  const validGroups = validateCollection(
    seed.groups,
    ["groupNumber", "groupName", "monthlyContribution", "maxMembers"],
    "groups",
  );
  const validGroupMembers = validateCollection(
    seed.groupMembers,
    ["userId", "groupId"],
    "groupMembers",
    [
      { field: "userId", set: profileIds },
      { field: "groupId", set: groupIds },
    ],
  );
  const validContributionSettings = validateCollection(
    seed.contributionSettings,
    ["userId", "groupId", "expectedMonthlyAmount", "totalExpected"],
    "contributionSettings",
    [
      { field: "userId", set: profileIds },
      { field: "groupId", set: groupIds },
    ],
  );
  const validContributions = validateCollection(
    seed.contributions,
    ["userId", "groupId", "month", "year", "amount", "contributionType"],
    "contributions",
    [
      { field: "userId", set: profileIds },
      { field: "groupId", set: groupIds },
    ],
  );
  const validTransactions = validateCollection(
    seed.transactions,
    ["userId", "reference", "amount", "type", "status"],
    "transactions",
    [
      { field: "userId", set: profileIds },
      { field: "groupId", set: groupIds },
    ],
  );

  if (dryRun) {
    return {
      ok: 1,
      dryRun: true,
      inputDir,
      paths: seed.paths,
      stats,
    };
  }

  await connectMongo({ mongoUri });

  if (reindex) {
    await Promise.all([
      UserModel.syncIndexes(),
      ProfileModel.syncIndexes(),
      GroupModel.syncIndexes(),
      GroupMembershipModel.syncIndexes(),
      ContributionSettingModel.syncIndexes(),
      ContributionModel.syncIndexes(),
    ]);
  }

  if (reset) {
    await Promise.all([
      UserModel.deleteMany({ _id: { $in: collectValues(validUsers, "_id") } }),
      ProfileModel.deleteMany({
        _id: { $in: collectValues(validProfiles, "_id") },
      }),
      GroupModel.deleteMany({
        _id: { $in: collectValues(validGroups, "_id") },
      }),
      GroupMembershipModel.deleteMany({
        _id: { $in: collectValues(validGroupMembers, "_id") },
      }),
      ContributionSettingModel.deleteMany({
        _id: { $in: collectValues(validContributionSettings, "_id") },
      }),
      ContributionModel.deleteMany({
        _id: { $in: collectValues(validContributions, "_id") },
      }),
      TransactionModel.deleteMany({
        _id: { $in: collectValues(validTransactions, "_id") },
      }),
    ]);
  }

  const userEmails = unique(collectValues(validUsers, "email")).filter(Boolean);
  const userPhones = unique(collectValues(validUsers, "phone")).filter(Boolean);

  const userFilters = [];
  if (userEmails.length) userFilters.push({ email: { $in: userEmails } });
  if (userPhones.length) userFilters.push({ phone: { $in: userPhones } });

  const existingUsers = userFilters.length
    ? await UserModel.find({ $or: userFilters })
    : [];
  const existingUsersByEmail = mapBy(existingUsers, (item) =>
    item.email ? String(item.email).toLowerCase() : null,
  );
  const existingUsersByPhone = mapBy(existingUsers, "phone");

  const newUsers = [];

  for (const seedUser of validUsers) {
    const keyEmail = seedUser.email
      ? String(seedUser.email).toLowerCase()
      : null;
    const keyPhone = seedUser.phone ? String(seedUser.phone) : null;
    const existing =
      (keyEmail && existingUsersByEmail.get(keyEmail)) ||
      (keyPhone && existingUsersByPhone.get(keyPhone));

    if (existing) {
      stats.users.existing += 1;
      if (!existing.profileId && seedUser.profileId) {
        const resolvedRoles = resolveSeedUserRoles(seedUser, existing);
        await UserModel.updateOne(
          { _id: existing._id },
          {
            $set: {
              profileId: seedUser.profileId,
              role: pickPrimaryRole(resolvedRoles),
              roles: resolvedRoles,
              emailVerifiedAt:
                seedUser.emailVerifiedAt ?? existing.emailVerifiedAt,
              phoneVerifiedAt:
                seedUser.phoneVerifiedAt ?? existing.phoneVerifiedAt,
            },
          },
        );
        stats.users.updated += 1;
      } else if (
        seedUser.profileId &&
        existing.profileId &&
        !existing.profileId.equals(seedUser.profileId)
      ) {
        stats.warnings.push(
          `User ${keyEmail ?? keyPhone} already linked to a different profile`,
        );
      }
      continue;
    }
    newUsers.push(seedUser);
  }

  if (newUsers.length) {
    const hashedUsers = await Promise.all(
      newUsers.map(async (user) => {
        const resolvedRoles = resolveSeedUserRoles(user);
        return {
          ...user,
          role: pickPrimaryRole(resolvedRoles),
          roles: resolvedRoles,
          password: await hashPasswordIfNeeded(user.password),
        };
      }),
    );
    await UserModel.insertMany(hashedUsers, { ordered: false });
    stats.users.inserted = newUsers.length;
  }

  const profileEmails = unique(collectValues(validProfiles, "email")).filter(
    Boolean,
  );
  const profilePhones = unique(collectValues(validProfiles, "phone")).filter(
    Boolean,
  );

  const profileFilters = [];
  if (profileEmails.length)
    profileFilters.push({ email: { $in: profileEmails } });
  if (profilePhones.length)
    profileFilters.push({ phone: { $in: profilePhones } });

  const existingProfiles = profileFilters.length
    ? await ProfileModel.find({ $or: profileFilters })
    : [];

  const existingProfilesByEmail = mapBy(existingProfiles, (item) =>
    item.email ? String(item.email).toLowerCase() : null,
  );
  const existingProfilesByPhone = mapBy(existingProfiles, "phone");
  const newProfiles = [];

  for (const seedProfile of validProfiles) {
    const keyEmail = seedProfile.email
      ? String(seedProfile.email).toLowerCase()
      : null;
    const keyPhone = seedProfile.phone ? String(seedProfile.phone) : null;
    const existing =
      (keyEmail && existingProfilesByEmail.get(keyEmail)) ||
      (keyPhone && existingProfilesByPhone.get(keyPhone));

    if (existing) {
      stats.profiles.existing += 1;
      continue;
    }
    newProfiles.push(seedProfile);
  }

  if (newProfiles.length) {
    await ProfileModel.insertMany(newProfiles, { ordered: false });
    stats.profiles.inserted = newProfiles.length;
  }

  const groupOps = validGroups.map((group) => ({
    updateOne: {
      filter: { groupNumber: group.groupNumber },
      update: { $set: group },
      upsert: true,
    },
  }));

  if (groupOps.length) {
    const groupResult = await GroupModel.bulkWrite(groupOps, {
      ordered: false,
    });
    stats.groups.upserted = groupResult.upsertedCount ?? 0;
    stats.groups.modified = groupResult.modifiedCount ?? 0;
  }

  const groupById = new Map(
    validGroups.map((group) => [String(group._id), group]),
  );

  const membershipPairs = validGroupMembers.map((membership) => ({
    userId: membership.userId,
    groupId: membership.groupId,
  }));

  const existingMemberships =
    membershipPairs.length > 0
      ? await GroupMembershipModel.find(
          { $or: membershipPairs },
          {
            userId: 1,
            groupId: 1,
            memberNumber: 1,
            memberSerial: 1,
            joinedAt: 1,
            createdAt: 1,
          },
        ).lean()
      : [];

  const existingMembershipByKey = new Map(
    existingMemberships.map((membership) => [
      `${membership.userId}-${membership.groupId}`,
      membership,
    ]),
  );

  const maxExistingMember = await GroupMembershipModel.findOne(
    { memberNumber: { $type: "number" } },
    { memberNumber: 1 },
  )
    .sort({ memberNumber: -1 })
    .lean();

  let nextGlobalNumber = Number(maxExistingMember?.memberNumber ?? 0) + 1;

  const normalizedGroupMembers = validGroupMembers.map((membership) => {
    const key = `${membership.userId}-${membership.groupId}`;
    const existing = existingMembershipByKey.get(key);
    const group = groupById.get(String(membership.groupId));

    let memberNumber = normalizeMemberNumber(existing?.memberNumber);
    if (!memberNumber) {
      memberNumber = nextGlobalNumber;
      nextGlobalNumber += 1;
    }

    const joinedAt =
      existing?.joinedAt ||
      membership.joinedAt ||
      membership.createdAt ||
      new Date().toISOString();

    const existingSerial =
      typeof existing?.memberSerial === "string" ? existing.memberSerial : null;
    const isNewFormat = (value) =>
      typeof value === "string" && value.startsWith("CRC/G");
    const computedSerial = group
      ? formatGroupMemberSerial({
          groupNumber: group.groupNumber,
          memberNumber,
        })
      : null;
    const hasExistingNumber = normalizeMemberNumber(existing?.memberNumber);
    const memberSerial = existing
      ? (isNewFormat(existingSerial) && hasExistingNumber
          ? existingSerial
          : computedSerial)
      : computedSerial;

    return {
      ...membership,
      memberNumber,
      joinedAt,
      memberSerial,
    };
  });

  const membershipOps = normalizedGroupMembers.map((membership) => {
    const insertDoc = stripFields(membership, ["updatedAt"]);
    return {
      updateOne: {
        filter: { userId: membership.userId, groupId: membership.groupId },
        update: { $setOnInsert: insertDoc },
        upsert: true,
      },
    };
  });

  if (membershipOps.length) {
    const membershipResult = await GroupMembershipModel.bulkWrite(
      membershipOps,
      {
        ordered: false,
      },
    );
    stats.groupMembers.upserted = membershipResult.upsertedCount ?? 0;
  }

  const membershipSerialOps = normalizedGroupMembers
    .filter((membership) => membership.memberSerial)
    .map((membership) => ({
      updateOne: {
        filter: {
          userId: membership.userId,
          groupId: membership.groupId,
          $or: [
            { memberSerial: { $exists: false } },
            { memberSerial: null },
            { memberSerial: "" },
            { memberSerial: { $not: /^CRC\/G/ } },
            { memberNumber: { $exists: false } },
            { memberNumber: null },
            { memberNumber: { $lte: 0 } },
          ],
        },
        update: {
          $set: {
            memberSerial: membership.memberSerial,
            memberNumber: membership.memberNumber,
            joinedAt: membership.joinedAt,
          },
        },
      },
    }));

  if (membershipSerialOps.length) {
    await GroupMembershipModel.bulkWrite(membershipSerialOps, {
      ordered: false,
    });
  }
  await ensureGroupMemberSequence();

  const settingsOps = validContributionSettings.map((setting) => ({
    updateOne: {
      filter: {
        userId: setting.userId,
        groupId: setting.groupId,
        year: setting.year,
        contributionType: setting.contributionType,
      },
      update: { $set: setting },
      upsert: true,
    },
  }));

  if (settingsOps.length) {
    const settingsResult = await ContributionSettingModel.bulkWrite(
      settingsOps,
      {
        ordered: false,
      },
    );
    stats.contributionSettings.upserted = settingsResult.upsertedCount ?? 0;
  }

  const contributionOps = validContributions.map((contribution) => ({
    updateOne: {
      filter: {
        userId: contribution.userId,
        groupId: contribution.groupId,
        month: contribution.month,
        year: contribution.year,
        contributionType: contribution.contributionType,
      },
      update: { $set: contribution },
      upsert: true,
    },
  }));

  if (contributionOps.length) {
    const contribResult = await ContributionModel.bulkWrite(contributionOps, {
      ordered: false,
    });
    stats.contributions.upserted = contribResult.upsertedCount ?? 0;
  }

  const transactionOps = validTransactions.map((transaction) => {
    const insertDoc = stripFields(transaction, ["updatedAt"]);
    return {
      updateOne: {
        filter: { reference: transaction.reference },
        update: { $setOnInsert: insertDoc },
        upsert: true,
      },
    };
  });

  if (transactionOps.length) {
    const txResult = await TransactionModel.bulkWrite(transactionOps, {
      ordered: false,
    });
    stats.transactions.upserted = txResult.upsertedCount ?? 0;
  }

  await mongoose.disconnect();

  return {
    ok: 1,
    inputDir,
    paths: seed.paths,
    stats,
  };
}
