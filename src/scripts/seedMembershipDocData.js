import dotenv from "dotenv";

dotenv.config();

import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import mongoose from "mongoose";
import { connectMongo } from "../db.js";
import { GroupModel } from "../models/Group.js";
import { GroupMembershipModel } from "../models/GroupMembership.js";
import { ProfileModel } from "../models/Profile.js";
import { UserModel } from "../models/User.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const parseArgs = (args) => {
  const output = {};
  for (let index = 0; index < args.length; index += 1) {
    const current = args[index];
    if (!current.startsWith("--")) continue;
    const key = current.replace(/^--/, "");
    const next = args[index + 1];
    if (!next || next.startsWith("--")) {
      output[key] = true;
    } else {
      output[key] = next;
      index += 1;
    }
  }
  return output;
};

const readJson = async (filePath) => {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
};

const args = parseArgs(process.argv.slice(2));
const inputDir = args.inputDir ?? path.resolve(__dirname, "../seed-data/membership-doc");
const groupsPath = args.groups ?? path.join(inputDir, "groups.seed.json");
const profilesPath = args.profiles ?? path.join(inputDir, "profiles.seed.json");
const usersPath = args.users ?? path.join(inputDir, "users.seed.json");
const membershipsPath =
  args.memberships ?? path.join(inputDir, "group-memberships.seed.json");
const isDryRun = Boolean(args["dry-run"]);
const shouldReset = Boolean(args.reset);
const shouldReindex = Boolean(args.reindex);

const mongoUri = process.env.MONGO_URI;
if (!mongoUri) {
  // eslint-disable-next-line no-console
  console.error("Missing MONGO_URI");
  process.exit(1);
}

const [groupsSeed, profilesSeed, usersSeed, membershipsSeed] = await Promise.all([
  readJson(groupsPath),
  readJson(profilesPath),
  readJson(usersPath),
  readJson(membershipsPath),
]);

const stats = {
  groups: { total: groupsSeed.length, upserted: 0 },
  profiles: { total: profilesSeed.length, created: 0, existing: 0, skipped: 0 },
  users: { total: usersSeed.length, created: 0, existing: 0, linked: 0, updatedRole: 0, skipped: 0 },
  memberships: { total: membershipsSeed.length, upserted: 0, skipped: 0 },
  reset: { enabled: shouldReset },
  reindexed: shouldReindex,
  warnings: [],
};

if (isDryRun) {
  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify(
      {
        ok: 1,
        dryRun: true,
        inputDir,
        groupsPath,
        profilesPath,
        usersPath,
        membershipsPath,
        stats,
      },
      null,
      2,
    ),
  );
  process.exit(0);
}

await connectMongo({ mongoUri });

if (shouldReindex) {
  await Promise.all([
    GroupModel.syncIndexes(),
    ProfileModel.syncIndexes(),
    UserModel.syncIndexes(),
    GroupMembershipModel.syncIndexes(),
  ]);
}

if (shouldReset) {
  await Promise.all([
    GroupMembershipModel.deleteMany({}),
    UserModel.deleteMany({}),
    ProfileModel.deleteMany({}),
    GroupModel.deleteMany({}),
  ]);
}

const groupIdBySeedKey = new Map();

for (const groupSeed of groupsSeed) {
  const {
    seedKey,
    coordinatorProfileSeedKey,
    ...payload
  } = groupSeed;
  if (!payload.groupNumber) continue;

  const group = await GroupModel.findOneAndUpdate(
    { groupNumber: payload.groupNumber },
    { $set: payload },
    { upsert: true, new: true, runValidators: true, setDefaultsOnInsert: true },
  );
  groupIdBySeedKey.set(seedKey, group._id);
  stats.groups.upserted += 1;
}

const profileIdBySeedKey = new Map();

for (const profileSeed of profilesSeed) {
  const { seedKey, email, phone, ...payload } = profileSeed;
  const filter = email ? { email } : phone ? { phone } : null;
  if (!filter) {
    stats.profiles.skipped += 1;
    continue;
  }

  const existing = await ProfileModel.findOne(filter);
  if (existing) {
    profileIdBySeedKey.set(seedKey, existing._id);
    stats.profiles.existing += 1;
    continue;
  }

  const created = await ProfileModel.create({ email, phone, ...payload });
  profileIdBySeedKey.set(seedKey, created._id);
  stats.profiles.created += 1;
}

for (const userSeed of usersSeed) {
  const { profileSeedKey, email, phone, password, role } = userSeed;
  const profileId = profileIdBySeedKey.get(profileSeedKey);
  const filter = email ? { email } : phone ? { phone } : null;
  if (!profileId || !filter) {
    stats.users.skipped += 1;
    if (!profileId) {
      stats.warnings.push(`Missing profile for seed key ${profileSeedKey ?? "unknown"}`);
    }
    continue;
  }

  const existing = await UserModel.findOne(filter);
  if (existing) {
    if (!existing.profileId) {
      existing.profileId = profileId;
      await existing.save();
      stats.users.linked += 1;
    }
    if (role && existing.role !== role) {
      existing.role = role;
      await existing.save();
      stats.users.updatedRole += 1;
    }
    stats.users.existing += 1;
    continue;
  }

  await UserModel.create({
    email,
    phone,
    password,
    role: role ?? "member",
    profileId,
  });
  stats.users.created += 1;
}

for (const membershipSeed of membershipsSeed) {
  const groupId = groupIdBySeedKey.get(membershipSeed.groupSeedKey);
  const profileId = profileIdBySeedKey.get(membershipSeed.profileSeedKey);
  if (!groupId || !profileId) {
    stats.memberships.skipped += 1;
    if (!groupId) {
      stats.warnings.push(
        `Missing group for seed key ${membershipSeed.groupSeedKey ?? "unknown"}`,
      );
    }
    if (!profileId) {
      stats.warnings.push(
        `Missing profile for seed key ${membershipSeed.profileSeedKey ?? "unknown"}`,
      );
    }
    continue;
  }

  await GroupMembershipModel.findOneAndUpdate(
    { groupId, userId: profileId },
    {
      groupId,
      userId: profileId,
      role: membershipSeed.role ?? "member",
      status: membershipSeed.status ?? "active",
      joinedAt: membershipSeed.joinedAt ? new Date(membershipSeed.joinedAt) : new Date(),
    },
    { upsert: true, new: true, runValidators: true, setDefaultsOnInsert: true },
  );
  stats.memberships.upserted += 1;
}

for (const groupSeed of groupsSeed) {
  const groupId = groupIdBySeedKey.get(groupSeed.seedKey);
  if (!groupId) continue;

  const coordinatorProfileId = groupSeed.coordinatorProfileSeedKey
    ? profileIdBySeedKey.get(groupSeed.coordinatorProfileSeedKey)
    : null;

  const activeCount = await GroupMembershipModel.countDocuments({
    groupId,
    status: "active",
  });

  await GroupModel.findByIdAndUpdate(groupId, {
    memberCount: activeCount,
    coordinatorId: coordinatorProfileId ?? null,
    coordinatorName: groupSeed.coordinatorName ?? null,
    coordinatorEmail: groupSeed.coordinatorEmail ?? null,
    coordinatorPhone: groupSeed.coordinatorPhone ?? null,
  });
}

// eslint-disable-next-line no-console
console.log(
  JSON.stringify(
    {
      ok: 1,
      inputDir,
      groupsPath,
      profilesPath,
      usersPath,
      membershipsPath,
      stats,
    },
    null,
    2,
  ),
);

await mongoose.disconnect();

