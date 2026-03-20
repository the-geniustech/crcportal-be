import dotenv from "dotenv";

dotenv.config();

import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import mongoose from "mongoose";
import { connectMongo } from "../db.js";
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
const inputDir = args.inputDir ?? path.resolve(__dirname, "../seed-data");
const profilesPath = args.profiles ?? path.join(inputDir, "profiles.seed.json");
const usersPath = args.users ?? path.join(inputDir, "users.seed.json");
const isDryRun = Boolean(args["dry-run"]);
const shouldReset = Boolean(args.reset);
const shouldResetSeedOnly = Boolean(args["reset-seed"]);
const shouldReindex = Boolean(args.reindex);

const mongoUri = process.env.MONGO_URI;
if (!mongoUri) {
  // eslint-disable-next-line no-console
  console.error("Missing MONGO_URI");
  process.exit(1);
}

const [profilesSeed, usersSeed] = await Promise.all([
  readJson(profilesPath),
  readJson(usersPath),
]);

const profileIdBySeedKey = new Map();
const stats = {
  profiles: { total: profilesSeed.length, created: 0, existing: 0, skipped: 0 },
  users: {
    total: usersSeed.length,
    created: 0,
    existing: 0,
    linked: 0,
    skipped: 0,
  },
  reset: {
    mode: shouldReset ? "all" : shouldResetSeedOnly ? "seed" : "none",
    usersDeleted: 0,
    profilesDeleted: 0,
    deletedEmails: [],
    deletedPhones: [],
  },
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
        profilesPath,
        usersPath,
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
  await Promise.all([ProfileModel.syncIndexes(), UserModel.syncIndexes()]);
}

if (shouldReset) {
  const [userResult, profileResult] = await Promise.all([
    UserModel.deleteMany({}),
    ProfileModel.deleteMany({}),
  ]);
  stats.reset.usersDeleted = userResult.deletedCount ?? 0;
  stats.reset.profilesDeleted = profileResult.deletedCount ?? 0;
} else if (shouldResetSeedOnly) {
  const emailSet = new Set();
  const phoneSet = new Set();

  profilesSeed.forEach(({ email, phone }) => {
    if (email) emailSet.add(email);
    if (phone) phoneSet.add(phone);
  });

  usersSeed.forEach(({ email, phone }) => {
    if (email) emailSet.add(email);
    if (phone) phoneSet.add(phone);
  });

  const emails = Array.from(emailSet);
  const phones = Array.from(phoneSet);
  const clauses = [];
  if (emails.length) clauses.push({ email: { $in: emails } });
  if (phones.length) clauses.push({ phone: { $in: phones } });

  if (clauses.length === 0) {
    stats.warnings.push("Reset seed requested, but no email/phone values found.");
  } else {
    const filter = clauses.length === 1 ? clauses[0] : { $or: clauses };
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const resetLogPath = path.join(
      inputDir,
      `reset-seed.log.${timestamp}.json`,
    );
    const [userResult, profileResult] = await Promise.all([
      UserModel.deleteMany(filter),
      ProfileModel.deleteMany(filter),
    ]);
    stats.reset.usersDeleted = userResult.deletedCount ?? 0;
    stats.reset.profilesDeleted = profileResult.deletedCount ?? 0;
    stats.reset.deletedEmails = emails;
    stats.reset.deletedPhones = phones;
    await fs.writeFile(
      resetLogPath,
      JSON.stringify(
        {
          summary: {
            timestamp,
            usersDeleted: stats.reset.usersDeleted,
            profilesDeleted: stats.reset.profilesDeleted,
          },
          resetSeedTargets: { emails, phones },
        },
        null,
        2,
      ),
    );
  }
}

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
      stats.warnings.push(
        `Missing profile for seed key ${profileSeedKey ?? "unknown"}`,
      );
    }
    continue;
  }

  const existing = await UserModel.findOne(filter);
  if (existing) {
    if (!existing.profileId) {
      existing.profileId = profileId;
      await existing.save();
      stats.users.linked += 1;
    } else if (!existing.profileId.equals(profileId)) {
      stats.warnings.push(
        `User ${email ?? phone} already linked to a different profile`,
      );
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

// eslint-disable-next-line no-console
console.log(
  JSON.stringify(
    {
      ok: 1,
      inputDir,
      profilesPath,
      usersPath,
      stats,
    },
    null,
    2,
  ),
);

await mongoose.disconnect();
