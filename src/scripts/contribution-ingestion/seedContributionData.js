import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import { seedContributionData } from "./seeders/contributionSeeder.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DEFAULT_INPUT_ROOT = path.resolve(
  __dirname,
  "../../seed-data/contributions",
);

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

const fileExists = async (candidate) => {
  try {
    const stat = await fs.stat(candidate);
    return stat.isFile();
  } catch {
    return false;
  }
};

const resolveInputDirs = async (rootDir, { allowMultiple } = {}) => {
  const directUsers = path.join(rootDir, "users.json");
  if (await fileExists(directUsers)) return [rootDir];

  const entries = await fs.readdir(rootDir, { withFileTypes: true });
  const dirs = entries.filter((entry) => entry.isDirectory());
  if (dirs.length === 0) {
    throw new Error(`No seed files found in ${rootDir}`);
  }

  const candidates = [];
  for (const dir of dirs) {
    const candidatePath = path.join(rootDir, dir.name);
    const usersPath = path.join(candidatePath, "users.json");
    if (await fileExists(usersPath)) {
      candidates.push(candidatePath);
    }
  }

  if (candidates.length === 0) {
    throw new Error(`No seed folders with users.json found in ${rootDir}`);
  }

  if (!allowMultiple && candidates.length > 1) {
    throw new Error(
      `Multiple seed directories found in ${rootDir}. Please pass --inputDir explicitly.`,
    );
  }

  return candidates.sort((a, b) => a.localeCompare(b));
};

const args = parseArgs(process.argv.slice(2));
const inputRoot =
  args.inputDir ?? process.env.SEED_INPUT_DIR ?? DEFAULT_INPUT_ROOT;

const explicitPathKeys = [
  "users",
  "profiles",
  "groups",
  "groupMembers",
  "contributions",
  "transactions",
  "contributionSettings",
];
const hasExplicitPaths = explicitPathKeys.some((key) => Boolean(args[key]));

const inputDirs = await resolveInputDirs(inputRoot, {
  allowMultiple: !hasExplicitPaths,
});

const results = [];
const errors = [];

for (const inputDir of inputDirs) {
  try {
    const usersPath = args.users ?? path.join(inputDir, "users.json");
    const profilesPath = args.profiles ?? path.join(inputDir, "profiles.json");
    const groupsPath = args.groups ?? path.join(inputDir, "groups.json");
    const groupMembersPath =
      args.groupMembers ?? path.join(inputDir, "groupMembers.json");
    const contributionsPath =
      args.contributions ?? path.join(inputDir, "contributions.json");
    const transactionsPath =
      args.transactions ?? path.join(inputDir, "transactions.json");
    const contributionSettingsPath =
      args.contributionSettings ?? path.join(inputDir, "contributionSettings.json");

    const result = await seedContributionData({
      inputDir,
      usersPath,
      profilesPath,
      groupsPath,
      groupMembersPath,
      contributionsPath,
      transactionsPath,
      contributionSettingsPath,
      dryRun: Boolean(args["dry-run"]),
      reset: Boolean(args.reset),
      reindex: Boolean(args.reindex),
    });
    results.push(result);
  } catch (error) {
    errors.push({
      inputDir,
      message: error?.message ?? String(error),
    });
  }
}

// eslint-disable-next-line no-console
console.log(
  JSON.stringify(
    {
      ok: errors.length ? 0 : 1,
      inputRoot,
      inputDirs,
      results,
      errors,
    },
    null,
    2,
  ),
);
