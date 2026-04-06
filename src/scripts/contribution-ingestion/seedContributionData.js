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

const resolveInputDir = async (rootDir) => {
  const directUsers = path.join(rootDir, "users.json");
  if (await fileExists(directUsers)) return rootDir;

  const entries = await fs.readdir(rootDir, { withFileTypes: true });
  const dirs = entries.filter((entry) => entry.isDirectory());
  if (dirs.length === 1) {
    return path.join(rootDir, dirs[0].name);
  }
  if (dirs.length === 0) {
    throw new Error(`No seed files found in ${rootDir}`);
  }
  throw new Error(
    `Multiple seed directories found in ${rootDir}. Please pass --inputDir explicitly.`,
  );
};

const args = parseArgs(process.argv.slice(2));
const inputRoot = args.inputDir ?? process.env.SEED_INPUT_DIR ?? DEFAULT_INPUT_ROOT;
const inputDir = await resolveInputDir(inputRoot);

const usersPath = args.users ?? path.join(inputDir, "users.json");
const profilesPath = args.profiles ?? path.join(inputDir, "profiles.json");
const groupsPath = args.groups ?? path.join(inputDir, "groups.json");
const groupMembersPath = args.groupMembers ?? path.join(inputDir, "groupMembers.json");
const contributionsPath = args.contributions ?? path.join(inputDir, "contributions.json");
const transactionsPath = args.transactions ?? path.join(inputDir, "transactions.json");
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

// eslint-disable-next-line no-console
console.log(JSON.stringify(result, null, 2));
