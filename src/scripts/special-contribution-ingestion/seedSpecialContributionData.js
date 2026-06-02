import fs from "fs/promises";
import path from "path";
import dotenv from "dotenv";
import { fileURLToPath } from "url";
import { parseArgs } from "../utils/userDataCleanup.js";
import { seedSpecialContributionData } from "./seeders/specialContributionSeeder.js";

const currentFilePath = fileURLToPath(import.meta.url);
const currentDir = path.dirname(currentFilePath);

dotenv.config({ path: path.resolve(currentDir, "../../../.env") });

const DEFAULT_INPUT_ROOT = path.resolve(
  currentDir,
  "../../seed-data/special-contributions",
);

const fileExists = async (candidate) => {
  try {
    const stat = await fs.stat(candidate);
    return stat.isFile();
  } catch {
    return false;
  }
};

const resolveInputDirs = async (rootDir, { allowMultiple } = {}) => {
  const directContributions = path.join(rootDir, "contributions.json");
  if (await fileExists(directContributions)) return [rootDir];

  const entries = await fs.readdir(rootDir, { withFileTypes: true });
  const dirs = entries.filter((entry) => entry.isDirectory());
  if (dirs.length === 0) {
    throw new Error(`No seed files found in ${rootDir}`);
  }

  const candidates = [];
  for (const dir of dirs) {
    const candidatePath = path.join(rootDir, dir.name);
    const contributionsPath = path.join(candidatePath, "contributions.json");
    if (await fileExists(contributionsPath)) {
      candidates.push(candidatePath);
    }
  }

  if (candidates.length === 0) {
    throw new Error(
      `No seed folders with contributions.json found in ${rootDir}`,
    );
  }

  if (!allowMultiple && candidates.length > 1) {
    throw new Error(
      `Multiple seed directories found in ${rootDir}. Please pass --inputDir explicitly.`,
    );
  }

  return candidates.sort((left, right) => left.localeCompare(right));
};

const args = parseArgs(process.argv.slice(2));
const inputRoot =
  args.inputDir ?? process.env.SPECIAL_CONTRIBUTION_SEED_INPUT_DIR ?? DEFAULT_INPUT_ROOT;

const explicitPathKeys = [
  "contributions",
  "transactions",
  "profileContributionSettings",
  "warnings",
  "meta",
];
const hasExplicitPaths = explicitPathKeys.some((key) => Boolean(args[key]));

const inputDirs = await resolveInputDirs(inputRoot, {
  allowMultiple: !hasExplicitPaths,
});

const results = [];
const errors = [];

for (const inputDir of inputDirs) {
  try {
    const contributionsPath =
      args.contributions ?? path.join(inputDir, "contributions.json");
    const transactionsPath =
      args.transactions ?? path.join(inputDir, "transactions.json");
    const profileContributionSettingsPath =
      args.profileContributionSettings ??
      path.join(inputDir, "profileContributionSettings.json");
    const warningsPath = args.warnings ?? path.join(inputDir, "warnings.json");
    const metaPath = args.meta ?? path.join(inputDir, "meta.json");

    const result = await seedSpecialContributionData({
      inputDir,
      contributionsPath,
      transactionsPath,
      profileContributionSettingsPath,
      warningsPath,
      metaPath,
      dryRun: Boolean(args["dry-run"]),
      useTransaction: !Boolean(args["no-transaction"]),
      failOnWarnings: Boolean(args["fail-on-warnings"]),
      forceSettingsYear: Boolean(args["force-settings-year"]),
    });

    results.push(result);
  } catch (error) {
    errors.push({
      inputDir,
      message: error?.message ?? String(error),
    });
  }
}

const hasResultFailures = results.some((result) => Number(result?.ok) !== 1);

// eslint-disable-next-line no-console
console.log(
  JSON.stringify(
    {
      ok: errors.length || hasResultFailures ? 0 : 1,
      inputRoot,
      inputDirs,
      results,
      errors,
    },
    null,
    2,
  ),
);
