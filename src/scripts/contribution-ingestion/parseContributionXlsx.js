import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import { parseContributionWorkbook } from "./parser/xlsxParser.js";
import { transformContributionSheet } from "./transformers/transformContributionSheet.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DEFAULT_INPUT =
  process.env.SEED_INPUT_PATH ??
  path.resolve(
    __dirname,
    "../Group_21_Prevailing_Champions.xlsx",
  );

const DEFAULT_OUTPUT_ROOT = path.resolve(
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

const slugify = (value) =>
  String(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 60);

const args = parseArgs(process.argv.slice(2));
const inputPath = args.input ?? DEFAULT_INPUT;
const sheetName = args.sheet ?? null;
const headerRowIndex = args.headerRow ? Number(args.headerRow) - 1 : null;
const outputDirArg = args.output ?? null;
const isDryRun = Boolean(args["dry-run"]);

const parsed = await parseContributionWorkbook({
  inputPath,
  sheetName,
  headerRowIndex,
});

const transformed = transformContributionSheet(parsed, {
  year: args.year ? Number(args.year) : undefined,
  groupName: args.groupName,
  groupNumber: args.groupNumber ? Number(args.groupNumber) : undefined,
  defaultPassword: args.password ?? process.env.SEED_DEFAULT_PASSWORD,
  defaultUnits: args.defaultUnits ? Number(args.defaultUnits) : undefined,
  defaultMonthlyContribution: args.monthlyContribution
    ? Number(args.monthlyContribution)
    : undefined,
  contributionType: args.contributionType,
  cycleDuration: args.cycleDuration ? Number(args.cycleDuration) : undefined,
  paymentDay: args.paymentDay ? Number(args.paymentDay) : undefined,
  joinedAt: args.joinedAt,
  membershipStatus: args.membershipStatus,
  groupStatus: args.groupStatus,
  verifiedAt: args.verifiedAt,
});

const groupSlug = slugify(transformed.meta.groupName || "group");
const outputDir =
  outputDirArg ?? path.join(DEFAULT_OUTPUT_ROOT, groupSlug || "group");

if (isDryRun) {
  // eslint-disable-next-line no-console
  console.log(JSON.stringify({ ok: 1, outputDir, ...transformed }, null, 2));
  process.exit(0);
}

await fs.mkdir(outputDir, { recursive: true });

const usersPath = path.join(outputDir, "users.json");
const profilesPath = path.join(outputDir, "profiles.json");
const groupsPath = path.join(outputDir, "groups.json");
const groupMembersPath = path.join(outputDir, "groupMembers.json");
const contributionsPath = path.join(outputDir, "contributions.json");
const transactionsPath = path.join(outputDir, "transactions.json");
const contributionSettingsPath = path.join(
  outputDir,
  "contributionSettings.json",
);
const metaPath = path.join(outputDir, "meta.json");

await Promise.all([
  fs.writeFile(usersPath, JSON.stringify(transformed.users, null, 2)),
  fs.writeFile(profilesPath, JSON.stringify(transformed.profiles, null, 2)),
  fs.writeFile(groupsPath, JSON.stringify(transformed.groups, null, 2)),
  fs.writeFile(
    groupMembersPath,
    JSON.stringify(transformed.groupMembers, null, 2),
  ),
  fs.writeFile(
    contributionsPath,
    JSON.stringify(transformed.contributions, null, 2),
  ),
  fs.writeFile(
    transactionsPath,
    JSON.stringify(transformed.transactions ?? [], null, 2),
  ),
  fs.writeFile(
    contributionSettingsPath,
    JSON.stringify(transformed.contributionSettings, null, 2),
  ),
  fs.writeFile(metaPath, JSON.stringify(transformed.meta, null, 2)),
]);

// eslint-disable-next-line no-console
console.log(
  JSON.stringify(
    {
      ok: 1,
      outputDir,
      usersPath,
      profilesPath,
      groupsPath,
      groupMembersPath,
      contributionsPath,
      transactionsPath,
      contributionSettingsPath,
      metaPath,
      summary: {
        users: transformed.users.length,
        profiles: transformed.profiles.length,
        groups: transformed.groups.length,
        groupMembers: transformed.groupMembers.length,
        contributions: transformed.contributions.length,
        transactions: transformed.transactions?.length ?? 0,
        contributionSettings: transformed.contributionSettings.length,
        warnings: transformed.meta.warnings,
      },
    },
    null,
    2,
  ),
);
