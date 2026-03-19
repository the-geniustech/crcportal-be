import dotenv from "dotenv";

dotenv.config();

import { connectMongo } from "../db.js";
import { SmsTemplateModel } from "../models/SmsTemplate.js";

const args = new Set(process.argv.slice(2));
const shouldReset = args.has("--reset");
const isDryRun = args.has("--dry-run");

const templates = [
  {
    key: "payment_reminder",
    name: "Payment Reminder",
    body: "Hi {name}, this is a reminder that your {payment_type} of NGN{amount} is due. Please make payment before the due date. Thank you.",
  },
  {
    key: "loan_approval",
    name: "Loan Approval",
    body: "Congratulations {name}! Your loan of NGN{amount} has been approved. Kindly check your dashboard for next steps.",
  },
  {
    key: "meeting_notification",
    name: "Meeting Notification",
    body: "Hi {name}, reminder: {group_name} meeting scheduled for {date}. Please be punctual. Thank you.",
  },
  {
    key: "contribution_confirmation",
    name: "Contribution Confirmation",
    body: "Hi {name}, your contribution of NGN{amount} to {group_name} has been received. Thank you for your commitment.",
  },
  {
    key: "withdrawal_approved",
    name: "Withdrawal Approved",
    body: "Hi {name}, your withdrawal request of NGN{amount} has been approved. Funds will be processed shortly.",
  },
];

const mongoUri = process.env.MONGO_URI;
if (!mongoUri) {
  // eslint-disable-next-line no-console
  console.error("Missing MONGO_URI");
  process.exit(1);
}

async function main() {
  await connectMongo();

  if (shouldReset && !isDryRun) {
    await SmsTemplateModel.deleteMany({});
  }

  if (isDryRun) {
    console.log(JSON.stringify(templates, null, 2));
    return;
  }

  const ops = templates.map((t) => ({
    updateOne: {
      filter: { key: t.key },
      update: { $set: { ...t, isActive: true } },
      upsert: true,
    },
  }));

  await SmsTemplateModel.bulkWrite(ops);
  console.log(`Seeded ${templates.length} SMS templates.`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
