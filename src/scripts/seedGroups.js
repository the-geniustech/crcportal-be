import dotenv from "dotenv";

dotenv.config();

import mongoose from "mongoose";
import { connectMongo } from "../db.js";
import { GroupModel } from "../models/Group.js";

const args = new Set(process.argv.slice(2));
const shouldReset = args.has("--reset");
const isDryRun = args.has("--dry-run");

const seedGroups = [
  {
    groupNumber: 1,
    groupName: "Lagos Professionals Circle",
    description:
      "A Contributions savings group for professionals in Lagos. We meet monthly to discuss financial goals and support each other in building wealth through collective savings.",
    location: "Lagos",
    category: "Professionals",
    meetingFrequency: "monthly",
    meetingDay: "Jan 11",
    imageUrl:
      "https://d64gsuwffb70l.cloudfront.net/694d1e2d65df4113e9e6f7e1_1766668648245_79b3ca67.png",
    isOpen: true,
    monthlyContribution: 25000,
    maxMembers: 200,
    totalSavings: 45000000,
    memberCount: 156,
    rules:
      "1. Monthly contributions must be made by the 5th of each month.\n2. Members must attend at least 75% of meetings.\n3. Loan requests require 6 months of active membership.\n4. Respect all members and maintain confidentiality.",
    status: "active",
  },
  {
    groupNumber: 2,
    groupName: "Tech Entrepreneurs Hub",
    description:
      "For tech founders and entrepreneurs looking to save together and access low-interest loans for their startups. Network with like-minded innovators.",
    location: "Abuja",
    category: "Entrepreneurs",
    meetingFrequency: "monthly",
    meetingDay: "Jan 8",
    imageUrl:
      "https://d64gsuwffb70l.cloudfront.net/694d1e2d65df4113e9e6f7e1_1766668648245_79b3ca67.png",
    isOpen: true,
    monthlyContribution: 50000,
    maxMembers: 100,
    totalSavings: 32000000,
    memberCount: 89,
    rules:
      "1. Contributions are due weekly.\n2. Business proposals for loans must be submitted in advance.\n3. Active participation in mentorship programs is encouraged.",
    status: "active",
  },
  {
    groupNumber: 3,
    groupName: "Women Empowerment Savings",
    description:
      "Empowering women through collective savings and financial literacy. Join us to build a secure financial future and support fellow women entrepreneurs.",
    location: "Port Harcourt",
    category: "Women",
    meetingFrequency: "monthly",
    meetingDay: "Jan 15",
    imageUrl:
      "https://d64gsuwffb70l.cloudfront.net/694d1e2d65df4113e9e6f7e1_1766668648245_79b3ca67.png",
    isOpen: true,
    monthlyContribution: 15000,
    maxMembers: 300,
    totalSavings: 28000000,
    memberCount: 234,
    rules:
      "1. Women-only membership.\n2. Monthly financial literacy workshops are mandatory.\n3. Support and encourage fellow members.",
    status: "active",
  },
  {
    groupNumber: 4,
    groupName: "Youth Future Builders",
    description:
      "Young professionals aged 18-35 saving for their future. Learn financial discipline and build wealth early in your career.",
    location: "Kano",
    category: "Youth",
    meetingFrequency: "monthly",
    meetingDay: "Jan 12",
    imageUrl:
      "https://d64gsuwffb70l.cloudfront.net/694d1e2d65df4113e9e6f7e1_1766668648245_79b3ca67.png",
    isOpen: true,
    monthlyContribution: 10000,
    maxMembers: 200,
    totalSavings: 15000000,
    memberCount: 178,
    status: "active",
  },
  {
    groupNumber: 5,
    groupName: "Market Traders Association",
    description:
      "For market traders and small business owners. Pool resources together and access emergency funds when needed.",
    location: "Ibadan",
    category: "Trade",
    meetingFrequency: "monthly",
    meetingDay: "Jan 7",
    imageUrl:
      "https://d64gsuwffb70l.cloudfront.net/694d1e2d65df4113e9e6f7e1_1766668648245_79b3ca67.png",
    isOpen: false,
    monthlyContribution: 20000,
    maxMembers: 350,
    totalSavings: 52000000,
    memberCount: 312,
    status: "active",
  },
  {
    groupNumber: 6,
    groupName: "Faith Community Savings",
    description:
      "A faith-based Contributions for members of our religious community. Save together, pray together, grow together.",
    location: "Enugu",
    category: "Religious",
    meetingFrequency: "monthly",
    meetingDay: "Jan 14",
    imageUrl:
      "https://d64gsuwffb70l.cloudfront.net/694d1e2d65df4113e9e6f7e1_1766668648245_79b3ca67.png",
    isOpen: true,
    monthlyContribution: 12000,
    maxMembers: 200,
    totalSavings: 18000000,
    memberCount: 145,
    status: "active",
  },
  {
    groupNumber: 7,
    groupName: "Neighborhood Unity Circle",
    description:
      "Bringing neighbors together through collective savings. Build community bonds while securing your financial future.",
    location: "Lagos",
    category: "Community",
    meetingFrequency: "monthly",
    meetingDay: "Jan 10",
    imageUrl:
      "https://d64gsuwffb70l.cloudfront.net/694d1e2d65df4113e9e6f7e1_1766668648245_79b3ca67.png",
    isOpen: true,
    monthlyContribution: 8000,
    maxMembers: 100,
    totalSavings: 5500000,
    memberCount: 67,
    status: "active",
  },
  {
    groupNumber: 8,
    groupName: "Healthcare Workers Contributions",
    description:
      "Doctors, nurses, and healthcare professionals saving together. Special loan programs for medical emergencies and professional development.",
    location: "Kaduna",
    category: "Professionals",
    meetingFrequency: "monthly",
    meetingDay: "Jan 9",
    imageUrl:
      "https://d64gsuwffb70l.cloudfront.net/694d1e2d65df4113e9e6f7e1_1766668648245_79b3ca67.png",
    isOpen: true,
    monthlyContribution: 35000,
    maxMembers: 150,
    totalSavings: 28000000,
    memberCount: 98,
    status: "active",
  },
];

if (isDryRun) {
  // eslint-disable-next-line no-console
  console.log(JSON.stringify(seedGroups, null, 2));
  process.exit(0);
}

const mongoUri = process.env.MONGO_URI;
if (!mongoUri) {
  // eslint-disable-next-line no-console
  console.error("Missing MONGO_URI");
  process.exit(1);
}

await connectMongo({ mongoUri });

if (shouldReset) {
  await GroupModel.deleteMany({});
}

const ops = seedGroups.map((g) => ({
  updateOne: {
    filter: { groupNumber: g.groupNumber },
    update: { $set: g },
    upsert: true,
  },
}));

const result = await GroupModel.bulkWrite(ops, { ordered: false });

// eslint-disable-next-line no-console
console.log(
  JSON.stringify(
    {
      ok: 1,
      reset: shouldReset,
      upserted: result.upsertedCount ?? 0,
      modified: result.modifiedCount ?? 0,
      matched: result.matchedCount ?? 0,
    },
    null,
    2,
  ),
);

await mongoose.disconnect();
