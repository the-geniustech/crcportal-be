import mongoose from "mongoose";
import dotenv from "dotenv";

// import { MemberModel } from "../src/models/Member.js";
import { GroupMembershipModel } from "./src/models/GroupMembership.js";
import { UserModel } from "./src/models/User.js";

dotenv.config();

async function run() {
  await mongoose.connect(process.env.MONGO_URI);

  const result = await UserModel.updateMany(
    {},
    {
      $set: {
        emailVerifiedAt: new Date("2024-01-01T00:00:00.000Z"),
      },
    },
  );

  console.log("Updated members:", result.modifiedCount);

  process.exit(0);
}

run();
