import mongoose from "mongoose";
import dotenv from "dotenv";

// import { MemberModel } from "../src/models/Member.js";
import { GroupMembershipModel } from "./src/models/GroupMembership.js";
import { UserModel } from "./src/models/User.js";
import { GroupModel } from "./src/models/Group.js";
import { ProfileModel } from "./src/models/Profile.js";

dotenv.config();

async function run() {
  await mongoose.connect(process.env.MONGO_URI);

  // const result = await GroupModel.updateMany(
  //   {},
  //   {
  //     $set: {
  //       maxMembers: 200,
  //       // imageUrl:
  //       //   "https://res.cloudinary.com/ddfpckzbw/image/upload/v1774645938/CRCLogo_Updated_nflkgl.jpg",
  //     },
  //   },
  // );

  const users = await ProfileModel.find({
    fullName: { $regex: /\(N\)$/ },
  }).select("fullName _id");

  const userIds = users.map((user) => user._id);
  const result = await GroupMembershipModel.updateMany(
    { userId: { $in: userIds } },
    { $set: { joinedAt: new Date("2026-01-01T11:00:00Z") } },
  );

  console.log("Updated Memberships:", result.modifiedCount);

  // const result = await GroupMembershipModel.updateMany(
  //   { fullName: { $regex: /\(N\)$/ } },
  //   { $set: { joinedAt: new Date("2026-01-01T00:00:00Z") } },
  // );

  // console.log("Updated Memberships:", result.modifiedCount);

  process.exit(0);
}

run();
