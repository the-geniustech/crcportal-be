import mongoose from "mongoose";
import dotenv from "dotenv";

// import { MemberModel } from "../src/models/Member.js";
import { GroupMembershipModel } from "./src/models/GroupMembership.js";
import { UserModel } from "./src/models/User.js";
import { GroupModel } from "./src/models/Group.js";

dotenv.config();

async function run() {
  await mongoose.connect(process.env.MONGO_URI);

  const result = await ContributionModel.updateMany(
    {},
    {
      $set: {
        imageUrl:
          "https://res.cloudinary.com/ddfpckzbw/image/upload/v1774539938/CRCLogo_001_ag2vq3.jpg",
      },
    },
  );

  console.log("Updated groups:", result.modifiedCount);

  process.exit(0);
}

run();
