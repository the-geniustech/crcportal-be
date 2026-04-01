import mongoose from "mongoose";
import dotenv from "dotenv";

// import { MemberModel } from "../src/models/Member.js";
import { GroupMembershipModel } from "./src/models/GroupMembership.js";
import { UserModel } from "./src/models/User.js";
import { GroupModel } from "./src/models/Group.js";

dotenv.config();

async function run() {
  await mongoose.connect(process.env.MONGO_URI);

  const result = await GroupModel.updateMany(
    {},
    {
      $set: {
        imageUrl:
          "https://res.cloudinary.com/ddfpckzbw/image/upload/v1774645938/CRCLogo_Updated_nflkgl.jpg",
      },
    },
  );

  console.log("Updated groups:", result.modifiedCount);

  process.exit(0);
}

run();
