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
        maxMembers: 200,
        // imageUrl:
        //   "https://res.cloudinary.com/ddfpckzbw/image/upload/v1774645938/CRCLogo_Updated_nflkgl.jpg",
      },
    },
  );

  console.log("Updated groups:", result.modifiedCount);

  process.exit(0);
}

run();

/*
Me and the agency manager have been dragging my fixed pay throughout the project from foundation to delivery now, he saying that they only little payment on the project considering that they are Nigerian agency and that some how also affect their charges on fiverr. So I reduced the to as low as $3k and He's still comfortable with that yet.
*/
