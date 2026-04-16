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
*Weekly Deliverables*

*1. Design Output (Core Production)*

* *5–8 completed designs weekly*, including:

  * Training flyers
  * Event and promotional graphics
  * Social media creatives (posts, carousels, announcements)

*2. Conversion-Focused Marketing Assets*

* *2–3 campaign-driven designs weekly*, focused on:

  * Sales promotions
  * Lead generation creatives
  * Event signups / awareness campaigns
* Each design must have a *clear objective (engagement, clicks, signups, or awareness)*

*3. Content Strategy Collaboration*

* Participate in *1 weekly content planning session*
* Contribute *at least 2–3 creative concepts weekly* for campaigns or brand storytelling
* Work with the team to translate ideas into *visual execution strategies*

*4. Brand Consistency & Quality Control*

* Ensure all designs follow *brand identity guidelines (tone, color, typography, messaging)*
* Maintain *consistent visual quality across all outputs*
* Ensure designs are *platform-optimized (Instagram, WhatsApp, web, etc.)*

*5. Delivery, Responsiveness & Accountability*

* Deliver *100% of assigned tasks within agreed timelines*
* Provide *daily/ongoing progress updates when working on tasks*
* Communicate delays early with clear reasons and revised timelines

*6. Revisions & Feedback Loop*

* Handle feedback and revisions within *24–48 hours*
* Ensure final outputs reflect *team approval before publication*

*7. Output Reporting (NEW – important upgrade)*

* Submit a *weekly summary report*, including:

  * Tasks completed
  * Number of designs delivered
  * Campaigns supported
  * Key contributions or ideas
  * Challenges (if any)

*/
