import jwt from "jsonwebtoken";
import { Server as SocketIOServer } from "socket.io";

import { UserModel } from "./models/User.js";
import { GroupMembershipModel } from "./models/GroupMembership.js";

let io = null;

function getAccessSecret() {
  return process.env.JWT_ACCESS_SECRET || process.env.JWT_SECRET;
}

function resolveCorsOrigins() {
  return process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(",") : true;
}

function userRoom(profileId) {
  return `user:${profileId}`;
}

export function initSocket(server) {
  io = new SocketIOServer(server, {
    cors: {
      origin: resolveCorsOrigins(),
      credentials: true,
    },
  });

  io.use(async (socket, next) => {
    try {
      const authHeader =
        socket.handshake.auth?.token || socket.handshake.headers?.authorization;

      if (!authHeader) {
        return next(new Error("Missing auth token"));
      }

      const token = String(authHeader).startsWith("Bearer ")
        ? String(authHeader).slice("Bearer ".length)
        : String(authHeader);

      const secret = getAccessSecret();
      if (!secret) {
        return next(new Error("Server auth misconfiguration"));
      }

      const decoded = jwt.verify(token, secret);
      if (decoded?.type !== "access" || !decoded?.id) {
        return next(new Error("Invalid token"));
      }

      const user = await UserModel.findById(decoded.id).select("+active");
      if (!user || user.active === false) {
        return next(new Error("User not found"));
      }

      if (String(user.role || "") !== "admin") {
        const activeMembership = await GroupMembershipModel.exists({
          userId: user.profileId,
          status: "active",
        });

        if (!activeMembership) {
          return next(
            new Error(
              "User is not approved in any group. Please contact your coordinator.",
            ),
          );
        }
      }

      socket.data.userId = String(user._id);
      socket.data.profileId = user.profileId ? String(user.profileId) : null;
      return next();
    } catch (err) {
      return next(err instanceof Error ? err : new Error("Socket auth failed"));
    }
  });

  io.on("connection", (socket) => {
    const profileId = socket.data?.profileId;
    if (profileId) {
      socket.join(userRoom(profileId));
    }

    socket.on("disconnect", (reason) => {
      // eslint-disable-next-line no-console
      console.log("Socket disconnected", {
        userId: socket.data?.userId,
        profileId,
        reason,
      });
    });

    socket.on("error", (err) => {
      // eslint-disable-next-line no-console
      console.error("Socket error", err);
    });
  });

  io.engine.on("connection_error", (err) => {
    // eslint-disable-next-line no-console
    console.error("Socket connection error", err);
  });

  return io;
}

export function getIO() {
  return io;
}

export function emitToUser(profileId, event, payload) {
  if (!io || !profileId) return;
  io.to(userRoom(profileId)).emit(event, payload);
}
