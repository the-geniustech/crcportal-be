import AppError from "../../utils/AppError.js";
import {
  lookupPhoneDndStatus,
  lookupSmsHistory,
  sendSms,
} from "./termiiClient.js";

function shouldFallbackOtpChannel(error, attemptedChannel) {
  if (String(attemptedChannel || "").trim().toLowerCase() !== "dnd") {
    return false;
  }

  return (
    error &&
    typeof error === "object" &&
    "provider" in error &&
    error.provider === "termii" &&
    "providerCategory" in error &&
    error.providerCategory === "route_inactive"
  );
}

async function assertGenericFallbackCanReachPhoneNumber(toPhone) {
  try {
    const dndStatus = await lookupPhoneDndStatus(toPhone);
    if (dndStatus.dndActive) {
      throw new AppError(
        "This phone number has Do-Not-Disturb enabled and the Termii DND route is inactive on this account, so SMS OTP delivery cannot be completed until that route is activated.",
        502,
      );
    }
  } catch (error) {
    if (
      error instanceof AppError &&
      error.message.includes("Do-Not-Disturb enabled")
    ) {
      throw error;
    }

    console.warn(
      `Unable to verify DND status before generic password-reset OTP fallback: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

function isTerminalUndeliveredSmsStatus(status) {
  const normalized = String(status || "").trim().toLowerCase();
  return [
    "dnd active on phone number",
    "message failed",
    "failed",
    "rejected",
    "expired",
  ].includes(normalized);
}

async function assertGenericFallbackWasNotRejected(sendResult) {
  const messageId = String(sendResult?.message_id || sendResult?.messageId || "").trim();
  if (!messageId) return;

  try {
    const historyEntry = await lookupSmsHistory(messageId);
    if (!historyEntry) return;

    const status = String(historyEntry.status || "").trim();
    if (isTerminalUndeliveredSmsStatus(status)) {
      throw new AppError(
        `Termii accepted the SMS request but reported the OTP message as \"${status}\". SMS delivery was not completed.`,
        502,
      );
    }
  } catch (error) {
    if (
      error instanceof AppError &&
      error.message.includes("SMS delivery was not completed")
    ) {
      throw error;
    }

    console.warn(
      `Unable to verify Termii SMS history after generic password-reset OTP fallback: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

export async function sendPasswordResetOtp({ toPhone, otp, ttlMinutes }) {
  const message = `Your password reset code is ${otp}. It expires in ${ttlMinutes} minutes.`;
  const preferredChannel = String(
    process.env.TERMII_OTP_CHANNEL || "dnd",
  ).trim();

  try {
    return await sendSms({
      to: toPhone,
      message,
      channel: preferredChannel,
    });
  } catch (error) {
    if (
      shouldFallbackOtpChannel(error, preferredChannel) &&
      preferredChannel.toLowerCase() !== "generic"
    ) {
      console.warn(
        "Falling back to Termii generic route for password-reset OTP delivery because the dnd route is inactive on this account.",
      );
      await assertGenericFallbackCanReachPhoneNumber(toPhone);
      const fallbackResult = await sendSms({
        to: toPhone,
        message,
        channel: "generic",
      });
      await assertGenericFallbackWasNotRejected(fallbackResult);
      return fallbackResult;
    }

    throw error;
  }
}
