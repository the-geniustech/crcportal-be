import AppError from "../../utils/AppError.js";
import {
  isDeliveredSmsStatus,
  isTerminalUndeliveredSmsStatus,
  lookupPhoneDndStatus,
  sendSms,
  sendVoiceOtpCall,
  waitForSmsDeliveryStatus,
} from "./termiiClient.js";

function isVoiceOtpFallbackEnabled() {
  return String(process.env.TERMII_ENABLE_VOICE_OTP_FALLBACK || "true")
    .trim()
    .toLowerCase() !== "false";
}

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

async function lookupDndStatusForFallback(toPhone, purposeLabel) {
  try {
    return await lookupPhoneDndStatus(toPhone);
  } catch (error) {
    console.warn(
      `Unable to verify DND status before fallback ${purposeLabel}: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
    return null;
  }
}

async function verifyGenericFallbackDelivery(sendResult, purposeLabel) {
  const messageId = String(
    sendResult?.message_id || sendResult?.messageId || "",
  ).trim();
  if (!messageId) {
    console.warn(
      `Termii generic ${purposeLabel} did not return a message ID. Accepting provider success response without delivery polling.`,
    );
    return null;
  }

  console.warn(`Termii generic ${purposeLabel} accepted`, {
    messageId,
    channel: sendResult?.channel || "generic",
    providerMessage: sendResult?.message || null,
  });

  try {
    const historyEntry = await waitForSmsDeliveryStatus(messageId);
    if (!historyEntry) {
      console.warn(`Termii generic ${purposeLabel} delivery not yet confirmed`, {
        messageId,
      });
      return null;
    }

    const status = String(historyEntry.status || "").trim();
    if (isDeliveredSmsStatus(status)) {
      console.warn(`Termii generic ${purposeLabel} delivered`, {
        messageId,
        status,
      });
      return historyEntry;
    }

    if (isTerminalUndeliveredSmsStatus(status)) {
      throw new AppError(
        `Termii accepted the ${purposeLabel} request but reported the message as "${status}". SMS delivery was not completed.`,
        502,
      );
    }

    console.warn(`Termii generic ${purposeLabel} still pending delivery`, {
      messageId,
      status: status || "pending",
    });
    return historyEntry;
  } catch (error) {
    if (
      error instanceof AppError &&
      error.message.includes("SMS delivery was not completed")
    ) {
      throw error;
    }

    console.warn(
      `Unable to verify Termii SMS history after generic ${purposeLabel}: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
    return null;
  }
}

async function attemptVoiceFallback({ toPhone, otp, purposeLabel, reason }) {
  if (!isVoiceOtpFallbackEnabled()) {
    throw new AppError(
      `Voice OTP fallback is disabled, and ${purposeLabel} could not be completed over SMS. ${reason}`,
      502,
    );
  }

  console.warn(`Attempting Termii voice OTP fallback for ${purposeLabel}`, {
    reason,
    toPhone,
  });

  const voiceResult = await sendVoiceOtpCall({
    to: toPhone,
    code: otp,
  });

  console.warn(`Termii voice OTP fallback accepted for ${purposeLabel}`, {
    messageId:
      voiceResult?.message_id || voiceResult?.messageId || voiceResult?.pinId,
    providerMessage: voiceResult?.message || null,
  });

  return voiceResult;
}

export async function sendTermiiOtpWithFallback({
  toPhone,
  otp,
  message,
  purposeLabel,
}) {
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
      !shouldFallbackOtpChannel(error, preferredChannel) ||
      preferredChannel.toLowerCase() === "generic"
    ) {
      throw error;
    }

    const dndStatus = await lookupDndStatusForFallback(toPhone, purposeLabel);
    if (dndStatus?.dndActive) {
      return attemptVoiceFallback({
        toPhone,
        otp,
        purposeLabel,
        reason:
          "the recipient number is on DND and the Termii DND SMS route is inactive on this account",
      });
    }

    console.warn(
      `Falling back to Termii generic route for ${purposeLabel} because the dnd route is inactive on this account.`,
    );

    let genericResult;
    try {
      genericResult = await sendSms({
        to: toPhone,
        message,
        channel: "generic",
      });
    } catch (genericError) {
      return attemptVoiceFallback({
        toPhone,
        otp,
        purposeLabel,
        reason: `generic SMS fallback failed: ${
          genericError instanceof Error
            ? genericError.message
            : String(genericError)
        }`,
      });
    }

    try {
      await verifyGenericFallbackDelivery(genericResult, purposeLabel);
      return genericResult;
    } catch (genericDeliveryError) {
      return attemptVoiceFallback({
        toPhone,
        otp,
        purposeLabel,
        reason:
          genericDeliveryError instanceof Error
            ? genericDeliveryError.message
            : String(genericDeliveryError),
      });
    }
  }
}
