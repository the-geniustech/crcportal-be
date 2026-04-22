import AppError from "../../utils/AppError.js";
import {
  isDeliveredSmsStatus,
  isTerminalUndeliveredSmsStatus,
  lookupPhoneDndStatus,
  sendSms,
  waitForSmsDeliveryStatus,
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
      `Unable to verify DND status before generic OTP fallback: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

async function verifyGenericFallbackDelivery(sendResult) {
  const messageId = String(sendResult?.message_id || sendResult?.messageId || "").trim();
  if (!messageId) {
    throw new AppError(
      "Termii accepted the fallback SMS request but did not return a message ID, so delivery could not be confirmed.",
      502,
    );
  }

  console.warn("Termii generic OTP fallback accepted", {
    messageId,
    channel: sendResult?.channel || "generic",
    providerMessage: sendResult?.message || null,
  });

  try {
    const historyEntry = await waitForSmsDeliveryStatus(messageId);
    if (!historyEntry) {
      throw new AppError(
        "Termii accepted the fallback SMS request, but delivery could not be confirmed yet. Because the DND route is inactive on this account, OTP SMS cannot be treated as delivered until Termii activates the route.",
        502,
      );
    }

    const status = String(historyEntry.status || "").trim();
    if (isDeliveredSmsStatus(status)) {
      console.warn("Termii generic OTP fallback delivered", {
        messageId,
        status,
      });
      return historyEntry;
    }

    if (isTerminalUndeliveredSmsStatus(status)) {
      throw new AppError(
        `Termii accepted the SMS request but reported the OTP message as \"${status}\". SMS delivery was not completed.`,
        502,
      );
    }

    throw new AppError(
      `Termii accepted the fallback SMS request, but delivery is still in status \"${status || "pending"}\". Because the DND route is inactive on this account, OTP SMS cannot be treated as delivered yet.`,
      502,
    );
  } catch (error) {
    if (
      error instanceof AppError &&
      (error.message.includes("SMS delivery was not completed") ||
        error.message.includes("delivery could not be confirmed") ||
        error.message.includes("cannot be treated as delivered") ||
        error.message.includes("did not return a message ID"))
    ) {
      throw error;
    }

    console.warn(
      `Unable to verify Termii SMS history after generic OTP fallback: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

export async function sendPhoneOtp({ toPhone, otp, ttlMinutes, purpose }) {
  const safePurpose = String(purpose || "").trim();
  const subject = safePurpose
    ? `Your ${safePurpose} code`
    : "Your verification code";
  const message = `${subject} is ${otp}. It expires in ${ttlMinutes} minutes.`;
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
        "Falling back to Termii generic route for OTP delivery because the dnd route is inactive on this account.",
      );
      await assertGenericFallbackCanReachPhoneNumber(toPhone);
      const fallbackResult = await sendSms({
        to: toPhone,
        message,
        channel: "generic",
      });
      await verifyGenericFallbackDelivery(fallbackResult);
      return fallbackResult;
    }

    throw error;
  }
}
