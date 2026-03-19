export async function sendSms({ to, message }) {
  const apiKey = process.env.TERMII_API_KEY;
  const senderId = process.env.TERMII_SENDER_ID;
  const baseUrl = process.env.TERMII_BASE_URL || "https://api.ng.termii.com";
  const path = process.env.TERMII_SEND_PATH || "/api/sms/send";

  to = "+234" + to.slice(-10);

  if (!apiKey) throw new Error("Missing TERMII_API_KEY");
  if (!senderId) throw new Error("Missing TERMII_SENDER_ID");
  console.log(`Termii: {to: ${to}}`);

  const res = await fetch(`${baseUrl}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "User-Agent": process.env.APP_NAME || "crc-backend",
    },
    body: JSON.stringify({
      api_key: apiKey,
      to,
      from: senderId,
      sms: message,
      type: "plain",
      channel: process.env.TERMII_CHANNEL || "generic",
    }),
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Termii error (${res.status}): ${body}`);
  }

  return res.json().catch(() => ({}));
}
