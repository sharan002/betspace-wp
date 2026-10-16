require('dotenv').config();
const OpenAI = require("openai");
const ACCESS_TOKEN = process.env.WHATSAPP_TOKEN;
const Client_2 = process.env.Client_2 || "1034770233048647";
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

let Knowledge_base = ``


async function typingindicator(to, messageId) {
  const baseUrl = `https://graph.facebook.com/v24.0/${Client_2}/messages`;

  try {

    // Mark message as Read
    const readBody = {
      messaging_product: "whatsapp",
      message_id: messageId,
      status: "read",
      typing_indicator: {
    type: "text"
  },
      to: to // REQUIRED
    };

    const readResponse = await fetch(baseUrl, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${ACCESS_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(readBody),
    });

    const readData = await readResponse.json();
    console.log("Message marked as read:", readData);

  } catch (error) {
    console.error("Error marking message as read:", error);
  }
}







async function sendMessageClient2(to, text) {
  if (!ACCESS_TOKEN) {
    console.error("Missing WHATSAPP_TOKEN for Client2 message send");
    return;
  }

  await fetch(`https://graph.facebook.com/v24.0/${Client_2}/messages`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${ACCESS_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      messaging_product: "whatsapp",
      to,
      type: "text",
      text: { body: text },
    }),
  });
}


async function demo_minmini2(body) {

  try {
    const entry = body.entry?.[0];
    const changes = entry?.changes?.[0];
    const message = changes?.value?.messages?.[0];


    if (!message) {
      console.log("No message found");
      return;
    }

    const from = message.from;
    if (!from) {
      console.log("No sender found");
      return;
    }

    const messageId = message.id;

    await typingindicator(from,messageId);

    let msgBody = "";
    if (message.type === "text" && message.text?.body) {
      msgBody = message.text.body.trim();
    }

    if (!msgBody) {
      console.log("Empty message body");
      return;
    }

    const messages = [
      {
        role: "system",
        content: `You are Ai Assistant, the friendly and persuasive AI assistant for Print On Go. use company details for answer \n ${Knowledge_base}`,
      },
      { role: "user", content: msgBody },
    ];

    const response = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages,
    });

    const reply = response?.choices?.[0]?.message?.content || "Sorry, I could not process that right now.";
    await sendMessageClient2(from, reply);

  } catch (err) {
    console.error("Webhook Error:", err);
  }
}

module.exports = {
  demo_minmini2,
  sendMessageClient2,
};


const companyUrls = [
"https://botkovai.in"
];


async function fetchCompanyInfo() {
  let combinedText = "";
  for (const url of companyUrls) {
    const text = await fetchTextFromUrl(url);
    if (text) {
      combinedText += `\n\n[Source: ${url}]\n${text}`;
    }
  }
  Knowledge_base = combinedText;

}


async function fetchTextFromUrl(url) {
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP error: ${res.status}`);

    const html = await res.text();

    // Extract body content
    const bodyMatch = html.match(/<body[^>]*>([\s\S]*?)<\/body>/i);
    const bodyHtml = bodyMatch ? bodyMatch[1] : html;

    // Remove scripts, styles, noscript
    const cleanedHtml = bodyHtml
      .replace(/<script[\s\S]*?<\/script>/gi, "")
      .replace(/<style[\s\S]*?<\/style>/gi, "")
      .replace(/<noscript[\s\S]*?<\/noscript>/gi, "");

    // Strip all HTML tags
    return cleanedHtml.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
  } catch (err) {
    console.error(`Error fetching ${url}:`, err);
    return "";
  }
}

setInterval(fetchCompanyInfo, 120 * 60 * 1000);

// --- Auto-run when server starts ---
fetchCompanyInfo(); // fetch once immediately 






