require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const bodyParser = require('body-parser');
const app = express();
const multer = require('multer');
const { User, Chat, TempLead, ManagerCheckIn } = require("./Schema");
const { demo_minmini } = require("./Client1");
const {demo_minmini2} = require("./Client2")
const { sendDailyTrainingToAllUsers, handleTrainingReply, triggerSalesTrainingNow } = require("./SalesTraining");
const cors = require("cors");
const axios = require('axios');
const cron = require("node-cron");
const { WebSocketServer } = require("ws");
const http  = require("http");
const bcrypt = require("bcryptjs");         // For password hashing
const jwt = require("jsonwebtoken");        // For creating and verifying JWT tokens
const cookieParser = require("cookie-parser"); // For parsing cookies
const crypto = require('crypto');
app.use(bodyParser.json());
app.use(express.json());
app.use(cookieParser()); 
const { Configuration, OpenAIApi } = require("openai");
const OpenAI = require("openai");
const { pipeline } = require('stream');

const dns  = require("dns");

dns.setServers(["1.1.1.1", "8.8.8.8"]);



const corsOptions = {
  origin: ['http://localhost:4200',"https://betaspace.in",'http://localhost:3000', 'https://shadowcarsfe.vercel.app','https://sharan002.github.io','http://127.0.0.1:5501','http://localhost:3001','http://127.0.0.1:5500','https://wpchat.betaspace.in','http://wpchat.betaspace.in','https://betaspace.in','http://localhost:5173','http://localhost:5174','https://minicrm.betaspace.in'],
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization','ngrok-skip-browser-warning'],
  credentials: true
};

app.use(cors(corsOptions));
app.options('*', cors(corsOptions));


const OPENAI_API_KEY = process.env.OPENAI_API_KEY
const openai = new OpenAI({
apiKey : process.env.OPENAI_API_KEY,});

const server = http.createServer(app);

const wss = new WebSocketServer({ server, path: "/ws" });
const clients = new Set();

// app.use(cors());

// It’s the same as writing:

// app.use(cors({ origin: '*' }));


const PORT = process.env.PORT || 8001;

mongoose.set('strictQuery',false)


mongoose.connect(process.env.MONGO_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true
})
.then(() => console.log('✅ Connected to MongoDB',))
.catch((err) => console.error('❌ MongoDB connection error:', err));




const VERIFY_TOKEN = "sharan"; // any random string
const ACCESS_TOKEN = process.env.WHATSAPP_TOKEN; 
const PHONE_NUMBER_ID = process.env.PHONE_NUMBER_ID || "1344598478740761";
const PAGE_ACCESS_TOKEN = process.env.PAGE_ACCESS_TOKEN


// Mongoose schema & model
const chunkSchema = new mongoose.Schema({
  chunkId: String,         // NEW - stable ID for a chunk
  text: String,
  embedding: [Number],
  hash: String,            // NEW
  updatedAt: Date
});

const Chunk = mongoose.model("Chunk", chunkSchema, "chunks");


async function embedText(text) {
  const res = await axios.post(
    "https://api.openai.com/v1/embeddings",
    {
      input: text,
      model: "text-embedding-3-small" // NEW MODEL
    },
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      }
    }
  );

  // API always returns array: data[0].embedding
  return res.data.data[0].embedding;
}



function hashText(str) {
  return crypto.createHash("sha256").update(str).digest("hex");
}


async function ingestKnowledge(content) {
  // Basic validation
  if (!content) {
    return;
  }

  // --- CHUNKING CONFIG (same behavior) --- //
  const APPROX_TOKENS = 500;
  const CHARS_PER_TOKEN = 4;
  const maxChars = APPROX_TOKENS * CHARS_PER_TOKEN;

  const overlapPercent = 0.15; // 15%
  const overlapChars = Math.floor(maxChars * overlapPercent);

  // --- BASE SPLIT (same regex behavior) --- //
  const base = content.match(new RegExp(`.{1,${maxChars}}`, "g")) || [];

  // --- BUILD FINAL CHUNKS WITH OVERLAP (same logic) --- //
  const chunks = [];
  for (let i = 0; i < base.length; i++) {
    let chunk = base[i];

    if (i > 0 && overlapChars > 0) {
      const tail = base[i - 1].slice(-overlapChars);
      chunk = tail + "\n" + chunk;
    }

    chunks.push(chunk);
  }

  // --- COUNTERS --- //
  let inserted = 0;
  let updated = 0;
  let skipped = 0;

  // Edge case: no chunks
  if (chunks.length === 0) {
    return {
      success: true,
      message: "Incremental update completed",
      inserted,
      updated,
      skipped
    };
  }

  // --- FETCH EXISTING CHUNKS ONCE --- //
  const chunkIds = chunks.map((_, i) => `chunk_${i}`);

  const existingDocs = await Chunk.find({
    chunkId: { $in: chunkIds }
  }).lean();

  const existingById = new Map(existingDocs.map(doc => [doc.chunkId, doc]));

  // --- PROCESS LOOP (same logic exactly) --- //
  for (let i = 0; i < chunks.length; i++) {
    const rawChunk = chunks[i];
    const normalized = rawChunk.replace(/\s+/g, " ").trim();
    const chunkId = `chunk_${i}`;
    const hash = hashText(normalized);

    const existing = existingById.get(chunkId);

    if (existing && existing.hash === hash) {
      skipped++;
      continue;
    }

    const embedding = await embedText(normalized);

    if (existing) {
      await Chunk.updateOne(
        { chunkId },
        {
          text: normalized,
          embedding,
          hash,
          updatedAt: new Date()
        }
      );
      updated++;
    } else {
      await Chunk.create({
        chunkId,
        text: normalized,
        embedding,
        hash,
        updatedAt: new Date()
      });
      inserted++;
    }
  }

  return {
    success: true,
    message: "Incremental update completed",
    inserted,
    updated,
    skipped
  };
}



// =====================================
// VECTOR SEARCH
// =====================================
// Takes user query → embeds → performs $vectorSearch
//
async function searchRelevantChunks(query, topK = 5) {
  const queryVec = await embedText(query);

  const results = await Chunk.aggregate([
    {
      $vectorSearch: {
        index: "vector_index",
        path: "embedding",
        queryVector: queryVec,
        numCandidates: 200,
        limit: topK
      }
    }
  ]).exec();

  return results.map(r => r.text);
}

async function gatherCourseRagContext(userQuery, course, topK = 4) {
  const courseName = (course || "").trim();
  const scoped = courseName ? `${courseName} - ` : "";

  const retrievalQueries = [
    userQuery,
    `${scoped}workspace highlights`,
    `${scoped}pricing and plans`,
    `${scoped}facilities and amenities`,
    `${scoped}location and site visit`,
    `${scoped}booking process`
  ].filter(Boolean);

  const retrievals = await Promise.all(
    retrievalQueries.map((q) => searchRelevantChunks(q, topK))
  );

  // Deduplicate while preserving order.
  const seen = new Set();
  const merged = [];
  for (const arr of retrievals) {
    for (const text of arr) {
      const key = text?.trim();
      if (!key || seen.has(key)) continue;
      seen.add(key);
      merged.push(key);
    }
  }

  return merged;
}




// 🌐 Webhook Verification (GET)
app.get("/webhook", (req, res) => {
    const mode = req.query["hub.mode"];
    const token = req.query["hub.verify_token"];
    const challenge = req.query["hub.challenge"];

    if (mode && token) {
        if (mode === "subscribe" && token === VERIFY_TOKEN) {
            console.log("WEBHOOK VERIFIED");
            res.status(200).send(challenge);
        } else {
            res.sendStatus(403);
        }
    }
});

wss.on("connection", (ws) => {
  console.log("WebSocket client connected");
  clients.add(ws);

  ws.on("close", () => {
    clients.delete(ws);
    console.log("WebSocket client disconnected");
  });

  ws.on("message", (message) => {
    console.log("Received message:", message.toString());
    // You can handle messages here if needed
  });
});

async function autoassign(leadId) {
  if (!leadId) return;
  console.log(leadId)

  try {
    const response = await fetch(
      `https://ask4people.in/crmapi/leads/backend/${leadId}`,
      {
        method: "POST",
      }
    );

    const result = await response.json();

  } catch (err) {
    console.error("Auto-assign failed:", err.message);
  }
}
// Broadcast helper
function broadcast(data) {
  const message = JSON.stringify(data);
  clients.forEach((client) => {
    if (client.readyState === 1) client.send(message);
  });

  // Trigger auto-assign safely (skip Meta Ads leads, since they are already bound to bindhu)
  if (data.type === "new_user" && data.user?._id) {
    if (data.user.leadfrom && data.user.leadfrom.toLowerCase() === "meta ads") {
      console.log("Skipping autoassign for Meta Ads lead (already set to bindhu):", data.user._id);
      return;
    }

    try {
      autoassign(data.user._id);
    } catch (err) {
      console.error("Auto-assign failed:", err);
    }
  }
}


// function broadcast(type, payload) {
//   const message = JSON.stringify({ type, payload });
//   clients.forEach((client) => {
//     if (client.readyState === 1) client.send(message);
//   });
// }

// Notify the counsellor assigned to a lead when they reply after a follow-up or reply to a campaign
async function notifyCounsellorOfReply(userChat, reason, campaignName) {
  try {
    if (!userChat.assignedto) return;

    const counsellor = await User.findOne({ username: userChat.assignedto });
    if (!counsellor || !counsellor.userNumber) return;

    const label = reason === "followup"
      ? "🔥 *Replied After Follow-up*"
      : "🔥 *Replied to Campaign*";

    const campaignLine = reason === "campaign" && campaignName
      ? `📣 *Campaign:* ${campaignName}\n`
      : "";

    const notificationPayload =
      `Hi ${counsellor.username}, 👋\n\n` +
      `${label}\n\n` +
      `👤 *Name:* ${userChat.userName}\n` +
      `📞 *Mobile Number:* ${userChat.userNumber}\n` +
      campaignLine + `\n` +
      `🔗 Please login to view details:\n` +
      `https://ask4people.in`;

    await sendMessage(counsellor.userNumber, notificationPayload);
    // await sendMessage("919597271533",notificationPayload)
  } catch (err) {
    console.error("Error notifying counsellor of reply:", err);
  }
}

async function sendMessage(to, text) {
  await fetch(`https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`, {
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

// ===== WhatsApp voice notes: recognize incoming, reply with voice too =====
// STT/TTS both go through the sarvam service (d:\Etherservices\sarvam) so
// the TTS_PROVIDER switch there (Sarvam Bulbul vs Cartesia Sonic) is the one
// place that controls voice-output billing for every channel, not just this one.
const SARVAM_SERVICE_URL = process.env.SARVAM_SERVICE_URL || "http://localhost:7000";

// async function downloadWhatsAppMedia(mediaId) {
//   const metaRes = await fetch(`https://graph.facebook.com/v20.0/${mediaId}`, {
//     headers: { Authorization: `Bearer ${ACCESS_TOKEN}` },
//   });
//   const meta = await metaRes.json();
//   if (!meta.url) throw new Error(`Could not resolve WhatsApp media URL: ${JSON.stringify(meta)}`);

//   const fileRes = await fetch(meta.url, {
//     headers: { Authorization: `Bearer ${ACCESS_TOKEN}` },
//   });
//   const arrayBuffer = await fileRes.arrayBuffer();
//   return { buffer: Buffer.from(arrayBuffer), mimeType: meta.mime_type || "audio/ogg" };
// }

// async function transcribeViaSarvam(buffer, mimeType) {
//   const form = new FormData();
//   form.append("audio", new Blob([buffer], { type: mimeType }), "voice-note.ogg");

//   const res = await fetch(`${SARVAM_SERVICE_URL}/api/stt`, { method: "POST", body: form });
//   if (!res.ok) throw new Error(`Sarvam STT failed: ${res.status} ${await res.text()}`);
//   const data = await res.json();
//   return data.transcript;
// }

// async function synthesizeViaSarvam(text) {
//   const res = await fetch(`${SARVAM_SERVICE_URL}/api/tts`, {
//     method: "POST",
//     headers: { "Content-Type": "application/json" },
//     body: JSON.stringify({ text }),
//   });
//   if (!res.ok) throw new Error(`Sarvam TTS failed: ${res.status} ${await res.text()}`);
//   return res.json(); // { audioBase64, audioMimeType }
// }

// async function generateReplyViaSarvam(messages) {
//   const res = await fetch(`${SARVAM_SERVICE_URL}/api/chat`, {
//     method: "POST",
//     headers: { "Content-Type": "application/json" },
//     body: JSON.stringify({ messages }),
//   });
//   if (!res.ok) throw new Error(`Sarvam chat failed: ${res.status} ${await res.text()}`);
//   const data = await res.json();
//   return data.reply;
// }

// Plain-text company info + Tamil-voice system prompt for WhatsApp voice-note
// replies — sent straight into Sarvam's LLM (sarvam-105b-conversations) with
// no embeddings/vector search, since a short spoken reply doesn't need RAG's
// precision and skipping it keeps voice-note turnaround fast. Kept separate
// from the text-chat RAG system prompt below, so text chat is unaffected.
const VOICE_COMPANY_INFO = `

About BetaSpace: [One or two lines about BetaSpace, e.g. flexible, ready-to-use workspaces in Coimbatore for startups, freelancers, and growing teams]. [Years in operation / number of members or companies served, if you want to mention it]. [Recognitions or partnerships, only if verified]. Ratings: Google [X.X]/5 with [N]+ reviews.

Locations: [Branch 1 name and area, e.g. near Gandhipuram], [Branch 2 name and area]. Easy to reach by [bus / main road / landmark]. Parking: [two-wheeler / four-wheeler details]. Office timing for visits and enquiries: [9:30 AM to 6:30 PM, days of the week]. Workspace access hours for members: [24x7 or specific hours].

Workspace types: [Hot desk / dedicated desk, private cabin, team office, meeting room, conference room, virtual office, training room, event space]. Add one short line about each type and who it suits, for example private cabin for small teams, virtual office for GST and business address registration.

Facilities: [High-speed Wi-Fi, power backup, air conditioning, furnished seating, meeting rooms, printing, pantry and tea or coffee, reception, CCTV and security, housekeeping, lift access, parking]. Mention only what is verified.

Pricing: [Starting price for each workspace type per seat or per month, or say pricing depends on team size and duration and the team will share a quote]. Deposit: [amount or none]. GST: [included or extra]. Minimum duration: [daily / monthly / yearly]. Discounts: [only if officially offered, otherwise say none].

Booking process: [How to book: site visit, then agreement, then payment]. Documents needed: [ID proof, company proof, GST details, etc.]. Availability must always be confirmed by the BetaSpace team. Never confirm a booking.

Benefits over a traditional office: [Lower setup cost, no long lease lock-in, ready infrastructure, flexible scaling, professional business address, networking with other members]. Mention only the ones that apply.

Site visits: [Visit timings, whether prior appointment is needed].

Contact: Phone or WhatsApp [number, spoken digit by digit style], email [address]. If the customer needs a link or detailed brochure, say we will share it on WhatsApp.
`;

function buildVoiceSystemPrompt(course) {
  const workspaceType = course; // the CRM "course" field now holds the lead's workspace interest
  return `
You are Beta Assistant, the friendly, professional, and persuasive workspace consultant for BetaSpace, Coimbatore, replying to a WhatsApp voice note.

🎯 Goal: Help customers find the right workspace at BetaSpace based on their business needs, team size, preferred location, budget, and duration, and convert genuine inquiries into site visits, pricing inquiries, and bookings.

🧭 Guidelines:
1. Never reveal you are an AI model. Always speak as "we" when referring to BetaSpace, like a real BetaSpace workspace consultant.
2. Reply primarily in natural spoken Tamil, keeping English and business words (workspace names like "coworking space", "private cabin", "meeting room", "virtual office", "GST", "Wi-Fi", "parking", "per seat", etc.) in English, in the same Tanglish style the caller uses. Do not reply only in English.
3. This reply is read aloud by text-to-speech: plain spoken sentences only. NO markdown, NO bullet points, NO numbered lists, NO emojis, NO asterisks, NO links. If the customer needs a link, say that we will share it on WhatsApp or text.
4. Maximum 2-3 short sentences. Never a long paragraph.
5. Personalize answers based on the customer's workspace interest: ${workspaceType || "not yet known, politely find out early"}.
6. Use only the company details below. Never invent availability, prices, discounts, facilities, lease terms, or booking confirmations. If unsure, say you are not certain and offer to connect them with the BetaSpace team.
7. When the user greets (Hi, Hello, Vanakkam, etc.), give a warm greeting and briefly introduce yourself, and ask what kind of workspace they are looking for. Introduce yourself only on a greeting or first turn, never mid-conversation.
8. Answer the actual question first, then ask at most ONE follow-up question, about the most important missing requirement. Gather requirements in this order: business type or need, team size, preferred location, budget, and duration. If the customer already told you something, do not ask for it again.
9. Do NOT push site visits or bookings in early replies. First understand their requirements and answer their questions. Suggest a site visit or booking call only after the essential requirements are clear, or if the user directly asks about pricing, availability, a visit, or booking.
10. Never say a workspace is available or a booking is confirmed. Say the BetaSpace team will confirm availability.
11. When useful, briefly mention the benefits of a flexible workspace compared with setting up a traditional office, such as lower setup cost, ready infrastructure, and flexible terms, but only if they are in the company details.

company info: ${VOICE_COMPANY_INFO}
`;
}

// async function uploadWhatsAppMedia(buffer, mimeType) {
//   const form = new FormData();
//   form.append("messaging_product", "whatsapp");
//   form.append("file", new Blob([buffer], { type: mimeType }), "reply.mp3");
//   form.append("type", mimeType);

//   const res = await fetch(`https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/media`, {
//     method: "POST",
//     headers: { Authorization: `Bearer ${ACCESS_TOKEN}` },
//     body: form,
//   });
//   const data = await res.json();
//   if (!data.id) throw new Error(`WhatsApp media upload failed: ${JSON.stringify(data)}`);
//   return data.id;
// }

// async function sendVoiceMessage(to, mediaId) {
//   await fetch(`https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`, {
//     method: "POST",
//     headers: {
//       Authorization: `Bearer ${ACCESS_TOKEN}`,
//       "Content-Type": "application/json",
//     },
//     body: JSON.stringify({
//       messaging_product: "whatsapp",
//       to,
//       type: "audio",
//       audio: { id: mediaId },
//     }),
//   });
// }


async function sendMessagecourse(to, text , replyTitle) {
  const userChat = await Chat.findOne({ userNumber: to });
  const conversation = {
    userMsg: `I was interested in ${replyTitle}`,
    botReply: text
  };

  await fetch(`https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`, {
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

  if (!userChat) {
    console.warn(`sendMessagecourse: Chat not found for ${to}, message sent but not saved.`);
    return;
  }

  userChat.conversations.push(conversation);
  userChat.lastInteracted = new Date();
  await userChat.save();

}

// async function transfer(body){
//   try {
//     await fetch(process.env.Ether_Microservice, {
//       method: "POST",
//       headers: {
//         "Content-Type": "application/json",
//       },
//       body: JSON.stringify(body)
//     });
//   } catch (err) {
//     console.error("Microservice unreachable:", err.message);
//   }

// } 


async function typingindicator(to, messageId) {
  const baseUrl = `https://graph.facebook.com/v24.0/${PHONE_NUMBER_ID}/messages`;

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



// Guards against duplicate processing when WhatsApp redelivers the same
// webhook event (e.g. because our handler took too long to respond 200).
const processedMessageIds = new Map();
const MESSAGE_ID_TTL_MS = 24 * 60 * 60 * 1000; // 24h

function isDuplicateMessage(messageId) {
  if (!messageId) return false;
  const now = Date.now();
  if (processedMessageIds.size > 5000) {
    for (const [id, ts] of processedMessageIds) {
      if (now - ts > MESSAGE_ID_TTL_MS) processedMessageIds.delete(id);
    }
  }
  if (processedMessageIds.has(messageId)) return true;
  processedMessageIds.set(messageId, now);
  return false;
}

// Admin number that owns the manager RAG chatbot (see the "report" and
// manager-chat branches below) — mirrors CRM-app-be's MANAGER_ADMIN_NUMBER.
// const MANAGER_ADMIN_NUMBER = "919360549855";
const MANAGER_ADMIN_NUMBER = "919597271533";

// Lets MANAGER_ADMIN_NUMBER toggle (by texting "change") between the manager
// RAG chatbot and chatting normally like any other user.
let managerChatModeEnabled = true;

app.post("/webhook", async (req, res) => {

  const body = req.body

    if (body.object === "page") {
      // res.sendStatus(200);
      handlepage(body);
      // res.sendStatus(200);
    }

  try {
    const entry = req.body.entry?.[0];
    const changes = entry?.changes?.[0];
    const message = changes?.value?.messages?.[0];
    const from = message?.from;
    const userName = changes?.value?.contacts?.[0]?.profile?.name || "User";

    if (!from) return res.sendStatus(200);
     const temp_id = req.body.entry[0]
  .changes[0]
  .value
  .metadata
  .phone_number_id;
    let msgBody = "";
    let isVoiceMessage = false;
    if (message.type === "text" && message.text?.body) {
      msgBody = message.text.body.trim();
    } else if (message.type === "audio" && message.audio?.id) {
      try {
        const { buffer, mimeType } = await downloadWhatsAppMedia(message.audio.id);
        const transcript = await transcribeViaSarvam(buffer, mimeType);
        if (transcript && transcript.trim()) {
          msgBody = transcript.trim();
          isVoiceMessage = true;
        }
      } catch (err) {
        console.error("❌ Voice note transcription failed:", err.message);
      }
    }

    if (from === "919597271533" && msgBody.toLowerCase() === "report") {
      sendDailyReport();
      return res.sendStatus(200);
    }

    // Let the admin number toggle between the manager RAG chatbot and
    // chatting normally like any other user by texting "change".
    if (from === MANAGER_ADMIN_NUMBER && msgBody.toLowerCase() === "change") {
      managerChatModeEnabled = !managerChatModeEnabled;
      await sendMessage(from, managerChatModeEnabled
        ? "🔀 Switched to Manager chat mode."
        : "🔀 Switched to normal chat mode.");
      return res.sendStatus(200);
    }

    // Manager RAG chatbot: any other message from the admin number is a
    // free-form question about CRM data — hand it to CRM-app-be's
    // ManagerChat.js, which computes the answer and sends the WhatsApp
    // reply itself (same pattern as the ManagerCheckIn routing below).
    if (from === MANAGER_ADMIN_NUMBER && msgBody && managerChatModeEnabled) {
      console.log("Manager called")
      if (process.env.CRM_API_URL) {
        fetch(`${process.env.CRM_API_URL}/manager/chat/incoming`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ from, message: msgBody })
        }).catch(err => console.error("❌ Manager chat routing failed:", err.message));
      } else {
        console.error("❌ CRM_API_URL not set — cannot route manager chat question");
      }
      return res.sendStatus(200);
    }

    const msg = changes?.value?.messages?.[0];

let leadFromAds = false;
 

if (msg?.referral) {
  leadFromAds = true;
  num = "919360549855"
  leadmsg = `Lead from ads ${from}`
  sendMessage(num,leadmsg)
    console.log("Lead from ads:", msg.referral);
} else {
    // console.log("Not from ads");
}
     const messageId = message.id; // WhatsApp message ID

     if (isDuplicateMessage(messageId)) {
       console.log("Duplicate webhook delivery ignored:", messageId);
       return res.sendStatus(200);
     }

     // Daily WhatsApp "manager" check-in: if this sender has an in-progress
     // check-in conversation with CRM-app-be, route the reply there instead of
     // the lead chatbot below, and stop — this must never touch lead Chat docs.
     try {
       const activeCheckIn = await ManagerCheckIn.findOne({ counsellorNumber: from, status: "in_progress" }).lean();
       if (activeCheckIn) {
         if (process.env.CRM_API_URL) {
           await fetch(`${process.env.CRM_API_URL}/manager/checkin/reply`, {
             method: "POST",
             headers: { "Content-Type": "application/json" },
             body: JSON.stringify({ userNumber: from, message: msgBody })
           });
         } else {
           console.error("❌ CRM_API_URL not set — cannot route manager check-in reply");
         }
         return res.sendStatus(200);
       }
     } catch (mgrErr) {
       console.error("❌ Manager check-in routing failed:", mgrErr.message);
     }

     // Daily sales-training quiz: if this sender has an in-progress training
     // session (see SalesTraining.js), treat this text as their answer
     // instead of the lead chatbot below, and stop.
     try {
       const trainingHandled = await handleTrainingReply(from, msgBody);
       if (trainingHandled) {
         return res.sendStatus(200);
       }
     } catch (trainingErr) {
       console.error("❌ Sales training reply routing failed:", trainingErr.message);
     }

     console.log(from,msgBody)

      // ✅ Step 1: Mark message as read + show typing indicator
      // await markMessageAsRead(from,messageId);

      await typingindicator(from,messageId);

      // await new Promise(resolve => setTimeout(resolve, 2000));


    // 🧠 Find or create user
    let userChat = await Chat.findOne({ userNumber: from });
    if (!userChat) {
      // If this number was messaged as part of a bulk-Excel campaign and is
      // replying for the first time, promote it from TempLead into a real lead.
      const tempLead = await TempLead.findOne({ userNumber: from });

      userChat = new Chat({
        userNumber: from,
        userName: (tempLead?.userName && tempLead.userName !== "Unknown") ? tempLead.userName : (userName || "Unknown"),
        conversations: [],
        courseSelected: null,
        lastInteracted: new Date(),
        leadfrom: tempLead ? "BulkMessage" : (leadFromAds ? "metaads" : "Whatsapp"),
        ...(tempLead ? {
          status: "HOT",
          campaignHistory: [{
            campaignName: tempLead.campaignName,
            message: tempLead.message,
            sentAt: tempLead.sentAt,
            repliedAt: new Date()
          }]
        } : {})
      });
      await userChat.save();

      if (tempLead) {
        await TempLead.deleteOne({ _id: tempLead._id });
      }

      broadcast({ type: "new_user", user: userChat });

      await sendMessage(from, `Hi ${userName}, Thanks for choosing Beta Space  Coimbatore How can we help you today`);
      // await sendInitialTwoButtons(from);
      // await sendCourseSelectionButtons(from);
      return res.sendStatus(200);
    }

    // 🎯 Handle course selection
//     if (message?.type === "interactive" && message.interactive.type === "list_reply") {
//       const replyId = message.interactive.list_reply.id;
//       const replyTitle = message.interactive.list_reply.title;

//       console.log(`📌 User selected: ${replyTitle} (${replyId})`);

//       // save course
//       userChat.course = replyTitle;
//       await userChat.save();


// const messages = [
//   `Excellent choice *${replyTitle}*! 🏢✨\nIt's a great option for creating a professional and productive work environment.\nWould you like to know more about our workspace options? 😊`,

//   `Great pick choosing *${replyTitle}*! 🚀\nWe can help you find a workspace that suits your business needs.\nWould you like to know about pricing, facilities, and availability?`,

//   `Awesome! *${replyTitle}* could be the right fit for your workspace needs! 📈\nLet's explore the available options at BetaSpace.\nWould you like to know more about our facilities and plans?`,

//   `Good decision selecting *${replyTitle}*! 💼\nA professional workspace can help your team work comfortably and grow your business.\nWould you like to explore our workspace options and pricing?`,

//   `Nice choice with *${replyTitle}*! 🙌\nAt BetaSpace, we're here to help you find a workspace that matches your requirements.\nWould you like to know about our locations, facilities, or pricing?`
// ];


// // Pick random message
// const randomMessage = messages[Math.floor(Math.random() * messages.length)];



//     // await sendMessage(from, randomMessage);
//      await sendMessagecourse(from,randomMessage,replyTitle)
//       return res.sendStatus(200);
//     }

// A follow-up is "awaiting a reply" whenever a cron has sent one and the lead
// hasn't answered yet. The flag is the reliable signal; the timestamp check is
// kept only as a fallback for leads whose follow-up predates this flag.
// NOTE: this must run before the "no course selected" early-return below, since
// the 10 AM cron also follows up with leads who never picked a course.
if (
  userChat.followUpAwaitingReply ||
  (
    userChat.lastFollowUpSentAt &&
    new Date(userChat.lastInteracted) < userChat.lastFollowUpSentAt && // last chat was before follow-up
    new Date() > userChat.lastFollowUpSentAt // current message after follow-up
  )
) {
  userChat.respondedAfterFollowUp = true;
  userChat.followUpAwaitingReply = false;
  userChat.status = "HOT";
  console.log(`✅ ${userName} responded after follow-up`);
  await notifyCounsellorOfReply(userChat, "followup");
  await userChat.save();
}

    // 🚫 If user hasn’t selected a course, block AI chat
    // if (!userChat.course) {
    //   await sendMessage(from, `Hi ${userName || 'User'}, you must select an option to continue chatting with AI 👇`);
    //   // await sendInitialTwoButtons(from);
    //     await sendCourseSelectionButtons(from);
    //   return res.sendStatus(200);
    // }

{
  let campaignReplied = false;
  let repliedCampaignName = null;
  let latestSentAt = null;
  (userChat.campaignHistory || []).forEach(entry => {
    if (!entry.repliedAt && new Date() > entry.sentAt) {
      entry.repliedAt = new Date();
      campaignReplied = true;
      if (!latestSentAt || entry.sentAt > latestSentAt) {
        latestSentAt = entry.sentAt;
        repliedCampaignName = entry.campaignName;
      }
    }
  });
  if (campaignReplied) {
    userChat.status = "HOT";
    userChat.markModified("campaignHistory");
    console.log(`✅ ${userName} replied to campaign`);
    notifyCounsellorOfReply(userChat, "campaign", repliedCampaignName);
  }
}

    // 💬 AI chat only after course selection + profession if needed
    if (msgBody) {
      const history = userChat.conversations.map(conv => ([
        { role: "user", content: conv.userMsg },
        { role: "assistant", content: conv.botReply },
      ])).flat()
      .filter(m => typeof m.content === "string" && m.content.trim() !== "");

      let reply;

      // Voice notes: answer with Sarvam's own LLM off plain company info
      // (no OpenAI RAG/vector search) and reply in Tamil, matching the
      // Cartesia TTS voice reply sent below. Falls back to the normal
      // OpenAI+RAG path if the Sarvam service is unreachable.
      if (isVoiceMessage) {
        try {
          reply = await generateReplyViaSarvam([
            { role: "system", content: buildVoiceSystemPrompt(userChat.course) },
            ...history.slice(-9),
            { role: "user", content: msgBody },
          ]);
        } catch (err) {
          console.error("❌ Sarvam chat reply failed, falling back to OpenAI:", err.message);
        }
      }

      if (!reply) {

    let new_content = await gatherCourseRagContext(msgBody, userChat.course, 4);

    console.log(userChat.course)

      const messages = [
        {
          role: "system",
          content: `

You are Beta Assistant, the friendly, professional, and persuasive AI workspace consultant for BetaSpace, Coimbatore.

🎯 Primary Goal

Help customers find the right workspace at BetaSpace based on their business needs, team size, preferred location, budget, and duration.

Your goal is to:

Explain workspace options and facilities clearly.

Understand customer requirements step by step.

Recommend suitable workspace solutions.

Build trust and highlight the benefits of BetaSpace.

Convert genuine inquiries into site visits, pricing inquiries, and workspace bookings.

🧭 Guidelines

Never reveal that you are an AI model. Communicate naturally as a BetaSpace workspace consultant.

Always use “we” when referring to BetaSpace. Speak as a representative of the company.

Keep replies concise, friendly, professional, and customer-focused.

Focus only on BetaSpace services, workspace solutions, facilities, locations, and related customer queries.

Personalize responses based on the customer's known requirements:

If any variable is unavailable, unknown, or not yet provided, ask the customer naturally when appropriate. Never assume missing values.

Use the verified company information provided below. Never invent availability, discounts, prices, facilities, lease terms, or booking confirmations.

Greeting behavior:
When the user says Hi, Hello, Hey, or another greeting, respond with a warm greeting, such as:

"👋 Hello! Welcome to BetaSpace! We're here to help you find the perfect workspace for your business. What kind of workspace are you looking for today?"

Important:

Introduce yourself only during a greeting or when an introduction is requested.

Do not repeatedly introduce yourself during ongoing conversations.

When customers ask questions or express interest, combine these roles naturally:

Workspace Consultant: Recommend a suitable workspace based on the customer's needs.

Business Advisor: Explain the benefits of flexible workspaces compared with setting up a traditional office.

Customer Support Representative: Answer questions about facilities, locations, access, and services.

Sales Consultant: Explain relevant options and guide customers toward the next step.

Relationship Builder: Make customers feel comfortable asking questions without applying unnecessary pressure.

Keep conversations crisp and focused:

Use short, clear sentences.

Use bullet points for facilities, benefits, pricing, and comparisons.

Avoid unnecessary repetition and long paragraphs.

Answer the actual question before asking a follow-up question.

Include relevant official links whenever users ask about workspace types, locations, facilities, pricing information, or bookings.

If you are uncertain about a detail, do not hallucinate. Explain that the information needs confirmation and direct the customer to the BetaSpace team.

Do not immediately push customers to book a workspace or schedule a visit. First understand their requirements and answer their questions.

Follow the Gather Customer Requirements Step by Step process in the specified order.

Ask only ONE follow-up question at a time, based on the most important missing requirement.

A site-visit or booking call to action is allowed after the essential requirements are understood, or earlier if the customer explicitly requests pricing, availability, a site visit, or a booking.

If the customer has already provided a detail, do not ask for it again. Remember and use information from the conversation.

Do not claim that a workspace is available or a booking is confirmed unless the relevant information has been verified by the BetaSpace team or an authorized booking system.
company info : ${new_content.join("\n")}
`,
        },
        ...history.slice(-9),
        { role: "user", content: msgBody },
      ];

      const response = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages,
      });

      reply = response.choices[0].message.content;
      }

      if (isVoiceMessage) {
        try {
          const { audioBase64, audioMimeType } = await synthesizeViaSarvam(reply);
          const mediaId = await uploadWhatsAppMedia(Buffer.from(audioBase64, "base64"), audioMimeType);
          await sendVoiceMessage(from, mediaId);
        } catch (err) {
          console.error("❌ Voice reply failed, falling back to text:", err.message);
          await sendMessage(from, reply);
        }
      } else {
        await sendMessage(from, reply);
      }

      const data = {
        type: "new_message",
        userNumber: from,
        conversation: {
          userMsg: msgBody,
          botReply: reply,
          time: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
        }
      };
      broadcast(data);


      userChat.conversations.push({ userMsg: msgBody, botReply: reply });
      // if (userChat.conversations.length > 10) {
      //   userChat.conversations = userChat.conversations.slice(-10);
      // }
      userChat.lastInteracted = new Date();
      await userChat.save();
    }

    return res.sendStatus(200);
  } catch (err) {
    console.error("❌ Webhook Error:", err);
    return res.sendStatus(500);
  }
});


const createAccessToken = (userId) => {
  // Access token valid for 15 minutes
  return jwt.sign({ id: userId }, process.env.JWT_ACCESS_SECRET, { expiresIn: "15m" });
};
 
const createRefreshToken = (userId) => {
  // Refresh token valid for 15 days
  return jwt.sign({ id: userId }, process.env.JWT_REFRESH_SECRET, { expiresIn: "15d" });
};
 
// 🧾 Register
app.post("/api/register", async (req, res) => {
  try {
    const { username, email, password } = req.body;

    console.log(username,email,password)
 
    if (!username || !email || !password)
      return res.status(400).json({ message: "All fields required" });
 
    const existing = await User.findOne({ email });
    if (existing) return res.status(400).json({ message: "User already exists" });
 
    const hashed = await bcrypt.hash(password, 10);
    const user = new User({ username, email, password: hashed });
    await user.save();
 
    res.status(201).json({ message: "✅ Registered successfully" });
  } catch (err) {
    res.status(500).json({ message: "Server error", error: err.message });
  }
});
 
// 🔑 Login
app.post("/api/login", async (req, res) => {
  try {
    const { email, password } = req.body;
 
    const user = await User.findOne({ email });
    if (!user) return res.status(400).json({ message: "Invalid credentials" });
 
    const valid = await bcrypt.compare(password, user.password);
    if (!valid) return res.status(400).json({ message: "Invalid credentials" });
 
    const accessToken = createAccessToken(user._id);
    const refreshToken = createRefreshToken(user._id);
 
    user.refreshToken = refreshToken;
    await user.save();
 
    res.cookie("accessToken", accessToken, {
      httpOnly: true,
      maxAge: 15 * 60 * 1000, // 15 minutes
      sameSite: "strict",
      secure: false,
    });
 
    res.cookie("refreshToken", refreshToken, {
      httpOnly: true,
      maxAge: 15 * 24 * 60 * 60 * 1000, // 15 days
      sameSite: "strict",
      secure: false,
    });
 
    res.json({ message: "✅ Login successful" });
  } catch (err) {
    console.error("Login Error:", err);
    res.status(500).json({ message: "Server error", error: err.message });
  }
});
 
// 🔁 Refresh Token Endpoint
app.post("/api/refresh", async (req, res) => {
  try {
    const refreshToken = req.cookies.refreshToken;
    if (!refreshToken) return res.status(401).json({ message: "No refresh token" });
 
    const user = await User.findOne({ refreshToken });
    if (!user) return res.status(403).json({ message: "Invalid refresh token" });
 
    jwt.verify(refreshToken, process.env.JWT_REFRESH_SECRET, (err, decoded) => {
      if (err) return res.status(403).json({ message: "Refresh token expired" });
 
      const newAccessToken = createAccessToken(decoded.id);
      res.cookie("accessToken", newAccessToken, {
        httpOnly: true,
        maxAge: 15 * 60 * 1000, // 15 minutes
        sameSite: "strict",
        secure: false,
      });
 
      res.json({ message: "🔄 Token refreshed" });
    });
  } catch (err) {
    res.status(500).json({ message: "Server error", error: err.message });
  }
});
 
// 🧭 Verify Access Token Middleware
const verifyToken = (req, res, next) => {
  const token = req.cookies.accessToken;
  if (!token) return res.status(401).json({ message: "Access token missing" });
 
  jwt.verify(token, process.env.JWT_ACCESS_SECRET, (err, decoded) => {
    if (err) return res.status(403).json({ message: "Access token expired" });
    req.userId = decoded.id;
    next();
  });
};

 
// 🧠 Protected Route
app.get("/api/dashboard", verifyToken, (req, res) => {
  res.json({ message: "✅ Welcome to Dashboard", userId: req.userId });
});
 
// 🚪 Logout
app.post("/api/logout", async (req, res) => {
  const refreshToken = req.cookies.refreshToken;
  if (refreshToken) {
    await User.updateOne({ refreshToken }, { $unset: { refreshToken: 1 } });
  }
  res.clearCookie("accessToken");
  res.clearCookie("refreshToken");
  res.json({ message: "👋 Logged out successfully" });
});





app.post('/api/website', async (req, res) => {
  try {
    const { First, college, NoofStudent, internshipdurationcheckbox } = req.body;
    const tel125 = req.body['tel-125']; 

    if (!tel125) {
      return res.status(400).json({ message: "Phone number is required." });
    }

    // ✅ 2. Remove non-digit characters and validate 10 digits
    const sanitizedNumber = tel125.replace(/\D/g, "");


    // ✅ 3. Ensure the number includes +91 prefix
    const fullNumber = cleanIndianMobileNumber(sanitizedNumber)

    // ✅ 4. Check if user already exists
    let userChat = await Chat.findOne({ userNumber: fullNumber });
    if (userChat) {
      return res.status(400).json({ message: "User already exists." });
    }

    // ✅ 5. Create and save new user
    userChat = new Chat({
      userNumber: fullNumber,
      userName: First?.trim() || "Unknown",
      conversations: [],
       course: "College Internship",
        lastInteracted: new Date(),
        leadfrom: "Website",
    });

    await userChat.save();


    // ✅ 6. Notify via WebSocket (if configured)
    broadcast({ type: "new_user", user: userChat });

    // ✅ 7. Add an initial conversation
    const followUp = `Hi ${First || "there"}! 👋 Thanks for showing interest in Beta Space. We’re excited to have you on board and can’t wait to help you kickstart your working journey! 🚀`;

    // await sendDynamicMessage(fullNumber,followUp)
    await sendfixedtemplate2(fullNumber)
    userChat.conversations.push({
      userMsg: `Hi, I'm ${First || "there" } from ${college || "Not mentioned"}. We have ${NoofStudent || "0" } students interested in the internship program for ${internshipdurationcheckbox || "Not mentioned"} `,
      botReply: followUp,
    });

    await userChat.save();

    res.status(201).json({ success: true, message: 'Contact saved successfully!' });

  } catch (err) {
    console.error('Error saving contact:', err);
    res.status(500).json({ error: 'Server error. Please try again later.' });
  }
});

app.post("/email/demobooking",async(req,res)=>{
  try {
    console.log(req.body)
    const { userName, email } = req.body;
    const appointmentDate = req.body['appointment-date'];
    const appointmentTime = req.body['appointment-time'];
    const course = req.body['course'];
    const tel125 = req.body['tel-125'];

    const sanitizedNumber = tel125.replace(/\D/g, "");
    const fullNumber = cleanIndianMobileNumber(sanitizedNumber);

    const userChat = await Chat.findOne({ userNumber: fullNumber });

    if (userChat) {
      userChat.demoBooked = true;
      userChat.demoBookedAt = new Date();
      await userChat.save();
      broadcast({ type: "new_user", user: userChat });
    }

    if (userChat && userChat.assignedto) {
      const assignedUser = await User.findOne({ username: userChat.assignedto });
      if (assignedUser && assignedUser.userNumber) {
        const msg = `📅 Demo Booked!\n\nName: ${userName}\nEmail: ${email}\nPhone: ${tel125}\nDate: ${appointmentDate}\nTime: ${appointmentTime}\ncourse :${course} `;
        await sendMessage(assignedUser.userNumber, msg);
        // await sendMessage("919597271533", msg);
        const adminMsg = `${msg}\nAssigned User: ${assignedUser.userNumber}`;
       await sendMessage("919597271533", adminMsg);
      }
    }

    res.status(200).json({ success: true, message: "Demo booking notification sent." });
  } catch (err) {
    console.error("Error in /email/demobooking:", err);
    res.status(500).json({ error: "Server error." });
  }
})

app.post('/api/enquiry', async (req, res) => {
  try {
    console.log(req.body)
    const { userName, programtype, profession , course } = req.body;
    const tel125 = req.body['tel-125']; 

    if (!tel125) {
      return res.status(400).json({ message: "Phone number is required." });
    }

    // ✅ 2. Remove non-digit characters and validate 10 digits
    const sanitizedNumber = tel125.replace(/\D/g, "");

    // ✅ 3. Ensure the number includes +91 prefix
    const fullNumber = cleanIndianMobileNumber(sanitizedNumber)

    // ✅ 4. Check if user already exists
    let userChat = await Chat.findOne({ userNumber: fullNumber });
    if (userChat) {
      return res.status(400).json({ message: "User already exists." });
    }

    // ✅ 5. Create and save new user
    userChat = new Chat({
      userNumber: fullNumber,
      userName: userName || "Unknown",
      conversations: [],
      profession : profession,
      programtype : programtype,
       course: course,
        lastInteracted: new Date(),
        leadfrom: detectLeadFrom(req.body),
    });

    await userChat.save();


    // ✅ 6. Notify via WebSocket (if configured)
    broadcast({ type: "new_user", user: userChat });

    // ✅ 7. Add an initial conversation
    const followUp = `Hi ${userName || "there"}! 👋 Thanks for showing interest in Beta Space. We’re excited to have you on board and can’t wait to help you kickstart your working journey! 🚀`;

    // await sendDynamicMessage(fullNumber,followUp)
    await sendfixedtemplate2(fullNumber)
    userChat.conversations.push({
      userMsg: `Hi, I'm ${userName || "there" } , I have enquired about ${course || "N/A"} for ${programtype || 'Specified hours'} `,
      botReply: followUp,
    });

    await userChat.save();

    res.status(201).json({ success: true, message: 'Contact saved successfully!' });

  } catch (err) {
    console.error('Error saving contact:', err);
    res.status(500).json({ error: 'Server error. Please try again later.' });
  }
});


// app.post("/ads/couns1",async (req,res)=>{

//   try{
//     const {userName , collegename , studyyear , duration , course , couns } = req.body;
//     const tel125 = req.body['tel-125']; 

//     if (!tel125) {
//       return res.status(400).json({ message: "Phone number is required." });
//     }

//     // ✅ 2. Remove non-digit characters and validate 10 digits
//     const sanitizedNumber = tel125.replace(/\D/g, "");


//     // ✅ 3. Ensure the number includes +91 prefix
//     const fullNumber = cleanIndianMobileNumber(sanitizedNumber)

//     // ✅ 4. Check if user already exists
//     let userChat = await Chat.findOne({ userNumber: fullNumber });
//     if (userChat) {
//       return res.status(400).json({ message: "User already exists." });
//     }

//     // ✅ 5. Create and save new user
//     userChat = new Chat({
//       userNumber: fullNumber,
//       userName: userName?.trim() || "Unknown",
//       conversations: [],
//        course: "College Internship",
//         lastInteracted: new Date(),
//         leadfrom: "metaads",
//         assignedto : "Abhi"
//     });

//     await userChat.save();


//     // ✅ 6. Notify via WebSocket (if configured)
//     broadcast({ type: "new_user", user: userChat });

//     // ✅ 7. Add an initial conversation
//     const followUp = `Hi ${userName || "there"}! 👋 Thanks for showing interest in Beta Space. We’re excited to have you on board and can’t wait to help you kickstart your working journey! 🚀`;

//     // await sendDynamicMessage(fullNumber,followUp)
//     // await sendfixedtemplate2(fullNumber)
//     userChat.conversations.push({
//       userMsg: `Hi, I'm ${userName || "there" } from ${collegename} of ${studyyear} I was intersted in ${course} Internship for ${duration} `,
//       botReply: followUp,
//     });

//     await userChat.save();

//     res.status(201).json({ success: true, message: 'Contact saved successfully!' });

//   } catch (err) {
//     console.error('Error saving contact:', err);
//     res.status(500).json({ error: 'Server error. Please try again later.' });
//   }

// })


// app.post("/ads/couns2",async (req,res)=>{

//   try{
//     const {userName , collegename , studyyear , duration , course , couns, programType } = req.body;
//     const tel125 = req.body['tel-125']; 
//     console.log(req.body)
//     if (!tel125) {
//       return res.status(400).json({ message: "Phone number is required." });
//     }

//     // ✅ 2. Remove non-digit characters and validate 10 digits
//     const sanitizedNumber = tel125.replace(/\D/g, "");


//     // ✅ 3. Ensure the number includes +91 prefix
//     const fullNumber = cleanIndianMobileNumber(sanitizedNumber)

//     // ✅ 4. Check if user already exists
//     let userChat = await Chat.findOne({ userNumber: fullNumber });
//     if (userChat) {
//       return res.status(400).json({ message: "User already exists." });
//     }

//     // ✅ 5. Create and save new user
//     userChat = new Chat({
//       userNumber: fullNumber,
//       userName: userName?.trim() || "Unknown",
//       conversations: [],
//        course: course,
//         lastInteracted: new Date(),
//         programType : programType,
//         leadfrom: detectLeadFrom(req.body),
//     });

//     await userChat.save();


//     // ✅ 6. Notify via WebSocket (if configured)
//     broadcast({ type: "new_user", user: userChat });

//     // ✅ 7. Add an initial conversation so the lead isn't sitting on an empty
//     // conversation history - otherwise the AI follow-up cron has no context to
//     // work with and falls back to the generic "Just checking in" message.
//     userChat.conversations.push({
//       userMsg: `Hi, I'm ${userName || "there"} from ${collegename || "Not mentioned"} of ${studyyear || "Not mentioned"}. I was interested in ${course || "the course"} for ${duration || "Not mentioned"} (${programType || "Not mentioned"} program)`,
//       botReply: "Lead captured from Google Ads form.",
//     });

//     // await sendDynamicMessage(fullNumber,followUp)
//     // await sendfixedtemplate2(fullNumber)

//     await userChat.save();

//     res.status(201).json({ success: true, message: 'Contact saved successfully!' });

//   } catch (err) {
//     console.error('Error saving contact:', err);
//     res.status(500).json({ error: 'Server error. Please try again later.' });
//   }
// })

// app.post("/ads/couns3",async (req,res)=>{

//   try{
//     const {userName , collegename , studyyear , duration , course , couns, programType } = req.body;
//     const tel125 = req.body['tel-125']; 
//     console.log(req.body)
//     if (!tel125) {
//       return res.status(400).json({ message: "Phone number is required." });
//     }

//     // ✅ 2. Remove non-digit characters and validate 10 digits
//     const sanitizedNumber = tel125.replace(/\D/g, "");


//     // ✅ 3. Ensure the number includes +91 prefix
//     const fullNumber = cleanIndianMobileNumber(sanitizedNumber)

//     // ✅ 4. Check if user already exists
//     let userChat = await Chat.findOne({ userNumber: fullNumber });
//     if (userChat) {
//       return res.status(400).json({ message: "User already exists." });
//     }

//     // ✅ 5. Create and save new user
//     userChat = new Chat({
//       userNumber: fullNumber,
//       userName: userName?.trim() || "Unknown",
//       conversations: [],
//        course: course,
//         lastInteracted: new Date(),
//         programType : programType,
//         leadfrom: "Meta ads",
//     });

//     await userChat.save();


//     // ✅ 6. Notify via WebSocket (if configured)
//     broadcast({ type: "new_user", user: userChat });

//     // ✅ 7. Add an initial conversation so the lead isn't sitting on an empty
//     // conversation history - otherwise the AI follow-up cron has no context to
//     // work with and falls back to the generic "Just checking in" message.
//     userChat.conversations.push({
//       userMsg: `Hi, I'm ${userName || "there"} from ${collegename || "Not mentioned"} of ${studyyear || "Not mentioned"}. I was interested in ${course || "the course"} for ${duration || "Not mentioned"} (${programType || "Not mentioned"} program)`,
//       botReply: "Lead captured from Meta Ads form.",
//     });

//     // await sendDynamicMessage(fullNumber,followUp)
//     // await sendfixedtemplate2(fullNumber)

//     await userChat.save();

//     res.status(201).json({ success: true, message: 'Contact saved successfully!' });

//   } catch (err) {
//     console.error('Error saving contact:', err);
//     res.status(500).json({ error: 'Server error. Please try again later.' });
//   }
// })



// async function sendEnrollmentTemplate(to, studentName, courseName, pdfUrl) {
//   try {
//     to = cleanIndianMobileNumber(to);
//     console.log("Sending to:", to);

//     const response = await axios.post(
//       `https://graph.facebook.com/v22.0/${PHONE_NUMBER_ID}/messages`,
//       {
//         messaging_product: "whatsapp",
//         to,
//         type: "template",
//         template: {
//           name: "course_enroll_cert", // your approved template name
//           language: { code: "en" },
//           components: [
//             // Header — PDF via URL
//             {
//               type: "header",
//               parameters: [
//                 {
//                   type: "document",
//                   document: {
//                     link: pdfUrl,
//                     filename: "Enrollment_Certificate.pdf",
//                   },
//                 },
//               ],
//             },
//             // Body — {{1}} name, {{2}} course
//             {
//               type: "body",
//               parameters: [
//                 { type: "text", text: studentName },
//                 { type: "text", text: courseName },
//               ],
//             },
//           ],
//         },
//       },
//       {
//         headers: {
//           Authorization: `Bearer ${ACCESS_TOKEN}`,
//           "Content-Type": "application/json",
//         },
//       }
//     );

//     console.log("✅ Enrollment message sent successfully:", response.data);
//     return response.data;
//   } catch (error) {
//     console.error("❌ Error sending enrollment template:", error.response?.data || error.message);
//     throw new Error(error.response?.data?.error?.message || error.message);
//   }
// }


async function handlepage(body){
  // const body = req.body;
  // console.log("📩 Received webhook:", JSON.stringify(body, null, 2));

  // Always respond 200 quickly so Meta doesn't retry
  // res.sendStatus(200);

  try {
    if (body.object === "page") {
      body.entry.forEach(async (entry) => {
        const changes = entry.changes || [];
        for (const change of changes) {
          if (change.field === "leadgen" && change.value && change.value.leadgen_id) {
            const leadgen_id = change.value.leadgen_id;
            console.log(`🆕 New leadgen_id: ${leadgen_id}`);

            // Make Graph API call to fetch lead data
            const url = `https://graph.facebook.com/v24.0/${leadgen_id}?access_token=${PAGE_ACCESS_TOKEN}`;

            try {
              const response = await axios.get(url);
              console.log("✅ Lead details:", JSON.stringify(response.data, null, 2));
              sendlead(response.data)
              // You can now handle this data, e.g., save to DB or send to Google Sheets 919343037478
            } catch (err) {
              console.error("❌ Error fetching lead details:", err.response?.data || err.message);
            }
          }
        }
      });
    }
  } catch (err) {
    console.error("❌ Webhook processing error:", err);
  }
}


async function sendlead(leadData) {
  try {
    // Extract lead info nicely
    const fields = leadData.field_data || [];
    const details = {};
    fields.forEach(f => {
      details[f.name] = f.values?.[0] || "";
    });

const leadnum = `${details["phone_number"] || details["phone"] || details["mobile"] ||  "N/A"}`
const userName = `${details["first_name"] || details["full_name"] || "N/A"}`
const location = `${details["city"] || "N/A"}`
 const sanitizedNumber = leadnum.replace(/\D/g, "");

    // ✅ 3. Ensure the number includes +91 prefix
    const fullNumber = cleanIndianMobileNumber(sanitizedNumber)

    // ✅ 4. Check if user already exists
    let userChat = await Chat.findOne({ userNumber: fullNumber });
    if (userChat) {
      // Backfill the Meta lead_id if this number came back in without one on file
      // (e.g. resubmitted the form) — needed later for Conversions API "Qualified" events.
      if (!userChat.leadgenId && leadData.id) {
        userChat.leadgenId = leadData.id;
        await userChat.save();
      }
      return
    }

    // ✅ 5. Create and save new user
    userChat = new Chat({
      userNumber: fullNumber,
      userName: userName || "Unknown",
      location: location || "N/A",
      conversations: [],
      lastInteracted: new Date(),
      leadfrom: "Meta Ads",
      leadgenId: leadData.id || null,
    });

    await userChat.save();


    // ✅ 6. Notify via WebSocket (if configured)
    broadcast({ type: "new_user", user: userChat });

    // ✅ 7. Add an initial conversation
    const followUp = `Hi ${userName || "there"}! 👋 Thanks for showing interest in Beta Space. We’re excited to have you on board and can’t wait to help you kickstart your working journey! 🚀`;

    // await sendDynamicMessage(fullNumber,followUp)
    sendfixedtemplate2(fullNumber)
const userMsg = `
Hi, I'm ${userName || "there"}

Here are my details:
${Object.entries(details)
  .map(([key, value]) => `- ${key}: ${value || "N/A"}`)
  .join("\n")}
`.trim();

userChat.conversations.push({
  userMsg,
  botReply: followUp,
});

await userChat.save();

  } catch (err) {
    console.error("❌ Error sending WhatsApp message:", err.response?.data || err.message);
  }
}

async function interactivechat(userMessages) {
    // Combine last 5 messages (or all if less than 5)
    const lastMessages = userMessages.slice(-5).join("\n");

    const systemPrompt = `
You are a friendly and professional sales assistant. 
Use the customer's previous chat history to write a personalized sales follow-up message. 

Your message should:
- Sound warm, polite, and natural (not robotic).
- Gently remind the customer about the product or service discussed.
- Express genuine interest in helping them make the best choice.
- Avoid sounding pushy or overly salesy.
- End with an inviting question or next step (e.g., offering assistance, scheduling a call, or sharing more details).

Now, based on the previous conversation, write a warm and polite follow-up message.
`;

    const response = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: `Here are the last 5 user messages:\n${lastMessages}` }
        ]
    });

    return response.choices[0].message.content.trim();
}


async function sendInitialTwoButtons(to){
  try{
    await axios.post(
      `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product:"whatsapp",
        to,
        type:"interactive",
        interactive:{
          type:"button",
          body:{ text:"Please select to continue 👇" },
          action:{
            buttons:[
              {
                type:"reply",
                reply:{ id:"select_job", title:"Job Courses" }
              },
              {
                type:"reply",
                reply:{ id:"select_internship", title:"College Internship" }
              }
            ]
          }
        }
      },
      {
        headers:{
          Authorization:`Bearer ${ACCESS_TOKEN}`,
          "Content-Type":"application/json",
        }
      }
    );
  }catch(err){
    console.log("Error sending 2-btn", err.response?.data || err);
  }
}



async function sendCourseSelectionButtons(to) {
  try {
    const response = await axios.post(
      `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "interactive",
        interactive: {
          type: "list",
          header: {
            type: "text",
            text: "📚 Please select location to enquire",
          },
          body: {
            text: "👋 Hello! I’m Beta Assistant from BetaSpace, Coimbatore. I’m here to help you find the perfect workspace for your needs! 🏢✨"
          },
          footer: {
            text: "Pick one option",
          },
          action: {
            button: "Select Location",
            sections: [
              {
                // title: "Inclusive of AI",
                rows: [
                    {
                    id: "Ram_Nagar",
                    title: "Ram Nagar",
                  },
                  {
                    id: "Singanallur",
                    title: "Singanallur",
                    // description: "Django + React with AI",
                  },
                  {
                    id: "Koundampalayam",
                    title: "Koundampalayam",
                    // description: "Springboot with AI",
                  },
                  {
                    id: "Neelikonampalayam",
                    title: "Neelikonampalayam",
                    // description: "React + Node with AI",
                  }

                ],
              },
            ],
          },
        },
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    // console.log("✅ Sent course list successfully:", response.data);
  } catch (error) {
    console.error("❌ Error sending course list:", error.response?.data || error);
  }
}


// async function sendProfessionButtons(to) {
//   const data = {
//     messaging_product: "whatsapp",
//     to,
//     type: "interactive",
//     interactive: {
//       type: "button",
//       body: { text: "Please select your profession:" },
//       action: {
//         buttons: [
//           { type: "reply", reply: { id: "student", title: "Student" } },
//            { type: "reply", reply: { id: "working", title: "Job Seeker" } },
//           { type: "reply", reply: { id: "career", title: "Carrer switch" } }
//         ]
//       }
//     }
//   };
//   await axios.post(`https://graph.facebook.com/v20.0/${process.env.PHONE_NUMBER_ID}/messages`, data, {
//     headers: {
//       Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`,
//       "Content-Type": "application/json",
//     },
//   });
// }


app.get("/users", async (req, res) => {
  try {
    const users = await Chat.find({});
    res.json(users);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Server error" });
  }
});

// Upload and send messages
app.post("/api/uploadExcel", async (req, res) => {
  try {
    const { data, messageTemplate } = req.body;

    if (!Array.isArray(data) || !data.length) {
      return res.status(400).json({ message: "No data found" });
    }

    const formattedData = data.map((item) => ({
      userName: item.full_name || item.Name,
      userNumber: item.phone || item.Number,
      leadfrom: item.platform || item.Source,
      course: item.course || null,
      leadStatus: "New",
      assignedto: null,
    }));

    // ✅ Send WhatsApp messages in sequence with small delay
    for (let i = 0; i < formattedData.length; i++) {
      const { userName, userNumber } = formattedData[i];

      // Skip invalid numbers
      // if (!userNumber || userNumber.toString().length < 10) continue;

      // Custom message (optional)
      const message = messageTemplate
        ? messageTemplate.replace("{name}", userName || "there")
        : `Hello ${userName || "there"}, thank you for your interest!`;

      await sendWhatsAppMessage(userNumber, message);

      // Add small delay (Meta recommends < 20 msg/sec)
      await new Promise((r) => setTimeout(r, 500));
    }

    res.status(200).json({ message: "Messages sent successfully!" });
  } catch (err) {
    console.error("❌ Error in /api/uploadExcel:", err);
    res.status(500).json({ message: "Server error" });
  }
});

const companyUrls = [
"https://betaspace.in/knowledge-base/"
];

let dynamicCompanyInfo = ""; // store the fetched data

app.post("/send",async (req,res)=>{
 const {to , message} = req.body
 await sendWhatsAppMessage(to,message)
 res.send({mess : "Sent"})
})

app.post("/send-template", async (req, res) => {
  try {
    const { to } = req.body; // expecting JSON like { "to": "918xxxxxxxxx" }

    const response = await axios.post(
       `https://graph.facebook.com/v24.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "template",
        template: {
          name: "welcome_betaspace",     // default pre-approved template
          language: { code: "en" }
        }
      },
      {
        headers: {
          "Authorization": `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json"
        }
      }
    );

    res.json({
      success: true,
      messageId: response.data.messages[0].id,
      info: "Template message sent successfully"
    });
  } catch (error) {
    console.error("❌ Error sending template:", error.response?.data || error.message);
    res.status(500).json({
      success: false,
      error: error.response?.data || error.message
    });
  }
});


app.post("/send-text", async (req, res) => {
  try {
    const { to, param1 } = req.body; 
    // Expecting body like: { "to": "918xxxxxxxxx", "param1": "Sharan" }

    const response = await axios.post(
      `https://graph.facebook.com/v24.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "template",
        template: {
          name: "text_msg", // Your template name
          language: { code: "en" },
          components: [
            {
              type: "body",
              parameters: [
                { type: "text", text: param1 } // replaces {1}
              ]
            }
          ]
        }
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json"
        }
      }
    );

    res.json({
      success: true,
      messageId: response.data.messages[0].id,
      info: "Template message sent successfully"
    });
  } catch (error) {
    console.error("❌ Error sending template:", error.response?.data || error.message);
    res.status(500).json({
      success: false,
      error: error.response?.data || error.message
    });
  }
});


app.post("/sendFlow", async (req, res) => {
  try {
    const { to } = req.body; // e.g. "919876543210"

    const response = await axios.post(
      `https://graph.facebook.com/v21.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "interactive",
        interactive: {
          type: "flow",
          header: {
            type: "text",
            text: "Feedback Form",
          },
          body: {
            text: "We’d love your feedback! Tap below to share your thoughts.",
          },
          footer: {
            text: "Powered by Beta Space",
          },
          action: {
            name: "flow",
            parameters: {
              flow_message_version: "3",
              flow_id: "25157053150579613", // string ✅
              flow_cta: "Start Feedback",
              flow_action: "navigate",
              flow_action_payload: {
                screen: "RECOMMEND",
              },
            },
          },
        },
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    res.json({
      success: true,
      message: "Flow sent successfully!",
      data: response.data,
    });
  } catch (error) {
    console.error(
      "❌ Error sending flow:",
      error.response?.data || error.message
    );
    res.status(500).json({
      success: false,
      error: error.response?.data || error.message,
    });
  }
});

async function sendDynamicMessage(to, param1) {
  try {
    to = cleanIndianMobileNumber(to)
    param1 = cleanWhatsAppText(param1)
    console.log("Sending to:", to, "with param:", param1);

    const response = await axios.post(
      `https://graph.facebook.com/v24.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "template",
        template: {
          name: "text_msg", // default pre-approved template name
          language: { code: "en" },
          // ✅ Optional: pass dynamic parameters if your template supports them
          components: [
            {
              type: "body",
              parameters: [{ type: "text", text: param1 }],
            },
          ],
        },
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    console.log("✅ Message sent successfully:", response.data);
    return response.data;
  } catch (error) {
    console.error("❌ Error sending template:", error.response?.data || error.message);
    // Re-throw the error so the calling function can handle it
    throw new Error(error.response?.data?.error?.message || error.message);
  }
}


async function sendfixedtemplate (to) {
  try {
    to = cleanIndianMobileNumber(to)
    console.log("Sending to:", to);

    const response = await axios.post(
      `https://graph.facebook.com/v22.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "template",
        template: {
          name: "welcome_betaspace", // default pre-approved template name
          language: { code: "en" },
          // ✅ Optional: pass dynamic parameters if your template supports them
          components: [
            {
              type: "body",
              parameters: [],
            },
          ],
        },
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    console.log("✅ Message sent successfully:", response.data);
    return response.data;
  } catch (error) {
    console.error("❌ Error sending template:", error.response?.data || error.message);
    // Re-throw the error so the calling function can handle it
    throw new Error(error.response?.data?.error?.message || error.message);
  }
}



async function sendfixedtemplate2 (to) {
  try {
    to = cleanIndianMobileNumber(to)
    console.log("Sending to:", to);

    const response = await axios.post(
      `https://graph.facebook.com/v22.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "template",
        template: {
          name: "received_form", // default pre-approved template name
          language: { code: "en" },
          // ✅ Optional: pass dynamic parameters if your template supports them
          components: [
            {
              type: "body",
              parameters: [],
            },
          ],
        },
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    console.log("✅ Message sent successfully:", response.data);
    return response.data;
  } catch (error) {
    console.error("❌ Error sending template:", error.response?.data || error.message);
    // Re-throw the error so the calling function can handle it
    throw new Error(error.response?.data?.error?.message || error.message);
  }
}


// ✅ POST /add route
app.post("/add", async (req, res) => {
  try {
    const { userName, userNumber, course, city, leadfrom } = req.body;

    // ✅ 1. Validate phone number presence
    if (!userNumber) {
      return res.status(400).json({ message: "Phone number is required." });
    }

    // ✅ 2. Remove non-digit characters and validate 10 digits
    const sanitizedNumber = userNumber.replace(/\D/g, "");
   
    // ✅ 3. Ensure the number includes +91 prefix
    const fullNumber = cleanIndianMobileNumber(sanitizedNumber)

    // ✅ 4. Check if user already exists
    let userChat = await Chat.findOne({ userNumber: fullNumber });
    if (userChat) {
      return res.status(400).json({ message: "User already exists." });
    }

    // ✅ 5. Create and save new user
    userChat = new Chat({
      userNumber: fullNumber,
      userName: userName?.trim() || "Unknown",
      conversations: [],
      courseSelected: course || null,
      lastInteracted: new Date(),
      leadfrom: leadfrom || "Manual Entry",
      city: city?.trim() || null,
    });

    await userChat.save();

    // ✅ 6. Notify via WebSocket (if configured)
    broadcast({ type: "new_user", user: userChat });

    // ✅ 7. Send welcome message
    const mockMsg = `Hi ${userName || "there"}, Welcome to Beta Space! Im Beta Assistant from BetaSpace, Coimbatore.`;
    // await sendDynamicMessage(fullNumber, mockMsg);

    // ✅ 8. Respond success
    return res.status(200).json({
      success: true,
      message: "Lead added successfully.",
      user: userChat,
    });
  } catch (err) {
    console.error("❌ Error adding user:", err);
    return res.status(500).json({
      message: "Internal Server Error. Please try again later.",
    });
  }
});

async function sendWhatsAppMessage(number, message) {
  try {
    await axios.post(
      `https://graph.facebook.com/v24.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to: number,
        text: { body: message }
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json"
        }
      }
    );
    console.log(`📩 Sent message to ${number}`);
  } catch (err) {
    console.error(`⚠️ Failed to send message to ${number}:`, err.response?.data || err.message);
  }
}

async function generateFollowUp(userMessages, course, counsellorRemarks = []) {
  if (!course) return null;
  const workspaceType = course; // the CRM "course" field now holds the lead's workspace interest

const systemPrompt = `
You are Beta Assistant, a friendly and professional workspace consultant from BetaSpace, Coimbatore.
Your job is to create a short follow-up message to re-engage leads and gently guide them toward the next step in choosing a workspace at BetaSpace.

Guidelines:
1. Always start the message with a cheerful morning greeting such as "Good morning", "Happy morning", or "Good day", since this message is sent around 8 AM.
2. Always prioritize the lead's workspace interest: "${workspaceType}".
3. Use the last 5 user messages to understand their tone, requirements (business type, team size, location, budget, duration), and intent, and follow up naturally.
4. Keep the tone warm, polite, and helpful. Never pushy or robotic.
5. Encourage one small action: scheduling a site visit, asking for pricing, or clarifying doubts. Do not ask for more than one thing.
6. Briefly mention one relevant benefit (like a ready-to-use office, flexible terms, lower setup cost than a traditional office, or a convenient location) when appropriate. Mention only benefits that are in the verified company information.
7. Keep it short (2-3 sentences) and conversational.
8. If other workspace types are mentioned, acknowledge them but focus mainly on "${workspaceType}".
9. Never reveal that you are an AI. Say you are Beta Assistant from the BetaSpace team.
10. If counsellor remarks are provided below, they capture concerns, objections, or hesitations the lead raised during calls. Directly and empathetically address those specific issues in the message rather than using generic phrasing.
11. Where relevant, highlight how the right workspace can support the lead's business growth, such as room to scale the team, a professional business address, or networking with other members.
12. Never invent availability, prices, discounts, facilities, or booking confirmations. If you are unsure about a detail, offer to confirm it with the BetaSpace team.
`;

  const remarksText = counsellorRemarks.length > 0
    ? counsellorRemarks.join("\n")
    : "No counsellor remarks available.";

  const response = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      { role: "system", content: systemPrompt },
      { role: "user", content: `Here are the last 5 user messages:\n${userMessages.join("\n")}\n\nHere are the counsellor's remarks from past interactions:\n${remarksText}` }
    ]
  });

  return response.choices[0].message.content.trim();
}

// Angles the 4th-day follow-up is allowed to research and draw on. The model picks
// whichever 1-2 are most persuasive for a given lead rather than cramming all of them
// into one WhatsApp message.
const DAY4_FOLLOWUP_TOPICS = [
  "benefits of the workspace type they selected",
  "cost savings of a flexible workspace compared with a traditional office",
  "how a ready-to-use workspace helps them start working faster",
  "about BetaSpace and what we offer",
  "how we are better than other workspace providers",
  "facilities and amenities included (Wi-Fi, power backup, meeting rooms, etc.)",
  "scope for scaling the team and moving to a bigger space later",
  "success stories and testimonials from our members",
  "case studies of startups and businesses working from BetaSpace",
  "networking and collaboration opportunities with other members",
  "benefits of a professional business address and virtual office",
  "how big companies and startups are adopting flexible workspaces and hybrid work",
  "workspace as an investment in productivity, not an expense",
  "how Coimbatore is growing as a business and startup hub",
];

// 4th-day morning follow-up: unlike generateFollowUp (days 1-3), this one is not allowed
// to be generic. It uses OpenAI's web_search tool to look up real, current information on
// one of the DAY4_FOLLOWUP_TOPICS angles for the lead's course, then writes a short,
// specific WhatsApp message grounded in what it actually found (no invented stats/links).
async function generateDay4FollowUp(userName, course, userMessages, counsellorRemarks = []) {
  if (!course) return null;
  const workspaceType = course; // the CRM "course" field now holds the lead's workspace interest

  const remarksText = counsellorRemarks.length > 0
    ? counsellorRemarks.join("\n")
    : "No counsellor remarks available.";

 const instructions = `
You are Beta Assistant, a friendly and professional workspace consultant from BetaSpace, Coimbatore.

This is a 4th-day morning follow-up to a lead who has not replied to the last 3 daily follow-ups. It must NOT be generic or repeat generic encouragement. It must feel researched and specific.

Use the web_search tool to look up current, real information about "${workspaceType}" and flexible workspaces on ONE OR TWO of these angles (pick whichever will be most persuasive for this specific lead, based on their messages and any counsellor remarks below):
${DAY4_FOLLOWUP_TOPICS.map(t => `- ${t}`).join("\n")}

Guidelines:
1. Start with a cheerful morning greeting (this is sent around 10 AM).
2. Ground the message in specific, real information you found via web search (a real statistic, cost comparison, coworking trend, testimonial, or fact). Do not invent statistics, testimonials, prices, or links.
3. If you use a statistics or trends angle, include the real source URL you found for it.
4. If you use the "about BetaSpace" or "better than other providers" angle, base it only on what you actually find about BetaSpace and the local Coimbatore workspace market. Do not disparage named competitors, just highlight our genuine strengths.
5. If counsellor remarks are provided, they capture concerns or objections the lead raised. Address those specifically if relevant to the angle you pick.
6. Keep the tone warm, polite, and helpful. Never pushy or robotic.
7. HARD LIMIT: the entire message must be under 500 characters (roughly 3-4 short sentences). WhatsApp will reject longer messages, so do not exceed this no matter how much you found. Pick the single best fact and end with one clear next step (e.g. reply to schedule a site visit, ask for pricing, or ask a question).
8. Never reveal you are an AI model. You are Beta Assistant from the BetaSpace team.
9. Always prioritize the lead's workspace interest: "${workspaceType}".
10. Never invent availability, discounts, or booking confirmations. If unsure about a detail, offer to confirm it with the BetaSpace team.
`;

  const response = await openai.responses.create({
    model: "gpt-4o-mini",
    tools: [
      {
        type: "web_search",
        user_location: { type: "approximate", country: "IN", region: "Tamil Nadu", city: "Coimbatore" },
      },
    ],
    instructions,
    input: `Lead name: ${userName || "there"}
Selected course: ${course}
Last user messages:
${userMessages.join("\n") || "None"}

Counsellor remarks (objections/concerns raised, if any):
${remarksText}`,
  });

  const text = response.output_text?.trim();
  return text ? truncateForTemplateParam(text) : null;
}

// WhatsApp template body parameters are capped (~1024 chars) - sending longer text fails
// with "(#132005) Translated text too long". The day-4 message is prompted to stay under
// 500 chars, but this is a hard safety net in case the model overruns anyway: trims at the
// nearest sentence boundary (falling back to a word boundary) so it never gets cut mid-word.
function truncateForTemplateParam(text, maxLen = 900) {
  if (!text || text.length <= maxLen) return text;

  const cut = text.slice(0, maxLen);
  const sentenceEnd = Math.max(cut.lastIndexOf(". "), cut.lastIndexOf("! "), cut.lastIndexOf("? "));
  if (sentenceEnd > maxLen * 0.4) {
    return cut.slice(0, sentenceEnd + 1).trim();
  }

  const wordEnd = cut.lastIndexOf(" ");
  return `${(wordEnd > 0 ? cut.slice(0, wordEnd) : cut).trim()}...`;
}

// Cron job every minute
cron.schedule("0 10 * * *", async () => {
  console.log("🔄 Running follow-up cronjob...");

  const cutoff = new Date(Date.now() - 1 * 60 * 1000); // 1 min ago
  const inactiveUsers = await Chat.find({
    lastInteracted: { $lt: cutoff },
    followUpCount: { $lt: 3 },
    pipeline: { $not: /^(Converted|Enrolled|Irrelevant|Not Interested|Not Intrested|On Hold)$/i }
  });

  for (const user of inactiveUsers) {
    const whatsappNumber = user.userNumber;

    try {
      if (!whatsappNumber) {
        throw new Error("Missing WhatsApp number");
      }

      if (!user.course) {
        // sendCourseSelectionButtons(whatsappNumber)
        await sendfixedtemplate(whatsappNumber)
        // await sendWhatsAppMessage(whatsappNumber, initialMessage);
        user.conversations.push({
        userMsg: `Follow-up #${user.followUpCount + 1} sent`,
        botReply: `👋 Hello! I'm Beta Assistant, your workspace consultant from BetaSpace, Coimbatore. Say "Hi" to know more about me and the workspaces we offer`,
      });
      } else {
        const lastMessages = user.conversations.slice(-5).map(c => c.userMsg);
        const counsellorRemarks = (user.remarks || []).map(r => r.remark).filter(Boolean);

        // Always try the AI-personalized message first - generateFollowUp works fine even
        // with an empty lastMessages array (it still has the course to build on), so there's
        // no need to skip straight to the generic fallback just because there's no chat
        // history yet (e.g. leads that came in via an ad form and never replied).
        // GPT calls occasionally fail transiently (timeouts/rate limits), so retry once
        // before giving up and using the generic fallback line.
        let followUp;
        try {
          followUp = await generateFollowUp(lastMessages, user.course, counsellorRemarks);
        } catch (err) {
          console.error(`⚠️ GPT error for ${user.userNumber} (attempt 1):`, err.message);
          try {
            followUp = await generateFollowUp(lastMessages, user.course, counsellorRemarks);
          } catch (err2) {
            console.error(`⚠️ GPT error for ${user.userNumber} (attempt 2):`, err2.message);
            followUp = "Hi! Just checking in to see if you have any questions or need assistance. We're here to help!";
          }
        }

        // await sendWhatsAppMessage(whatsappNumber, followUp);
        await sendDynamicMessage(whatsappNumber,followUp)
        console.log(`📩 Sent follow-up #${user.followUpCount + 1} to ${user.userNumber}`);
            user.conversations.push({
        userMsg: `Follow-up #${user.followUpCount + 1} sent`,
        botReply: followUp,
      });
      }
      user.followUpCount = (user.followUpCount || 0) + 1;
      // user.lastInteracted = new Date();
       user.lastFollowUpSentAt = new Date();
       user.followUpAwaitingReply = true;
      await user.save();
    } catch (err) {
      // Bad record (invalid number, WhatsApp API rejection, etc.) - log it and move on
      // so one bad lead doesn't block the rest of the batch from getting their follow-up.
      console.error(`❌ Follow-up failed for ${user.userNumber || user._id}:`, err.message);
      try {
        user.conversations.push({
          userMsg: `Follow-up #${user.followUpCount + 1} attempt failed`,
          botReply: `⚠️ Skipped: ${err.message}`,
        });
        await user.save();
      } catch (saveErr) {
        console.error(`❌ Also failed to save error note for ${user.userNumber || user._id}:`, saveErr.message);
      }
    }
  }
},{
    timezone: "Asia/Kolkata", // 👈 add this line to fix time zone to IST
  }
);

// 4th-day morning follow-up (web-search based, non-generic). Runs separately from the
// 10 AM cron above and only picks up leads that finished that 3-day cycle
// (followUpCount === 3) without replying and haven't had this sent yet, so it never
// overlaps with the days 1-3 follow-ups or the 5 PM evening cron.
// Restricted to leads created within the last 5 days so it never fires on older leads
// (e.g. ones that were already sitting at followUpCount === 3 before this feature existed).
cron.schedule("15 10 * * *", async () => {
  console.log("🔄 Running 4th-day (web-search) follow-up cronjob...");

  const cutoff = new Date(Date.now() - 1 * 60 * 1000); // 1 min ago
  const fiveDaysAgo = new Date(Date.now() - 5 * 24 * 60 * 60 * 1000);
  const day4Users = await Chat.find({
    lastInteracted: { $lt: cutoff },
    datecreated: { $gte: fiveDaysAgo },
    followUpCount: 3,
    day4FollowUpSent: { $ne: true },
    course: { $ne: null },
    pipeline: { $not: /^(Converted|Enrolled|Irrelevant|Not Interested|Not Intrested|On Hold)$/i }
  });

  for (const user of day4Users) {
    const whatsappNumber = user.userNumber;

    try {
      if (!whatsappNumber) {
        throw new Error("Missing WhatsApp number");
      }

      const lastMessages = user.conversations.slice(-5).map(c => c.userMsg);
      const counsellorRemarks = (user.remarks || []).map(r => r.remark).filter(Boolean);

      let followUp;
      try {
        followUp = await generateDay4FollowUp(user.userName, user.course, lastMessages, counsellorRemarks);
      } catch (err) {
        console.error(`⚠️ Day-4 web-search follow-up GPT error for ${user.userNumber}:`, err.message);
        followUp = null;
      }

      if (!followUp) {
        // Web search / GPT failed - fall back to the same style of message used on days
        // 1-3 rather than skip the lead's 4th-day follow-up entirely.
        try {
          followUp = await generateFollowUp(lastMessages, user.course, counsellorRemarks);
        } catch (err) {
          followUp = "Hi! Just checking in to see if you have any questions or need assistance. We're here to help!";
        }
      }

      await sendDynamicMessage(whatsappNumber, followUp);
      console.log(`📩 Sent 4th-day follow-up to ${user.userNumber}`);

      user.conversations.push({
        userMsg: `Follow-up #4 (day-4 web-search) sent`,
        botReply: followUp,
      });

      user.followUpCount = 4;
      user.day4FollowUpSent = true;
      user.lastFollowUpSentAt = new Date();
      user.followUpAwaitingReply = true;
      await user.save();
    } catch (err) {
      // Bad record (invalid number, WhatsApp API rejection, etc.) - log it and move on
      // so one bad lead doesn't block the rest of the batch from getting their follow-up.
      console.error(`❌ Day-4 follow-up failed for ${user.userNumber || user._id}:`, err.message);
      try {
        user.conversations.push({
          userMsg: `Follow-up #4 (day-4 web-search) attempt failed`,
          botReply: `⚠️ Skipped: ${err.message}`,
        });
        await user.save();
      } catch (saveErr) {
        console.error(`❌ Also failed to save error note for ${user.userNumber || user._id}:`, saveErr.message);
      }
    }
  }
}, {
  timezone: "Asia/Kolkata",
});

// Purge un-replied bulk-campaign TempLead prospects older than 7 days.
// Replied prospects are already deleted at promotion time (see /webhook), so
// this only ever touches numbers that never responded.
cron.schedule("0 2 * * *", async () => {
  try {
    const cutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    const result = await TempLead.deleteMany({ sentAt: { $lt: cutoff } });
    if (result.deletedCount) {
      console.log(`🧹 Purged ${result.deletedCount} stale TempLead prospect(s)`);
    }
  } catch (err) {
    console.error("❌ TempLead cleanup failed:", err.message);
  }
}, {
  timezone: "Asia/Kolkata",
});



cron.schedule(
  "30 10 * * *", // 10:00 AM IST
  async () => {
    try {
      console.log("🔄 Running grouped reminder cron...");

const now = new Date();

// Start of today (IST)
const startOfDay = new Date(now);
startOfDay.setHours(0, 0, 0, 0);

// End of today (IST)
const endOfDay = new Date(now);
endOfDay.setHours(23, 59, 59, 999);

      // 🔹 Group chats by assigned user
      const groupedChats = await Chat.aggregate([
        {
          $match: {
            reminder: { $gte: startOfDay, $lte: endOfDay },
            assignedto: { $ne: null },
          },
        },
        {
          $group: {
            _id: "$assignedto", // assigned user's number
            leads: {
              $push: {
                userName: "$userName",
                userNumber: "$userNumber",
                course: "$course",
                remarks: "$remarks",
                pipeline : "$pipeline"
              },
            },
          },
        },
      ]);

      console.log(`📌 Found ${groupedChats.length} users to notify`);

      for (const group of groupedChats) {
        const assignedUserName = group._id;

        // 🔹 Fetch assigned user
         const assignedUser = await User.findOne({
    username: assignedUserName,
  });


        if (!assignedUser) {
          console.warn(
            `⚠️ No user found for number: ${assignedUserNumber}`
          );
          continue;
        }

        // 🔹 Build grouped message
        let message = `🔔Good Morning ${assignedUser.username || "😊"},  *Today's Follow-up Reminders*\n\n`;

        group.leads.forEach((lead, index) => {
          const lastRemark =
            lead.remarks?.length > 0
              ? lead.remarks.at(-1).remark
              : "No remarks";

          message += `
${index + 1}. 👤 ${lead.userName || "Unknown"}
📞 ${lead.userNumber}
🎓 ${lead.course || "N/A"}
Pipeline -  ${lead.pipeline || "N/A"}
📝 ${lastRemark}

`;
        });

        message += `Please follow up today and update status in CRM`;

        // 🔹 Send WhatsApp
        await sendWhatsAppMessage(
          assignedUser.userNumber,
          message
        );

        console.log(
          `✅ Grouped reminder sent to ${assignedUser.username}`
        );
      }
    } catch (error) {
      console.error("❌ Grouped reminder cron error:", error);
    }
  },
  {
    timezone: "Asia/Kolkata",
  }
);


app.post("/login-log", async (req, res) => {
  try {
    const { username, loginTime, device } = req.body;

    // Convert to Indian date & time format
    const dateObj = new Date(loginTime);

    const formattedDate = dateObj.toLocaleDateString("en-IN", {
      day: "2-digit",
      month: "short",
      year: "numeric",
      timeZone: "Asia/Kolkata"
    });

    const formattedTime = dateObj.toLocaleTimeString("en-IN", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: true,
      timeZone: "Asia/Kolkata"
    });

    const login_msg = 
`🔐 *CRM Login Alert*

👤 User: ${username}
💻 Device: ${device}

📅 Date: ${formattedDate}
⏰ Time: ${formattedTime}

– CRM System`;

    console.log(login_msg);

    await sendMessage("919597271533", login_msg);

    res.json({ success: true });

  } catch (error) {
    console.error("Login log error:", error);
    res.status(500).json({ success: false });
  }
});

app.post("/new/enrollment", async (req, res) => {
  try {
    console.log(req.body)
    const { userId, userName, userNumber, course, programType , certificate } = req.body;

    if (!userId || !userNumber) {
      return res.status(400).json({ success: false, message: "Missing required fields" });
    }

    // Find user in DB
    const user = await Chat.findById(userId);

    if (!user) {
      return res.status(404).json({ success: false, message: "User not found" });
    }

    // Create message
    const msg = `🎉 *New Enrollment Alert*

👤 Name: ${userName}
📞 Number: ${userNumber}
📚 Course: ${course}
🎓 Program Type: ${programType}

A new user has enrolled successfully.`;


    const result = await sendEnrollmentTemplate(
      userNumber,
      userName,
      course,
      certificate?.url
    );

    res.json({
      success: true,
      message: "Enrollment notification sent"
    });

  } catch (error) {
    console.error("Enrollment API error:", error);
    res.status(500).json({
      success: false,
      message: "Server error"
    });
  }
});


// ---------------------------------------------------------------------------
// BATCH ASSIGNMENT NOTIFICATIONS
// ---------------------------------------------------------------------------
// Template: batchstudent
//   Raise in Meta with body params:
//     {{1}} = student name
//     {{2}} = trainer name
//     {{3}} = batch ID
//     {{4}} = batch date
//     {{5}} = course
//   Suggested body: "Hi {{1}}, your batch has been assigned! Your trainer is
//   {{2}}. Batch ID: {{3}}, Start Date: {{4}}, Course: {{5}}. We look forward
//   to seeing you in class!"
async function sendBatchStudentNotification(to, studentName, trainerName, batchId, batchDate, course) {
  try {
    to = cleanIndianMobileNumber(to);
    const response = await axios.post(
      `https://graph.facebook.com/v24.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "template",
        template: {
          name: "batchstudent",
          language: { code: "en" },
          components: [
            {
              type: "body",
              parameters: [
                { type: "text", text: studentName },  // {{1}}
                { type: "text", text: trainerName },  // {{2}}
                { type: "text", text: batchId },       // {{3}}
                { type: "text", text: batchDate || "TBD" }, // {{4}}
                { type: "text", text: course || "Your Course" }, // {{5}}
              ],
            },
          ],
        },
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );
    console.log("✅ Batch student notification sent:", response.data);
    return response.data;
  } catch (error) {
    console.error("❌ Error sending batchstudent template:", error.response?.data || error.message);
    throw new Error(error.response?.data?.error?.message || error.message);
  }
}

// Template: batchtrainer
//   Raise in Meta with body params:
//     {{1}} = trainer name
//     {{2}} = student name
//     {{3}} = batch ID
//     {{4}} = batch date
//     {{5}} = course
//   Suggested body: "Hi {{1}}, a new student has been assigned to your batch.
//   Student: {{2}}, Batch ID: {{3}}, Start Date: {{4}}, Course: {{5}}. Please
//   prepare accordingly!"
async function sendBatchTrainerNotification(to, trainerName, studentName, batchId, batchDate, course) {
  try {
    to = cleanIndianMobileNumber(to);
    const response = await axios.post(
      `https://graph.facebook.com/v24.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "template",
        template: {
          name: "batchtrainer",
          language: { code: "en" },
          components: [
            {
              type: "body",
              parameters: [
                { type: "text", text: trainerName },  // {{1}}
                { type: "text", text: studentName },  // {{2}}
                { type: "text", text: batchId },       // {{3}}
                { type: "text", text: batchDate || "TBD" }, // {{4}}
                { type: "text", text: course || "Your Course" }, // {{5}}
              ],
            },
          ],
        },
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );
    console.log("✅ Batch trainer notification sent:", response.data);
    return response.data;
  } catch (error) {
    console.error("❌ Error sending batchtrainer template:", error.response?.data || error.message);
    throw new Error(error.response?.data?.error?.message || error.message);
  }
}

// POST /new/batch-assigned
// Called by CRM-app-be when a batch is assigned to a student.
// Body: { studentNumber, studentName, trainerMobile, trainerName, batchId, batchDate, course }
app.post("/new/batch-assigned", async (req, res) => {
  try {
    const { studentNumber, studentName, trainerMobile, trainerName, batchId, batchDate, course } = req.body;

    if (!studentNumber || !trainerMobile) {
      return res.status(400).json({ success: false, message: "studentNumber and trainerMobile are required" });
    }

    // Send both notifications concurrently; failures are logged but won't crash each other
    const results = await Promise.allSettled([
      sendBatchStudentNotification(studentNumber, studentName || "Student", trainerName || "Trainer", batchId, batchDate, course),
      sendBatchTrainerNotification(trainerMobile, trainerName || "Trainer", studentName || "Student", batchId, batchDate, course),
    ]);

    const studentResult = results[0];
    const trainerResult = results[1];

    if (studentResult.status === "rejected") {
      console.error("Student WA notification failed:", studentResult.reason?.message);
    }
    if (trainerResult.status === "rejected") {
      console.error("Trainer WA notification failed:", trainerResult.reason?.message);
    }
    
    sendMessage("919597271533",`Hi ${ trainerName || "Trainer"}, a new student has been assigned to your batch.Student:  ${studentName || "Student"}, Batch ID: ${batchId}, Start Date: ${batchDate}, Course: ${course}. Please prepare accordingly!`)
    return res.json({
      success: true,
      message: "Batch assignment notifications processed",
      studentNotification: studentResult.status,
      trainerNotification: trainerResult.status,
    });
  } catch (error) {
    console.error("POST /new/batch-assigned error:", error);
    return res.status(500).json({ success: false, message: "Server error" });
  }
});


// ---------------------------------------------------------------------------
// NEW JOB POSTED — notify recently-enrolled students
// ---------------------------------------------------------------------------
// Template: new_job_posted
//   Raise in Meta with body params:
//     {{1}} = student name
//     {{2}} = job role
//     {{3}} = company
//     {{4}} = location
//   Body:
//     Hi {{1}},
//     A new job opportunity has been posted on the *Student Portal*.
//     💼 *Job Role:* {{2}}
//     🏢 *Company:* {{3}}
//     📍 *Location:* {{4}}
//     Please log in to the Workspace Portal to view the complete job details and apply.
//     Thank you,
//     *Appin Technology*
async function sendJobOpportunityNotification(to, studentName, jobRole, company, location) {
  to = cleanIndianMobileNumber(to);
  const response = await axios.post(
    `https://graph.facebook.com/v24.0/${PHONE_NUMBER_ID}/messages`,
    {
      messaging_product: "whatsapp",
      to,
      type: "template",
      template: {
        name: "new_job_posted",
        language: { code: "en" },
        components: [
          {
            type: "body",
            parameters: [
              { type: "text", text: cleanWhatsAppText(studentName) || "Student" },   // {{1}}
              { type: "text", text: cleanWhatsAppText(jobRole) || "New Role" },       // {{2}}
              { type: "text", text: cleanWhatsAppText(company) || "Not specified" },  // {{3}}
              { type: "text", text: cleanWhatsAppText(location) || "Not specified" }, // {{4}}
            ],
          },
        ],
      },
    },
    {
      headers: {
        Authorization: `Bearer ${ACCESS_TOKEN}`,
        "Content-Type": "application/json",
      },
    }
  );
  return response.data;
}

// POST /new/job-posted
// Called by Trainer-BE when a new job is added via POST /api/jobs. Trainer-BE
// has already picked the recipients (Enrolled + enrolled within 5 months); this
// endpoint only fires the template to each.
// Body: { job: { id, title, company, location }, recipients: [{ name, number }] }
app.post("/new/job-posted", async (req, res) => {
  try {
    const { job, recipients } = req.body || {};

    if (!job || !job.title || !Array.isArray(recipients) || recipients.length === 0) {
      return res.status(400).json({
        success: false,
        message: "job.title and a non-empty recipients array are required",
      });
    }

    // Respond right away — delivering to a large batch can take a while.
    res.json({
      success: true,
      message: `Dispatching "${job.title}" job alert to ${recipients.length} student(s)`,
      count: recipients.length,
    });

    let sent = 0;
    let failed = 0;
    for (const r of recipients) {
      if (!r || !r.number) { failed++; continue; }
      try {
        await sendJobOpportunityNotification(
          r.number,
          r.name || "Student",
          job.title,
          job.company,
          job.location
        );
        sent++;
        await new Promise((resolve) => setTimeout(resolve, 250)); // gentle pacing for WA rate limits
      } catch (err) {
        failed++;
        console.error(`❌ Job alert to ${r.number} failed:`, err.response?.data?.error?.message || err.message);
      }
    }
    await sendWhatsAppMessage(MANAGER_ADMIN_NUMBER, `✅ Job Posted "${job.title}" @ ${job.company || "N/A"} — sent ${sent}`);
    await sendWhatsAppMessage("917708171533", `✅ Job Posted "${job.title}" @ ${job.company || "N/A"} — sent ${sent}`);
    console.log(`✅ Job alert "${job.title}" @ ${job.company || "N/A"} — sent ${sent}, failed ${failed}`);
  } catch (error) {
    console.error("POST /new/job-posted error:", error);
    if (!res.headersSent) res.status(500).json({ success: false, message: "Server error" });
  }
});


async function Sendbulkimg(to, param1, imageUrl) {
  try {
    to = cleanIndianMobileNumber(to);

    const response = await axios.post(
      `https://graph.facebook.com/v24.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to: to,
        type: "template",
        template: {
          name: "bulk_img",
          language: { code: "en" },
          components: [
            {
              type: "header",
              parameters: [
                {
                  type: "image",
                  image: {
                    link: imageUrl
                  }
                }
              ]
            },
            {
              type: "body",
              parameters: [
                {
                  type: "text",
                  text: param1
                }
              ]
            }
          ]
        }
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json"
        }
      }
    );

    console.log("✅ Message sent:", response.data);
    return response.data;

  } catch (error) {
    console.error("❌ Error:", error.response?.data || error.message);
    throw new Error(error.response?.data?.error?.message || error.message);
  }
}


// Sendbulkimg(
//   "919360549855",
//   "Sharan",
//   "https://appincoimbatore.com/wp-content/uploads/2025/09/student-of-the-week-5.jpg"
// );


async function sendRequestConfirmationMsg(to, name, supportUrl) {
  try {
    to = cleanIndianMobileNumber(to);

    const response = await axios.post(
      `https://graph.facebook.com/v24.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "template",
        template: {
          name: "enroll_course_2",
          language: { code: "en" },
          components: [
            {
              type: "body",
              parameters: [
                { type: "text", text: name } // ✅ only 1 param
              ],
            },
            {
              type: "button",
              sub_type: "url",
              index: "0", // first button
              parameters: [
                {
                  type: "text",
                  text: supportUrl // ✅ dynamic URL goes here
                }
              ]
            }
          ],
        },
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    console.log("✅ Message sent:", response.data);
    return response.data;

  } catch (error) {
    console.error("❌ Error:", error.response?.data || error.message);
    throw new Error(error.response?.data?.error?.message || error.message);
  }
}

// sendRequestConfirmationMsg("919360549855","sharan","919629344584")

async function sendInternmsg(to, param1 , param2) {
  try {
    to = cleanIndianMobileNumber(to)
    console.log("Sending to: college interns", to, "with param:", param1 );

    param2 =  "Internship"  
    
    const response = await axios.post(
      `https://graph.facebook.com/v24.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "template",
        template: {
          name: "enroll_intern", // default pre-approved template name
          language: { code: "en" },
          // ✅ Optional: pass dynamic parameters if your template supports them
          components: [
            {
              type: "body",
              parameters: [{ type: "text", text: param1 },
                // { type: "text", text: param2 }
              ],
            },
          ],
        },
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    console.log("✅ Message sent successfully:", response.data);
    return response.data;
  } catch (error) {
    console.error("❌ Error sending template:", error.response?.data || error.message);
    // Re-throw the error so the calling function can handle it
    throw new Error(error.response?.data?.error?.message || error.message);
  }
}


// sendEnrollmentmsg("919360549855","Mohan","Python Course")

//           parameters: [
//             { type: "text", text: "Installment" },  // {{1}}
//             { type: "text", text: `₹${amount}` },   // {{2}}
//             { type: "text", text: diffDays === 0 ? "0" : `${diffDays}` },  // {{3}}
//             { type: "text", text: "late charges" }  // {{4}}
//           ]

app.delete("/clear-chunks", async (req, res) => {
  try {
    console.log("Hiii")

    const result = await Chunk.deleteMany({});

    res.json({
      success: true,
      message: "All documents cleared from chunks collection",
      deletedCount: result.deletedCount
    });
  } catch (err) {
    console.error("❌ Error clearing collection:", err);
    res.status(500).json({ success: false, error: "Failed to clear collection" });
  } 
});


app.delete("/:userNumber", async (req, res) => {
    const { userNumber } = req.params;

    try {
        const deletedUser = await Chat.findOneAndDelete({ userNumber });

        if (!deletedUser) {
            return res.status(404).json({ message: "User not found" });
        }

        res.status(200).json({ message: "User deleted successfully", deletedUser });
    } catch (error) {
        console.error("Error deleting user:", error);
        res.status(500).json({ message: "Internal server error" });
    }
});


async function Sendbulkmessage(to, param1) {
  try {
    to = cleanIndianMobileNumber(to)
    console.log("Sending to:", to, "with param:", param1);

    const response = await axios.post(
      `https://graph.facebook.com/v24.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "template",
        template: {
          name: "enquiry", // default pre-approved template name
          language: { code: "en" },
          // ✅ Optional: pass dynamic parameters if your template supports them
          components: [
            {
              type: "body",
              parameters: [{ type: "text", text: param1 }],
            },
          ],
        },
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    console.log("✅ Message sent successfully:", response.data);
    return response.data;
  } catch (error) {
    console.error("❌ Error sending template:", error.response?.data || error.message);
    // Re-throw the error so the calling function can handle it
    throw new Error(error.response?.data?.error?.message || error.message);
  }
}


function normalizeWhatsappMessage(message) {
  if (!message || typeof message !== "string") return "";

  return message
    .split(/\s+/)      // split by ANY whitespace (space, tab, newline)
    .filter(Boolean)   // remove empty words
    .join(" ")         // join with single space
    .trim();           // final safety trim
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

app.post("/leadmsg/crm", async (req, res) => {
  try {
    const { leadIds, message , couns_number, campaignName } = req.body;
    const cleanMessage = normalizeWhatsappMessage(message);
    const cleanCampaignName = typeof campaignName === "string" ? campaignName.trim() : "";

    if (!Array.isArray(leadIds) || !leadIds.length || !message) {
      return res.status(400).json({ error: "leadIds and message are required" });
    }

    // ✅ If leadIds are Chat _ids
    const chats = await Chat.find(
      { _id: { $in: leadIds } },
      { userNumber: 1 }
    );

    const BATCH_SIZE = 5;
    const DELAY_MS = 1500;

    let success = 0;
    let failed = 0;
    await Sendbulkmessage(couns_number,cleanMessage)
    await sendMessage("919597271533",cleanMessage) //Mohan Number
    for (let i = 0; i < chats.length; i += BATCH_SIZE) {
      const batch = chats.slice(i, i + BATCH_SIZE);
      const sentChatIds = [];

      await Promise.allSettled(
        batch.map(chat => {
          if (!chat.userNumber) {
            failed++;
            return Promise.reject("No user number");
          }

          const number = chat.userNumber.replace(/\D/g, "");
          const fullNumber = number.startsWith("91") ? number : "91" + number;

          console.log("Sending to:", fullNumber);

          return Sendbulkmessage(fullNumber, cleanMessage)
            .then(() => { success++; sentChatIds.push(chat._id); })
            .catch(() => failed++);
        })
      );

      if (cleanCampaignName && sentChatIds.length) {
        await Chat.updateMany(
          { _id: { $in: sentChatIds } },
          { $push: { campaignHistory: { campaignName: cleanCampaignName, message: cleanMessage, sentAt: new Date() } } }
        );
      }

      if (i + BATCH_SIZE < chats.length) {
        await sleep(DELAY_MS);
      }
    }

    res.json({
      message: "Bulk messages sent",
      total: chats.length,
      success,
      failed
    });

  } catch (err) {
    console.error("Bulk msg error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});


// Bulk WhatsApp campaign to brand-new (non-lead) contacts uploaded from Excel.
// Numbers that already belong to an existing Chat/lead are skipped entirely —
// this route only targets genuinely unknown numbers, staged in TempLead until
// they reply (see the /webhook find-or-create block, which promotes them to Chat).
app.post("/leadmsg/bulk-temp", async (req, res) => {
  try {
    const { contacts, message, campaignName } = req.body;
    const cleanMessage = normalizeWhatsappMessage(message);
    const cleanCampaignName = typeof campaignName === "string" ? campaignName.trim() : "";

    if (!Array.isArray(contacts) || !contacts.length || !cleanMessage || !cleanCampaignName) {
      return res.status(400).json({ error: "contacts, message and campaignName are required" });
    }

    const normalized = [];
    const seen = new Set();
    for (const c of contacts) {
      const fullNumber = cleanIndianMobileNumber(c?.userNumber);
      if (!fullNumber || seen.has(fullNumber)) continue;
      seen.add(fullNumber);
      normalized.push({ userNumber: fullNumber, userName: (c?.userName || "").trim() || "Unknown" });
    }

    let skippedExisting = 0;
    const targets = [];
    for (const contact of normalized) {
      const existing = await Chat.findOne({ userNumber: contact.userNumber }, { _id: 1 });
      if (existing) {
        skippedExisting++;
      } else {
        targets.push(contact);
      }
    }

    const BATCH_SIZE = 5;
    const DELAY_MS = 1500;

    let success = 0;
    let failed = 0;

    for (let i = 0; i < targets.length; i += BATCH_SIZE) {
      const batch = targets.slice(i, i + BATCH_SIZE);

      await Promise.allSettled(
        batch.map(contact =>
          Sendbulkmessage(contact.userNumber, cleanMessage)
            .then(async () => {
              success++;
              await TempLead.findOneAndUpdate(
                { userNumber: contact.userNumber },
                {
                  userName: contact.userName,
                  campaignName: cleanCampaignName,
                  message: cleanMessage,
                  sentAt: new Date(),
                  status: "sent"
                },
                { upsert: true, new: true }
              );
            })
            .catch(() => { failed++; })
        )
      );

      if (i + BATCH_SIZE < targets.length) {
        await sleep(DELAY_MS);
      }
    }

    res.json({
      message: "Bulk campaign sent",
      total: normalized.length,
      success,
      failed,
      skippedExisting
    });

  } catch (err) {
    console.error("Bulk temp campaign error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});



app.post("/notify/crm", async (req, res) => {
  console.log(req.body)
  try {
    const { leadId, assignedTo, userNumber, userName } = req.body;

    // 1️⃣ Validate request body
    if (!leadId || !assignedTo ) {
      return res.status(400).json({
        success: false,
        message: "Missing required fields"
      });
    }

    // 2️⃣ Find counsellor
    const counsellor = await User.findOne({ username: assignedTo });

    if (!counsellor) {
      return res.status(404).json({
        success: false,
        message: "Counsellor not found"
      });
    }


const notificationPayload =
  `Hi ${counsellor.username}, 👋\n\n` +
  `📌 *New Lead Assigned*\n\n` +
  `👤 *Name:* ${userName}\n` +
  `📞 *Mobile Number:* ${userNumber}\n\n` +
  `🔗 Please login to view details:\n` +
  `https://ask4people.in`;

    // 4️⃣ Send message to counsellor
        console.log(counsellor.userNumber, notificationPayload)
     sendMessage(counsellor.userNumber,notificationPayload);

    console.log("Lead notified")

    return res.status(200).json({
      success: true,
      message: "Counsellor notified successfully"
    });

  } catch (error) {
    console.error("Notify CRM Error:", error);
    return res.status(500).json({
      success: false,
      message: "Internal server error"
    });
  }
});


app.post("/notify/addlead", async (req, res) => {
  console.log(req.body)
  try {
    const { fullNumber } = req.body;


    // 2️⃣ Find counsellor
   const user = await Chat.findOne({ userNumber: fullNumber });

    console.log(user)

    if (!user) {
      return res.status(404).json({
        success: false,
        message: "user not found"
      });
    }

    const assignedUser = await User.findOne({
      username: user.assignedto
    });

    console.log("Assigned User:", assignedUser.userNumber);

  const notificationPayload = `Hi ${user.userName} Thanks for interest in Beta Space`;

    // 4️⃣ Send message to counsellor
        console.log(user.userNumber, notificationPayload)
      //  sendEnrollmentmsg(user.userNumber,user.userName,user.course)
        sendRequestConfirmationMsg(user.userNumber,user.userName, assignedUser.userNumber)

    console.log("Lead notified")

    return res.status(200).json({
      success: true,
      message: "Counsellor notified successfully"
    });

  } catch (error) {
    console.error("Notify CRM Error:", error);
    return res.status(500).json({
      success: false,
      message: "Internal server error"
    });
  }
});

// Determines lead source: "Google ads" if any Google Ads tracking param is present with a value, else "Website"
function detectLeadFrom(body) {
  const adsFields = ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "gclid", "landing_page"];
  const hasAdsData = adsFields.some((field) => body?.[field] && body[field].toString().trim() !== "");
  return hasAdsData ? "Google ads" : "Website";
}

function cleanIndianMobileNumber(input) {
  if (!input) return null;

  let digits = input.toString().replace(/\D/g, '');

  if (digits.startsWith("91") && digits.length === 12) {
    return digits;
  }

  if (digits.length === 10) {
    return "91" + digits;
  }

  if (digits.length === 11 && digits.startsWith("0")) {
    return "91" + digits.slice(1);
  }

  return digits;
}

// Sanitizes text for WhatsApp template params — removes newlines/tabs and collapses runs of 5+ spaces
function cleanWhatsAppText(text) {
  if (!text) return text;
  return text
    .replace(/[\n\r\t]/g, ' ')   // newlines and tabs → single space
    .replace(/ {5,}/g, '    ');  // 5+ consecutive spaces → 4 spaces (max allowed)
}



// Fetch and clean text from a single URL
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

// Fetch company info
async function fetchCompanyInfo() {
  let combinedText = "";
  for (const url of companyUrls) {
    const text = await fetchTextFromUrl(url);
    if (text) {
      combinedText += `\n\n[Source: ${url}]\n${text}`;
    }
  }
  dynamicCompanyInfo = combinedText;
  console.log("✅ Company info updated");
  // console.log(dynamicCompanyInfo);
 injested_res = await  ingestKnowledge(dynamicCompanyInfo)
}

// --- Auto-run when server starts ---
// fetchCompanyInfo(); // fetch once immediately 

// --- Refresh every 10 minutes ---
// setInterval(fetchCompanyInfo, 120 * 60 * 1000);

app.get('/myapp', (req, res) => {
  sendMessage("919360549855","Whatsapp Servie called")
  console.log("Whatsapp Service called")
  res.status(200).json({ CODE: 'HII BRO' });
});

app.post("/my/post",async(req,res)=>{
   console.log(req.body)
   const {name , remarks , assignedto , UserNumber } = req.body
  const sanitizedNumber = UserNumber.replace(/\D/g, "");
  const fullNumber = cleanIndianMobileNumber(sanitizedNumber)
  let userChat = await Chat.findOne({ userNumber: fullNumber });
    if (userChat) {
      return res.status(400).json({ message: "User already exists." });
    }
     userChat = new Chat({
      userNumber: fullNumber,
      userName: name.trim() || "Unknown",
      assignedto : assignedto,
      conversations: [],
        lastInteracted: new Date(),
        leadfrom: "Call",
        remarks: remarks ? [{ remark: remarks.trim(), timestamp: new Date() }] : [],
    });

    await userChat.save();
    // ✅ 6. Notify via WebSocket (if configured)
    broadcast({ type: "new_user", user: userChat });

  res.json({
    success: true,
  });
  console.log(req.body)
})


// To receive incomming call automation

app.post('/api/democ/:number', async (req, res) => {
  // const number = req.params.number;
    // console.log(req.body)
  const number = decodeURIComponent(req.params.number);
   const sanitizedNumber = number.replace(/\D/g, "");
   const {name , remarks , assignedto } = req.body
  console.log(name , remarks ,sanitizedNumber)
  // console.log("Received number:", number);
  const fullNumber = cleanIndianMobileNumber(sanitizedNumber)
    // ✅ 4. Check if user already exists
    let userChat = await Chat.findOne({ userNumber: fullNumber });
    if (userChat) {
      return res.status(400).json({ message: "User already exists." });
    }
      userChat = new Chat({
      userNumber: fullNumber,
      userName: name.trim() || "Unknown",
      assignedto : assignedto,
      conversations: [],
        lastInteracted: new Date(),
        leadfrom: "Call",
      remarks: remarks ? [{ remark: remarks.trim(), timestamp: new Date() }] : [],
    });

    await userChat.save();
    // ✅ 6. Notify via WebSocket (if configured)
    broadcast({ type: "new_user", user: userChat });

  res.json({
    success: true,
    number
  });
});

async function paymentReminderTemplate(
  to,
  userName,
  reminderText,
  total,
  paidAmount,
  balance,
  installmentAmount,
  dueDate
) {
  try {
    to = cleanIndianMobileNumber(to);

    console.log("Sending to:", to);

    const response = await axios.post(
      `https://graph.facebook.com/v24.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "template",
        template: {
          name: "fees_remainder_1", // ✅ your new template
          language: { code: "en" },
          components: [
            {
              type: "body",
              parameters: [
                { type: "text", text: userName },           // {{1}}
                { type: "text", text: reminderText },       // {{2}}
                { type: "text", text: total.toString() },   // {{3}}
                { type: "text", text: paidAmount.toString() }, // {{4}}
                { type: "text", text: balance.toString() }, // {{5}}
                { type: "text", text: installmentAmount.toString() }, // {{6}}
                { type: "text", text: dueDate }             // {{7}}
              ],
            },
          ],
        },
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    console.log("✅ Message sent successfully:", response.data);
    return response.data;

  } catch (error) {
    console.error("❌ Error sending template:", error.response?.data || error.message);
    throw new Error(error.response?.data?.error?.message || error.message);
  }
}

async function counsellorFeesReminder() {
  try {
    // Same date range as sendFeeReminders: today → +2 days
    const start = new Date();
    start.setHours(0, 0, 0, 0);

    const end = new Date();
    end.setHours(23, 59, 59, 999);
    end.setDate(end.getDate() + 2);

    function getDaysLeft(dueDate) {
      const today = new Date();
      today.setHours(0, 0, 0, 0);
      const due = new Date(dueDate);
      due.setHours(0, 0, 0, 0);
      return Math.ceil((due - today) / (1000 * 60 * 60 * 24));
    }

    // ===== 2. FETCH USERS =====
    const users = await Chat.find({
      "fees.installments": {
        $elemMatch: {
          dueDate: { $gte: start, $lte: end },
          status: "Pending"
        }
      }
    });

    console.log("Users found:", users.length);
    console.log(`📌 counsellorFeesReminder: ${users.length} users with upcoming fees`);

    // Group users by assignedto counsellor
    const counsellorMap = {};
    for (const user of users) {
      const { userName, userNumber, fees, assignedto } = user;
      if (!fees || !fees.installments) continue;

      const total = fees.billedAmount || 0;
      let paidAmount = 0;
      fees.installments.forEach(inst => {
        if (inst.status === "Paid") paidAmount += inst.amount;
      });
      const balance = total - paidAmount;

      const dueInstallments = fees.installments.filter(inst =>
        inst.status === "Pending" &&
        new Date(inst.dueDate) >= start &&
        new Date(inst.dueDate) <= end
      );

      if (dueInstallments.length === 0) continue;

      if (!counsellorMap[assignedto]) counsellorMap[assignedto] = [];
      counsellorMap[assignedto].push({ userName, userNumber, balance, dueInstallments });
    }

    // Send one grouped message per counsellor
    for (const [counsellorUsername, leads] of Object.entries(counsellorMap)) {
      const counsellor = await User.findOne({ username: counsellorUsername });
      if (!counsellor || !counsellor.userNumber) {
        console.warn(`⚠️ No user/number found for counsellor: ${counsellorUsername}`);
        continue;
      }

      let message = `💰 *Fee Collection Reminder*\n\nHi ${counsellor.username}, here are the students you need to collect fees from:\n\n`;

      leads.forEach((lead, index) => {
        lead.dueInstallments.forEach(inst => {
          const daysLeft = getDaysLeft(inst.dueDate);
          const whenLabel = daysLeft === 0 ? "Due *Today*" : daysLeft === 1 ? "Due *Tomorrow*" : `Due in *${daysLeft} days*`;

          message += `${index + 1}. 👤 *${lead.userName || "Unknown"}*\n`;
          message += `   📞 ${lead.userNumber}\n`;
          message += `   📅 ${whenLabel} — ₹${inst.amount} (${new Date(inst.dueDate).toDateString()})\n`;
          message += `   📊 Total Pending Balance: ₹${lead.balance}\n\n`;
        });
      });

      message += `Please follow up and update payment status in CRM. \n— Beta Space`;

      await sendWhatsAppMessage(counsellor.userNumber, message);
      await sendMessage("919597271533",message)
      console.log(`✅ Fee reminder sent to counsellor ${counsellor.username} (${counsellor.userNumber}) for ${leads.length} students`);
    }
  } catch (err) {
    console.error("❌ counsellorFeesReminder error:", err);
  }
}





async function sendFeeReminders() {
  try {
    // ===== 1. DATE RANGE (Today → +2 days) =====
    const start = new Date();
    start.setHours(0, 0, 0, 0);

    const end = new Date();
    end.setHours(23, 59, 59, 999);
    end.setDate(end.getDate() + 2);

    // ===== 2. FETCH USERS =====
    const users = await Chat.find({
      "fees.installments": {
        $elemMatch: {
          dueDate: { $gte: start, $lte: end },
          status: "Pending"
        }
      }
    });

    console.log("Users found:", users.length);

    // ===== 3. HELPER FUNCTION =====
    function getDaysDifference(dueDate) {
      const today = new Date();
      today.setHours(0, 0, 0, 0);

      const due = new Date(dueDate);
      due.setHours(0, 0, 0, 0);

      const diffTime = due - today;
      return Math.ceil(diffTime / (1000 * 60 * 60 * 24));
    }

    // ===== 4. PROCESS USERS =====
    for (const user of users) {
      const { userName, userNumber, fees } = user;

      if (!fees || !fees.installments) continue;

      const total = fees.billedAmount || 0;

      // ===== Calculate Paid Amount =====
      let paidAmount = 0;
      fees.installments.forEach(inst => {
        if (inst.status === "Paid") {
          paidAmount += inst.amount;
        }
      });

      const balance = total - paidAmount;

      // ===== Filter Relevant Installments =====
      const dueInstallments = fees.installments.filter(inst =>
        inst.status === "Pending" &&
        inst.dueDate >= start &&
        inst.dueDate <= end
      );

      // ===== 5. PROCESS EACH INSTALLMENT =====
      for (const inst of dueInstallments) {
        const daysLeft = getDaysDifference(inst.dueDate);

        // Ensure reminder tracking exists
        if (!inst.remindersSent) {
          inst.remindersSent = {
            twoDay: false,
            oneDay: false,
            sameDay: false
          };
        }

        let reminderText = "";
        let shouldSend = false;

        // ===== 6. CHECK WHICH REMINDER =====
        if (daysLeft === 2 && !inst.remindersSent.twoDay) {
          reminderText = "Reminder: Your payment is due in 2 days.";
          inst.remindersSent.twoDay = true;
          shouldSend = true;
        } 
        else if (daysLeft === 1 && !inst.remindersSent.oneDay) {
          reminderText = "Reminder: Your payment is due tomorrow.";
          inst.remindersSent.oneDay = true;
          shouldSend = true;
        } 
        else if (daysLeft === 0 && !inst.remindersSent.sameDay) {
          reminderText = "Reminder: Your payment is due today.";
          inst.remindersSent.sameDay = true;
          shouldSend = true;
        }

        if (!shouldSend) continue;

        // ===== 7. FORMAT MESSAGE =====
        const message = `Course Fees Reminder

Hi ${userName}

${reminderText}

*Total Course Value:* ₹${total}
*Amount Paid Till Now:* ₹${paidAmount}
*Balance Amount:* ₹${balance}

Your installment of ₹${inst.amount} is due on ${inst.dueDate.toDateString()}.

Kindly make the payment on or before the due date.

— Beta Space`;

        console.log(`Sending to ${userNumber} (${daysLeft} days left)`);

        // await sendMessage(userNumber, message);
await  paymentReminderTemplate(
  userNumber,
  userName,
  reminderText,
  total,
  paidAmount,
  balance,
  inst.amount,
  inst.dueDate.toDateString()
) 
      }

      // ===== 8. SAVE UPDATED FLAGS =====
      await user.save();
    }

  } catch (error) {
    console.error("Fee Reminder Error:", error);
  }
}

app.get("/num/:mobile", (req, res) => {
  let { mobile } = req.params;

  mobile = mobile.replace(/\D/g, "");

  return res.redirect(`https://wa.me/${mobile}`);
});


app.post("/leadfrom/chatbot", async (req, res) => {
  try {

    console.log("Hii received")
    const { fullNumber, message } = req.body;

    if (!fullNumber || !message) {
      return res.status(400).json({
        success: false,
        error: "fullNumber and message are required"
      });
    }

    console.log("Incoming CRM WhatsApp request:", {
      number: fullNumber,
      message
    });

    const result = await sendMessage(fullNumber, message);

    res.json({
      success: true,
      result
    });

  } catch (error) {
    console.error("WhatsApp send error:", error);

    res.status(500).json({
      success: false,
      error: "Failed to send WhatsApp message"
    });
  }
});

// Runs every day at 9:00 AM
cron.schedule("0 11 * * *", async () => {
  console.log("Running fee reminder job at 9 AM");
  await sendFeeReminders();
  await counsellorFeesReminder();
},{
    timezone: "Asia/Kolkata", // 👈 add this line to fix time zone to IST
  });

// Runs every day at 9:00 AM IST — intern schedule reminder (2 days ahead)
function getIstDayUtcRange(daysAhead = 0) {
  const IST_OFFSET_MS = 5.5 * 60 * 60 * 1000;
  const istNow = new Date(Date.now() + IST_OFFSET_MS);
  const targetIstDate = new Date(istNow);
  targetIstDate.setUTCDate(targetIstDate.getUTCDate() + daysAhead);

  const targetIstMidnightUtcMs = Date.UTC(
    targetIstDate.getUTCFullYear(),
    targetIstDate.getUTCMonth(),
    targetIstDate.getUTCDate()
  );

  const start = new Date(targetIstMidnightUtcMs - IST_OFFSET_MS);
  const end = new Date(start.getTime() + (24 * 60 * 60 * 1000) - 1);

  return { start, end };
}


async function sendDailyReport() {
  try {
    console.log("📊 Running daily WhatsApp report...");

    const now = new Date();
    // now.setDate(now.getDate() - 1); For previous day
    const start = new Date(now);
    start.setHours(0, 0, 0, 0);
    const end = new Date(now);
    end.setHours(23, 59, 59, 999);

    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0, 0);
    const monthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59, 999);

    const [report, monthlyAgg, leadsThisMonth] = await Promise.all([
      Chat.aggregate([
      { $match: { assignedto: { $exists: true, $ne: null } } },
      {
        $addFields: {
          isNewLeadOnDate: {
            $and: [
              { $gte: ["$datecreated", start] },
              { $lte: ["$datecreated", end] },
              {
                $not: {
                  $and: [
                    { $eq: [{ $toLower: { $ifNull: ["$pipeline", ""] } }, "enrolled"] },
                    { $eq: [{ $toLower: { $ifNull: ["$course", ""] } }, "college internship"] }
                  ]
                }
              }
            ]
          }
        }
      },
      {
        $addFields: {
          dateRemarks: {
            $filter: {
              input: { $ifNull: ["$remarks", []] },
              as: "remark",
              cond: {
                $and: [
                  { $gte: ["$$remark.timestamp", start] },
                  { $lte: ["$$remark.timestamp", end] }
                ]
              }
            }
          }
        }
      },
      {
        $addFields: {
          paidOnDateInstallments: {
            $filter: {
              input: { $ifNull: ["$fees.installments", []] },
              as: "inst",
              cond: {
                $and: [
                  { $eq: ["$$inst.status", "Paid"] },
                  { $gte: ["$$inst.dueDate", start] },
                  { $lte: ["$$inst.dueDate", end] }
                ]
              }
            }
          },
          firstInstDueDate: {
            $let: {
              vars: { first: { $arrayElemAt: ["$fees.installments", 0] } },
              in: { $ifNull: ["$$first.dueDate", null] }
            }
          },
          reminderOnDateFlag: {
            $cond: [
              {
                $and: [
                  { $ifNull: ["$reminder", false] },
                  { $gte: ["$reminder", start] },
                  { $lte: ["$reminder", end] }
                ]
              },
              1, 0
            ]
          }
        }
      },
      {
        $addFields: {
          paidOnDateAmount: {
            $sum: {
              $map: {
                input: "$paidOnDateInstallments",
                as: "inst",
                in: { $ifNull: ["$$inst.amount", 0] }
              }
            }
          },
          billedOnDate: {
            $and: [
              { $ifNull: ["$firstInstDueDate", false] },
              { $gte: ["$firstInstDueDate", start] },
              { $lte: ["$firstInstDueDate", end] }
            ]
          },
          billedOnDateAmount: {
            $cond: [
              {
                $and: [
                  { $ifNull: ["$firstInstDueDate", false] },
                  { $gte: ["$firstInstDueDate", start] },
                  { $lte: ["$firstInstDueDate", end] }
                ]
              },
              { $ifNull: ["$fees.billedAmount", 0] },
              0
            ]
          }
        }
      },
      {
        $group: {
          _id: "$assignedto",
          newLeadsToday: { $sum: { $cond: ["$isNewLeadOnDate", 1, 0] } },
          // New leads with no remarks today (not contacted)
          newLeadsMissed: {
            $sum: {
              $cond: [
                { $and: ["$isNewLeadOnDate", { $eq: [{ $size: "$dateRemarks" }, 0] }] },
                1, 0
              ]
            }
          },
          // Remarks on old leads today
          oldRemarksCount: {
            $sum: {
              $cond: [
                { $not: "$isNewLeadOnDate" },
                { $size: "$dateRemarks" },
                0
              ]
            }
          },
          // Remarks on new leads today
          newRemarksCount: {
            $sum: {
              $cond: [
                "$isNewLeadOnDate",
                { $size: "$dateRemarks" },
                0
              ]
            }
          },
          feesUpdatedToday: { $sum: "$paidOnDateAmount" },
          billedTodayTotal: { $sum: "$billedOnDateAmount" },
          // Reminders today where lead was contacted (has remarks today)
          remindersDone: {
            $sum: {
              $cond: [
                { $and: [{ $eq: ["$reminderOnDateFlag", 1] }, { $gt: [{ $size: "$dateRemarks" }, 0] }] },
                1, 0
              ]
            }
          },
          // Reminders today where lead was NOT contacted (no remarks today)
          remindersMissed: {
            $sum: {
              $cond: [
                { $and: [{ $eq: ["$reminderOnDateFlag", 1] }, { $eq: [{ $size: "$dateRemarks" }, 0] }] },
                1, 0
              ]
            }
          }
        }
      },
      {
        $project: {
          _id: 0,
          username: "$_id",
          newLeadsToday: 1,
          newLeadsMissed: 1,
          oldRemarksCount: 1,
          newRemarksCount: 1,
          feesUpdatedToday: 1,
          billedTodayTotal: 1,
          remindersDone: 1,
          remindersMissed: 1
        }
      },
      { $sort: { username: 1 } }
      ]),
      Chat.aggregate([
        { $match: { pipeline: { $in: ["Converted", "Enrolled"] } } },
        {
          $project: {
            firstInstDueDate: { $arrayElemAt: ["$fees.installments.dueDate", 0] },
            billedAmount: { $ifNull: ["$fees.billedAmount", 0] },
            paidInstallments: { $ifNull: ["$fees.installments", []] }
          }
        },
        {
          $addFields: {
            billedThisMonth: {
              $cond: [
                {
                  $and: [
                    { $ifNull: ["$firstInstDueDate", false] },
                    { $gte: ["$firstInstDueDate", monthStart] },
                    { $lte: ["$firstInstDueDate", monthEnd] }
                  ]
                },
                "$billedAmount",
                0
              ]
            },
            paidThisMonth: {
              $sum: {
                $map: {
                  input: "$paidInstallments",
                  as: "inst",
                  in: {
                    $cond: [
                      {
                        $and: [
                          { $eq: ["$$inst.status", "Paid"] },
                          { $ifNull: ["$$inst.dueDate", false] },
                          { $gte: ["$$inst.dueDate", monthStart] },
                          { $lte: ["$$inst.dueDate", monthEnd] }
                        ]
                      },
                      { $ifNull: ["$$inst.amount", 0] },
                      0
                    ]
                  }
                }
              }
            }
          }
        },
        {
          $group: {
            _id: null,
            totalBilledMonth: { $sum: "$billedThisMonth" },
            totalPaidMonth: { $sum: "$paidThisMonth" }
          }
        }
      ]),
      Chat.countDocuments({
        datecreated: { $gte: monthStart, $lte: monthEnd },
        $nor: [{ pipeline: { $regex: /^enrolled$/i }, course: { $regex: /^college internship$/i } }]
      })
    ]);

    const dateStr = now.toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" });

    let totalBilled = 0;
    let totalPaid = 0;
    let msg = `📊 *Daily Report - ${dateStr}*\n`;
    msg += `━━━━━━━━━━━━━━━━━━━━\n\n`;

    for (const staff of report) {
      totalBilled += staff.billedTodayTotal || 0;
      totalPaid += staff.feesUpdatedToday || 0;

      const billed = (staff.billedTodayTotal || 0).toLocaleString("en-IN");
      const paid = (staff.feesUpdatedToday || 0).toLocaleString("en-IN");

      msg += `👤 *${staff.username}*\n`;
      msg += `NEW: ${staff.newLeadsToday}  |  Missed: ${staff.newLeadsMissed}\n`;
      msg += `REMARKS:\n  old: ${staff.oldRemarksCount} , new: ${staff.newRemarksCount}\n`;
      msg += `BILLED: ₹${billed}  |  PAID: ₹${paid}\n`;
      msg += `REMINDERS Marked Today:\n  Done: ${staff.remindersDone}  |  Missed: ${staff.remindersMissed}\n\n`;
    }

    const totalNewLeadsToday = report.reduce((sum, s) => sum + (s.newLeadsToday || 0), 0);
    const monthBilled = monthlyAgg[0]?.totalBilledMonth || 0;
    const monthPaid = monthlyAgg[0]?.totalPaidMonth || 0;
    const monthName = now.toLocaleDateString("en-IN", { month: "long" });

    msg += `━━━━━━━━━━━━━━━━━━━━\n`;
    msg += `💰 *FEES BILLED TODAY: ₹${totalBilled.toLocaleString("en-IN")}*\n`;
    msg += `✅ *FEES PAID TODAY: ₹${totalPaid.toLocaleString("en-IN")}*\n\n`;
    msg += `🆕 New Leads Today: *${totalNewLeadsToday}*\n`;
    msg += `━━━━━━━━━━━━━━━━━━━━\n`;
    msg += `📅 *${monthName.toUpperCase()} SUMMARY*\n`;
    msg += `💵 Total Billed This Month: *₹${monthBilled.toLocaleString("en-IN")}*\n`;
    msg += `✅ Total Paid This Month: *₹${monthPaid.toLocaleString("en-IN")}*\n\n`;
    msg += `📋 Leads This Month: *${leadsThisMonth}*`;

    const wpUrl = process.env.WP_CHAT_SERVICE_URL;
    if (!wpUrl) {
      console.error("❌ WP_CHAT_SERVICE_URL not set — cannot send daily report");
      return;
    }

    await sendMessage("919597271533",msg)

    console.log("✅ Daily WhatsApp report sent successfully");
  } catch (err) {
    console.error("❌ Daily report error:", err);
  }
}


cron.schedule("45 9 * * *", async () => {
  console.log("🔔 Running intern schedule reminder cron...");
  try {
    const { start: startOfReminderDay, end: endOfReminderDay } = getIstDayUtcRange(2);

    // Group interns by batch_code whose scheduled date is exactly 2 IST calendar days away.
    const batchGroups = await Chat.aggregate([
      { $match: { InternDetails: { $exists: true, $ne: [] } } },
      { $unwind: "$InternDetails" },
      {
        $addFields: {
          scheduledDateForReminder: {
            $ifNull: ["$InternDetails.date_scheduled", "$date_scheduled"]
          }
        }
      },
      {
        $match: {
          scheduledDateForReminder: { $gte: startOfReminderDay, $lte: endOfReminderDay },
          "InternDetails.batch_code": { $nin: [null, ""] }
        }
      },
      {
        $group: {
          _id: "$InternDetails.batch_code",
          college: { $first: "$InternDetails.college" },
          count: { $sum: 1 },
          scheduledDate: { $first: "$scheduledDateForReminder" },
          counsellors: { $addToSet: "$assignedto" }
        }
      }
    ]);

    console.log(`📌 Found ${batchGroups.length} batch(es) with upcoming schedules`);

    for (const batch of batchGroups) {
      const { _id: batchCode, college, count, scheduledDate, counsellors } = batch;

      for (const counsellorUsername of counsellors) {
        if (!counsellorUsername) continue;

        const counsellor = await User.findOne({ username: counsellorUsername });
        if (!counsellor || !counsellor.userNumber) {
          console.warn(`⚠️ No user/number found for counsellor: ${counsellorUsername}`);
          continue;
        }

        const dateStr = scheduledDate
          ? new Date(scheduledDate).toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" })
          : "upcoming";

        const message =
          `🔔 *Intern Schedule Reminder*\n\n` +
          `Hi ${counsellor.username}, you have *${count}* intern(s) from *${college || "N/A"}* ` +
          `(Batch: *${batchCode}*) scheduled on *${dateStr}*.\n\n` +
          `Please be prepared and follow up with them! ✅`;

        await sendWhatsAppMessage(counsellor.userNumber, message);
        console.log(`✅ Schedule reminder sent to ${counsellor.username} for batch ${batchCode}`);
      }
    }
  } catch (error) {
    console.error("❌ Intern schedule reminder cron error:", error);
  }
}, {
  timezone: "Asia/Kolkata"
});

// Internal endpoint used by CRM backend to send a plain WhatsApp text message
app.post("/internal/send-text", async (req, res) => {
  const { to, message } = req.body;
  if (!to || !message) {
    return res.status(400).json({ success: false, message: "to and message are required" });
  }
  try {
    await sendWhatsAppMessage(to, message);
    res.json({ success: true });
  } catch (err) {
    console.error("❌ /internal/send-text error:", err.message);
    res.status(500).json({ success: false, message: err.message });
  }
});

// Called by the CRM's "Trigger Now" button (SalesTraining.js on the CRM
// side) to kick off today's sales training immediately instead of waiting
// for the 3:30 PM cron below. Safe to call more than once — counsellors who
// already have a training session started today are skipped.
app.post("/training/trigger-now", async (req, res) => {
  try {
    const result = await triggerSalesTrainingNow();
    res.json({ success: true, ...result });
  } catch (err) {
    console.error("❌ /training/trigger-now error:", err.message);
    res.status(500).json({ success: false, message: err.message });
  }
});


const COURSE_SYLLABUS = {
    "Python Fullstack with AI": "https://appincoimbatore.com/full-stack-python-syllabus.html",
    "Java Fullstack with AI": "https://appincoimbatore.com/full-stack-java-syllabus.html",
    "Data Analytics": "https://appincoimbatore.com/data-analytics-syllabus.html",
    "Digital Marketing": "https://appincoimbatore.com/digital-marketing-syllabus.html",
}

// company_profile
// Hi {{1}}! 👋
// Thanks for reaching out about {{2}}.
// Here's our company profile 📄 — inside you'll find:
// ✅ All our courses & AI-integrated curriculum
// ✅ Placement support & hiring partners
// ✅ Student success stories & certifications
// Have questions? Just reply here, or talk to our counsellor directly:
// 📞 {{3}}
// I'm Minmini, your AI assistant — happy to help! 😊
async function sendCompanyProfileTemplate(to, userName, course, counsellorNumber) {
  try {
    to = cleanIndianMobileNumber(to);
    console.log("Sending company_profile to:", to);

    const response = await axios.post(
      `https://graph.facebook.com/v22.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "template",
        template: {
          name: "company_profile",
          language: { code: "en" },
          components: [
            {
              type: "body",
              parameters: [
                { type: "text", text: userName },        // {{1}}
                { type: "text", text: course },           // {{2}}
                { type: "text", text: counsellorNumber }, // {{3}}
              ],
            },
          ],
        },
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    console.log("✅ company_profile template sent:", response.data);
    return response.data;
  } catch (error) {
    console.error("❌ Error sending company_profile template:", error.response?.data || error.message);
    throw new Error(error.response?.data?.error?.message || error.message);
  }
}

// syllabus_followup
// Hi {{1}}! 👋
// Following up on your enquiry about {{2}}.
// Here's the full syllabus 👇
// {{3}}
// It covers everything — modules, hands-on projects & certification. I'm Minmini, your AI assistant — reply here if you have questions about fees, batch timings, or the curriculum.
// Prefer to talk directly? Our counsellor is happy to help:  {{4}}.
// I'm Minmini, your AI assistant
async function sendSyllabusFollowupTemplate(to, userName, course, syllabusLink, counsellorNumber) {
  try {
    to = cleanIndianMobileNumber(to);
    console.log("Sending syllabus_followup to:", to);

    const response = await axios.post(
      `https://graph.facebook.com/v22.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "template",
        template: {
          name: "syllabus_followup",
          language: { code: "en" },
          components: [
            {
              type: "body",
              parameters: [
                { type: "text", text: userName },        // {{1}}
                { type: "text", text: course },           // {{2}}
                { type: "text", text: syllabusLink },     // {{3}}
                { type: "text", text: counsellorNumber }, // {{4}}
              ],
            },
          ],
        },
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    console.log("✅ syllabus_followup template sent:", response.data);
    return response.data;
  } catch (error) {
    console.error("❌ Error sending syllabus_followup template:", error.response?.data || error.message);
    throw new Error(error.response?.data?.error?.message || error.message);
  }
}

// placement_support
// Hi {{1}}! 🌟
// Students from Appin Technology, Coimbatore are now placed and building great careers.
// Need guidance on choosing the right batch? Our counsellor is happy to help:
// 📞 {{2}}
// I'm Minmini, your AI assistant — happy to answer quick questions too!
// See their journeys, placement partners & testimonials 👇
async function sendPlacementSupportTemplate(to, userName, counsellorNumber) {
  try {
    to = cleanIndianMobileNumber(to);
    console.log("Sending placement_support to:", to);

    const response = await axios.post(
      `https://graph.facebook.com/v22.0/${PHONE_NUMBER_ID}/messages`,
      {
        messaging_product: "whatsapp",
        to,
        type: "template",
        template: {
          name: "placement_support",
          language: { code: "en" },
          components: [
            {
              type: "body",
              parameters: [
                { type: "text", text: userName },        // {{1}}
                { type: "text", text: counsellorNumber }, // {{2}}
              ],
            },
          ],
        },
      },
      {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json",
        },
      }
    );

    console.log("✅ placement_support template sent:", response.data);
    return response.data;
  } catch (error) {
    console.error("❌ Error sending placement_support template:", error.response?.data || error.message);
    throw new Error(error.response?.data?.error?.message || error.message);
  }
}

// 5 PM day-wise follow-up cron: day 1 -> company_profile, day 2 -> syllabus_followup, day 3 -> placement_support.
// Reuses lastFollowUpSentAt (shared with the 10 AM cron) so the existing respondedAfterFollowUp detection
// on incoming replies keeps working without any change there.
// cron.schedule("0 17 * * *", async () => {
//   console.log("🔄 Running 5 PM evening follow-up cronjob...");

//   const cutoff = new Date(Date.now() - 1 * 60 * 1000); // 1 min ago
//   const inactiveUsers = await Chat.find({
//     lastInteracted: { $lt: cutoff },
//     course: { $ne: null },
//     $or: [
//       { eveningFollowUpCount: { $lt: 3 } },
//       { eveningFollowUpCount: { $exists: false } }
//     ],
//     pipeline: { $not: /^(Converted|Enrolled|Irrelevant|Not Interested|Not Intrested|On Hold)$/i }
//   });

//   for (const user of inactiveUsers) {
//     const whatsappNumber = user.userNumber;
//     const day = (user.eveningFollowUpCount || 0) + 1;

//     try {
//       if (!whatsappNumber) {
//         throw new Error("Missing WhatsApp number");
//       }

//       if (!user.assignedto) {
//         console.warn(`⚠️ No counsellor assigned for ${user.userNumber}, skipping evening follow-up`);
//         continue;
//       }

//       const counsellor = await User.findOne({ username: user.assignedto });
//       if (!counsellor || !counsellor.userNumber) {
//         console.warn(`⚠️ No counsellor number found for assignee "${user.assignedto}", skipping evening follow-up for ${user.userNumber}`);
//         continue;
//       }
//       const counsellorNumber = counsellor.userNumber;
//       const userName = user.userName || "there";

//       let templateName;
//       let botReply;

//       if (day === 1) {
//         await sendCompanyProfileTemplate(whatsappNumber, userName, user.course, counsellorNumber);
//         templateName = "company_profile";
//         botReply = `[Template: company_profile] Sent company profile for "${user.course}" with counsellor contact ${counsellorNumber}`;
//       } else if (day === 2) {
//         const syllabusLink = COURSE_SYLLABUS[user.course];
//         if (!syllabusLink) {
//           console.warn(`⚠️ No syllabus link mapped for course "${user.course}" (${user.userNumber}), skipping evening follow-up`);
//           continue;
//         }
//         await sendSyllabusFollowupTemplate(whatsappNumber, userName, user.course, syllabusLink, counsellorNumber);
//         templateName = "syllabus_followup";
//         botReply = `[Template: syllabus_followup] Sent syllabus link for "${user.course}" with counsellor contact ${counsellorNumber}`;
//       } else {
//         await sendPlacementSupportTemplate(whatsappNumber, userName, counsellorNumber);
//         templateName = "placement_support";
//         botReply = `[Template: placement_support] Sent placement support info with counsellor contact ${counsellorNumber}`;
//       }

//       console.log(`📩 Sent evening follow-up #${day} (${templateName}) to ${user.userNumber}`);

//       user.conversations.push({
//         userMsg: `Evening follow-up #${day} sent`,
//         botReply,
//       });

//       user.eveningFollowUpCount = day;
//       user.lastFollowUpSentAt = new Date();
//       user.followUpAwaitingReply = true;
//       await user.save();
//     } catch (err) {
//       // Bad record (invalid number, WhatsApp API rejection, etc.) - log it and move on
//       // so one bad lead doesn't block the rest of the batch from getting their follow-up.
//       console.error(`❌ Evening follow-up failed for ${user.userNumber || user._id}:`, err.message);
//       try {
//         user.conversations.push({
//           userMsg: `Evening follow-up #${day} attempt failed`,
//           botReply: `⚠️ Skipped: ${err.message}`,
//         });
//         await user.save();
//       } catch (saveErr) {
//         console.error(`❌ Also failed to save error note for ${user.userNumber || user._id}:`, saveErr.message);
//       }
//     }
//   }
// }, {
//   timezone: "Asia/Kolkata",
// });

// 🚨 Counsellor inactivity escalation — runs once daily at 6 PM IST. Notifies
// admin (WhatsApp) of any counsellor (role: "user") who has not messaged the
// WhatsApp number since 12:00 AM IST today, since their 24-hour customer-service
// window (needed for free-form outbound texts) may have lapsed. Admin's own
// number is always treated as within-window, so a plain text send is used.
const ADMIN_ESCALATION_NUMBER = "919597271533";
// 919597271533
cron.schedule("0 11 * * *", async () => {
  console.log("🔄 Running counsellor inactivity escalation cronjob...");
  const EXCLUDED_NUMBERS = ["918667850844", "917397081629"];
  try {
    const counsellors = await User.find({
      role: "user",
      userNumber: { $exists: true, $ne: null, $ne: "",$nin: EXCLUDED_NUMBERS }
    });

    const { start: todayStartIST } = getIstDayUtcRange(0);

    const inactiveCounsellors = [];

    for (const counsellor of counsellors) {
      const number = cleanIndianMobileNumber(counsellor.userNumber);
      const chat = await Chat.findOne({ userNumber: number }, { lastInteracted: 1 }).lean();
      const hasInteractedToday = chat?.lastInteracted && new Date(chat.lastInteracted) >= todayStartIST;

      if (!hasInteractedToday) {
        inactiveCounsellors.push(`${counsellor.username || "Unknown"} (${number})`);
      }
    }
    // console.log

    if (inactiveCounsellors.length > 0) {
      const list = inactiveCounsellors.map((entry, i) => `${i + 1}. ${entry}`).join("\n");
      const message =
        `⚠️ *Inactivity Alert*\n\n` +
        `The following counsellor(s) have not messaged the WhatsApp number since 12:00 AM today:\n\n${list}\n\n` +
        `Their 24-hour WhatsApp messaging window may be closed.`;
      await sendWhatsAppMessage(ADMIN_ESCALATION_NUMBER, message);
      console.log(`📩 Escalated ${inactiveCounsellors.length} inactive counsellor(s) to admin`);
    } else {
      console.log("✅ All counsellors interacted today — no escalation needed");
    }
  } catch (err) {
    console.error("❌ Counsellor inactivity escalation failed:", err.message);
  }
}, {
  timezone: "Asia/Kolkata",
});

// 🎯 Daily sales-training quiz — sends every counsellor a WhatsApp voice
// note + text on a random sales topic, then asks 5 questions via chat
// (see SalesTraining.js). Runs once daily at 3:30 PM IST.
cron.schedule("30 15 * * *", async () => {
  try {
    await sendDailyTrainingToAllUsers();
  } catch (err) {
    console.error("❌ Sales training cronjob failed:", err.message);
  }
}, {
  timezone: "Asia/Kolkata",
});


server.listen(8001);
