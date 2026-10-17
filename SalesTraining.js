require('dotenv').config();
const OpenAI = require("openai");
const { User, Chat, SalesTrainingSession } = require("./Schema");

const ACCESS_TOKEN = process.env.WHATSAPP_TOKEN;
const PHONE_NUMBER_ID = process.env.PHONE_NUMBER_ID || "825569433970567";
const GRAPH_API = "https://graph.facebook.com/v20.0";

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// Flip to false to pause the daily training send without touching the cron
// job registration in index.js.
const SALES_TRAINING_ENABLED = true;

// How many of a user's most recent topics to avoid repeating.
const TOPIC_HISTORY_WINDOW = 30;

// 100 educational-sales topics for course counsellors/admin staff. Picked
// randomly per user each day, skipping their recent history where possible.
const SALES_TRAINING_TOPICS = [
  "Making a strong first impression on a new lead",
  "Effective cold-calling scripts for course enquiries",
  "Qualifying a lead's genuine interest quickly",
  "Building rapport in the first 60 seconds",
  "Asking the right discovery questions before pitching",
  "Identifying a lead's real motivation for joining a course",
  "Converting Instagram and social media enquiries",
  "Handling walk-in enquiries professionally",
  "Writing a first WhatsApp message that gets a reply",
  "Setting the tone for a trustworthy conversation",
  "Consultative selling versus pushy selling",
  "Uncovering a lead's career goals before pitching a course",
  "Matching the right course to the right learner",
  "Active listening during a sales conversation",
  "Reading buying signals in a lead's messages",
  "Understanding parent vs student decision-making dynamics",
  "Selling to working professionals versus fresh students",
  "Identifying the real objection behind 'I'll think about it'",
  "Mapping objections to their root cause",
  "Building a buyer persona for course enquiries",
  "Handling the objection 'the course is too expensive'",
  "Explaining the ROI of a certification course clearly",
  "Justifying course fees without sounding defensive",
  "Positioning EMI and installment options confidently",
  "Comparing course value against free YouTube resources",
  "Handling 'another institute is cheaper' objections",
  "Negotiating fees without devaluing the course",
  "Explaining placement assistance as part of course value",
  "Addressing 'is this course accredited' concerns",
  "Framing course cost as a career investment",
  "Handling 'I don't have time for this course' objection",
  "Responding to 'I need to ask my parents first'",
  "Overcoming 'I'm not sure this is right for me'",
  "Handling silence from a previously interested lead",
  "Re-engaging a lead who stopped responding",
  "Addressing skepticism about online reviews or reputation",
  "Building confidence in career switchers",
  "Handling 'I already know the basics' objection",
  "Responding to 'will I actually get a job after this'",
  "Easing hesitation from first-generation learners",
  "Creating urgency ethically around batch start dates",
  "Using limited-seats messaging responsibly",
  "The assumptive closing technique for warm leads",
  "The alternative-choice closing technique",
  "Closing a sale over a WhatsApp conversation",
  "Closing a sale over a phone call",
  "Converting a 'maybe' into a confident 'yes'",
  "Recognizing the right moment to ask for commitment",
  "Avoiding pressure tactics that damage trust",
  "Following up on a promised decision date",
  "Designing an effective follow-up cadence",
  "Writing a follow-up message that doesn't feel repetitive",
  "Nurturing a cold lead back to interest",
  "Following up after a missed demo class",
  "Sending value instead of just reminders in follow-ups",
  "Using success stories in follow-up conversations",
  "Handling 'I'll call you back' politely but firmly",
  "Setting clear next steps at the end of every call",
  "Tracking and honoring follow-up promises",
  "Reviving a dead lead after 30 days",
  "Using testimonials to build instant credibility",
  "Storytelling to make a course pitch memorable",
  "Positioning trainer expertise in the pitch",
  "Explaining curriculum value in simple terms",
  "Sharing placement success stories effectively",
  "Building long-term trust beyond a single sale",
  "Ethical selling practices in education",
  "Being transparent about what a course does not cover",
  "Using data and outcomes to support claims",
  "Building personal rapport quickly with strangers",
  "Tonality and enthusiasm without sounding scripted",
  "Mirroring a lead's communication style",
  "WhatsApp etiquette for professional conversations",
  "Using voice notes for a personal touch",
  "Simplifying technical course details for non-technical leads",
  "Staying warm and human across repeated calls",
  "Handling an angry or frustrated lead calmly",
  "Managing multiple stakeholders in one decision",
  "Communicating confidently without over-promising",
  "Reading tone in text-based conversations",
  "Upselling an existing student to an advanced batch",
  "Cross-selling a complementary course",
  "Asking for referrals at the right moment",
  "Building a referral program pitch",
  "Turning a satisfied student into a brand advocate",
  "Reactivating alumni for new course offerings",
  "Handling fee refund conversations gracefully",
  "Explaining certificate value to future employers",
  "Selling internship opportunities alongside a course",
  "Retaining a student who wants to drop out",
  "Prioritizing hot leads over cold ones",
  "CRM discipline and accurate lead notes",
  "Meeting monthly targets without compromising ethics",
  "Emotional intelligence during high-pressure sales weeks",
  "Bouncing back from a string of rejections",
  "Time management across a large lead list",
  "Preparing for a demo class that converts",
  "Converting webinar attendees into enrolled students",
  "Handling group decisions among friends enrolling together",
  "Continuous learning to stay sharp as a sales counsellor",
];

function normalizeIndianNumber(input) {
  if (!input) return null;
  const digits = input.toString().replace(/\D/g, '');
  if (digits.startsWith("91") && digits.length === 12) return digits;
  if (digits.length === 10) return "91" + digits;
  if (digits.length === 11 && digits.startsWith("0")) return "91" + digits.slice(1);
  return digits;
}

// Pulls this counsellor's lead activity for today: new leads received,
// remarks they logged, and follow-ups missed (a reminder due today with no
// remark logged against that lead today). Returns null if the user isn't
// assigned any leads (e.g. admin accounts) so callers can fall back to the
// plain generic training with no behavior change.
async function getDailyLeadStatsForUser(username) {
  if (!username) return null;

  const start = new Date();
  start.setHours(0, 0, 0, 0);
  const end = new Date();
  end.setHours(23, 59, 59, 999);

  const leads = await Chat.find({
    assignedto: username,
    $or: [
      { datecreated: { $gte: start, $lte: end } },
      { reminder: { $gte: start, $lte: end } },
      { "remarks.timestamp": { $gte: start, $lte: end } },
    ],
  }).select("datecreated reminder remarks").lean();

  if (leads.length === 0) return null;

  let newLeadsToday = 0;
  let remindersDueToday = 0;
  let remindersMissedToday = 0;
  const remarksToday = [];

  for (const lead of leads) {
    if (lead.datecreated && lead.datecreated >= start && lead.datecreated <= end) {
      newLeadsToday += 1;
    }

    const leadRemarksToday = (lead.remarks || []).filter(
      r => r.remark && r.timestamp && r.timestamp >= start && r.timestamp <= end
    );
    leadRemarksToday.forEach(r => remarksToday.push(r.remark));

    if (lead.reminder && lead.reminder >= start && lead.reminder <= end) {
      remindersDueToday += 1;
      if (leadRemarksToday.length === 0) remindersMissedToday += 1;
    }
  }

  return {
    newLeadsToday,
    remarksLoggedToday: remarksToday.length,
    remindersDueToday,
    remindersMissedToday,
    sampleRemarks: remarksToday.slice(0, 5),
  };
}

// Turns today's lead stats into a short brief the OpenAI prompt can use to
// personalize part of the training. Returns "" when there's nothing
// meaningful to report, so the caller can skip personalization entirely.
function buildPersonalizedBrief(stats) {
  if (!stats) return "";
  const { newLeadsToday, remarksLoggedToday, remindersDueToday, remindersMissedToday, sampleRemarks } = stats;

  if (newLeadsToday === 0 && remarksLoggedToday === 0 && remindersDueToday === 0) return "";

  const parts = [];
  parts.push(`New leads received today: ${newLeadsToday}.`);
  parts.push(`Remarks logged today: ${remarksLoggedToday}.`);
  if (remindersDueToday > 0) {
    parts.push(`Follow-ups due today: ${remindersDueToday}, of which ${remindersMissedToday} were missed (no remark logged on that lead today).`);
  }
  if (sampleRemarks.length > 0) {
    parts.push(`Sample remarks they logged today: ${sampleRemarks.map(r => `"${r}"`).join("; ")}.`);
  }
  return parts.join(" ");
}

async function pickTopicForUser(userNumber) {
  const recentSessions = await SalesTrainingSession.find({ userNumber })
    .sort({ startedAt: -1 })
    .limit(TOPIC_HISTORY_WINDOW)
    .select("topic")
    .lean();

  const recentTopics = new Set(recentSessions.map(s => s.topic));
  const unusedTopics = SALES_TRAINING_TOPICS.filter(t => !recentTopics.has(t));
  const pool = unusedTopics.length > 0 ? unusedTopics : SALES_TRAINING_TOPICS;

  return pool[Math.floor(Math.random() * pool.length)];
}

// One OpenAI call: a short spoken-style script introducing the topic, plus
// 5 quiz questions about it, as JSON. When personalizedBrief is non-empty,
// roughly 30% of the script and one quiz question are grounded in that
// counsellor's actual leads/remarks/missed-follow-ups from today.
async function buildTrainingContent(topic, personalizedBrief = "") {
  const systemPrompt = `You are a sales trainer creating a short daily training session for course counsellors at Appin Technology, an education/training institute. Given a topic, produce:
1. "script": a warm, spoken-style script (120-160 words) explaining the topic and how to apply it when selling courses to students/parents. Written to be read aloud naturally (no bullet points, no markdown).
2. "questions": exactly 5 short quiz questions (each one sentence) that test understanding of the topic and how a counsellor would apply it in a real sales conversation.
${personalizedBrief ? `
This counsellor's actual activity today: ${personalizedBrief}
Use this to personalize roughly 30% of the script — one to two sentences near the end that reference these specifics directly (do not invent details beyond what's given) and give concrete advice on how they can improve, e.g. logging clearer/more actionable remarks, or acting faster on follow-ups so timing isn't missed. Tie it back to today's topic where it fits naturally. Also make exactly one of the 5 quiz questions test this personalized angle (e.g. asking how they'd handle a missed follow-up, or how they'd improve on a remark like the ones given).` : ""}
Respond ONLY with JSON in this exact shape: {"script": "...", "questions": ["...", "...", "...", "...", "..."]}`;

  const response = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    response_format: { type: "json_object" },
    messages: [
      { role: "system", content: systemPrompt },
      { role: "user", content: `Topic: ${topic}` }
    ]
  });

  const parsed = JSON.parse(response.choices[0].message.content);
  if (!parsed.script || !Array.isArray(parsed.questions) || parsed.questions.length !== 5) {
    throw new Error("Malformed training content from OpenAI");
  }
  return parsed;
}

async function scoreTrainingAnswers(topic, questions) {
  const systemPrompt = `You are grading a sales counsellor's quiz answers on the topic "${topic}" from a course-sales training session. For each question/answer pair, score 1 if the answer shows reasonable understanding and effort relevant to the question, or 0 if it is blank, irrelevant, or nonsensical. Give one short sentence of feedback per answer.

Respond ONLY with JSON in this exact shape: {"results": [{"score": 0 or 1, "feedback": "..."}, ...]} with exactly ${questions.length} entries, in the same order as given.`;

  const userContent = questions
    .map((q, i) => `Q${i + 1}: ${q.text}\nA${i + 1}: ${q.answer || "(no answer)"}`)
    .join("\n\n");

  const response = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    response_format: { type: "json_object" },
    messages: [
      { role: "system", content: systemPrompt },
      { role: "user", content: userContent }
    ]
  });

  const parsed = JSON.parse(response.choices[0].message.content);
  if (!Array.isArray(parsed.results) || parsed.results.length !== questions.length) {
    throw new Error("Malformed scoring result from OpenAI");
  }
  return parsed.results;
}

// async function textToSpeechOpus(script) {
//   const speech = await openai.audio.speech.create({
//     model: "tts-1",
//     voice: "alloy",
//     input: script,
//     response_format: "opus",
//   });
//   const arrayBuffer = await speech.arrayBuffer();
//   return Buffer.from(arrayBuffer);
// }

// async function uploadMediaToWhatsApp(buffer, mimeType, filename) {
//   const form = new FormData();
//   form.append("messaging_product", "whatsapp");
//   form.append("type", mimeType);
//   form.append("file", new Blob([buffer], { type: mimeType }), filename);

//   const res = await fetch(`${GRAPH_API}/${PHONE_NUMBER_ID}/media`, {
//     method: "POST",
//     headers: { Authorization: `Bearer ${ACCESS_TOKEN}` },
//     body: form,
//   });
//   const data = await res.json();
//   if (!data.id) {
//     throw new Error(`WhatsApp media upload failed: ${JSON.stringify(data)}`);
//   }
//   return data.id;
// }

// async function sendWhatsAppAudio(to, mediaId) {
//   const res = await fetch(`${GRAPH_API}/${PHONE_NUMBER_ID}/messages`, {
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
//   const data = await res.json();
//   if (data.error) {
//     throw new Error(`WhatsApp audio send failed: ${JSON.stringify(data.error)}`);
//   }
// }

async function sendWhatsAppText(to, text) {
  const res = await fetch(`${GRAPH_API}/${PHONE_NUMBER_ID}/messages`, {
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
  const data = await res.json();
  if (data.error) {
    throw new Error(`WhatsApp text send failed: ${JSON.stringify(data.error)}`);
  }
}

// Starts one training session for a single user: picks a topic, generates
// the script + questions, sends the voice note + text, and creates the
// in_progress SalesTrainingSession. Any prior unfinished session for that
// user is marked "abandoned" first, so today's training always goes out
// and handleTrainingReply has only one in_progress session to find.
async function startTrainingSession(user) {
  const number = normalizeIndianNumber(user.userNumber);
  if (!number) return;

  const existing = await SalesTrainingSession.findOne({ userNumber: number, status: "in_progress" });
  if (existing) {
    console.log(`♻️ Sales training: closing out unfinished session for ${number} before starting today's`);
    existing.status = "abandoned";
    await existing.save();
  }

  const topic = await pickTopicForUser(number);

  let personalizedBrief = "";
  try {
    const stats = await getDailyLeadStatsForUser(user.username);
    personalizedBrief = buildPersonalizedBrief(stats);
  } catch (err) {
    console.error(`⚠️ Sales training: failed to load today's lead stats for ${user.username}:`, err.message);
  }

  const { script, questions } = await buildTrainingContent(topic, personalizedBrief);

  // const audioBuffer = await textToSpeechOpus(script);
  // const mediaId = await uploadMediaToWhatsApp(audioBuffer, "audio/ogg", "training.ogg");
  // await sendWhatsAppAudio(number, mediaId);

  await sendWhatsAppText(
    number,
    `📚 *Today's Sales Training Topic:*\n${topic}\n\n${script}\n\nYou'll be asked 5 quick questions about this — reply one at a time.`
  );
  await sendWhatsAppText(number, `❓ Question 1/5: ${questions[0]}`);

  await SalesTrainingSession.create({
    userNumber: number,
    username: user.username || "",
    topic,
    script,
    questions: questions.map(text => ({ text })),
    currentQuestionIndex: 0,
    status: "in_progress",
  });

  console.log(`📩 Sales training started for ${number} — topic: ${topic}`);
}

// Numbers excluded from the daily sales-training quiz (same exclusion list
// used by the counsellor inactivity escalation cron in index.js).
const EXCLUDED_NUMBERS = ["918667850844", "917397081629"];

// Admin number that should also receive the daily training, alongside
// every counsellor.
const ADMIN_TRAINING_NUMBER = "919597271533";

// Sends today's training to every counsellor (role: "user") with a WhatsApp
async function sendDailyTrainingToAllUsers() {
  if (!SALES_TRAINING_ENABLED) {
    console.log("⏸️ Sales training is disabled — skipping daily send");
    return;
  }

  const users = await User.find({
    role: "user",
    userNumber: { $exists: true, $ne: null, $ne: "", $nin: EXCLUDED_NUMBERS },
  });

  users.push({ userNumber: ADMIN_TRAINING_NUMBER, username: "admin" });

  console.log(`🔄 Running sales training cronjob for ${users.length} user(s)...`);

  for (const user of users) {
    try {
      await startTrainingSession(user);
    } catch (err) {
      console.error(`⚠️ Sales training failed for ${user.userNumber}:`, err.message);
    }
  }
}

// Called from the webhook for every inbound text message. If the sender has
// an in-progress training session, records the answer, advances to the next
// question or scores+completes the session, and returns true (handled).
// Returns false if there's no active session, so the caller can fall
// through to the normal lead-chatbot flow.
async function handleTrainingReply(from, msgBody) {
  const number = normalizeIndianNumber(from);
  if (!number || !msgBody) return false;

  const session = await SalesTrainingSession.findOne({ userNumber: number, status: "in_progress" });
  if (!session) return false;

  const idx = session.currentQuestionIndex;
  if (idx >= session.questions.length) return false;

  session.questions[idx].answer = msgBody;
  session.currentQuestionIndex += 1;

  if (session.currentQuestionIndex < session.questions.length) {
    await session.save();
    await sendWhatsAppText(
      number,
      `❓ Question ${session.currentQuestionIndex + 1}/5: ${session.questions[session.currentQuestionIndex].text}`
    );
    return true;
  }

  // All 5 answered — score and close out the session.
  try {
    const results = await scoreTrainingAnswers(session.topic, session.questions);
    results.forEach((r, i) => {
      session.questions[i].score = r.score;
      session.questions[i].feedback = r.feedback;
    });
    session.totalScore = results.reduce((sum, r) => sum + (r.score || 0), 0);
  } catch (err) {
    console.error(`⚠️ Sales training scoring failed for ${number}:`, err.message);
    session.totalScore = null;
  }

  session.status = "completed";
  session.completedAt = new Date();
  await session.save();

  const summaryLines = session.questions
    .map((q, i) => `${i + 1}. ${q.feedback || (q.score ? "Good answer." : "Needs improvement.")}`)
    .join("\n");

  const scoreLine = session.totalScore === null
    ? "We couldn't score your answers automatically this time."
    : `Your Score: *${session.totalScore}/5*`;

  await sendWhatsAppText(
    number,
    `✅ *Training Complete!*\nTopic: ${session.topic}\n${scoreLine}\n\n${summaryLines}\n\nSee you in tomorrow's session!`
  );

  return true;
}

function getISTDateString(d = new Date()) {
  return d.toLocaleDateString("en-CA", { timeZone: "Asia/Kolkata" });
}

// Start/end instants (as real Dates) of the IST calendar day for a
// "YYYY-MM-DD" string, for filtering the startedAt timestamp field.
function istDayBounds(dateStr) {
  const start = new Date(`${dateStr}T00:00:00+05:30`);
  const end = new Date(`${dateStr}T23:59:59.999+05:30`);
  return { start, end };
}

// ─── Manual trigger (admin "Trigger Now" button in Sales Training) ─────
// Same eligibility + per-user send as the 3:30 PM cron above, just invoked on
// demand instead of on a schedule. Skips anyone who already has a training
// session started today, so pressing the button twice — or pressing it after
// the 3:30 PM cron already ran — never double-sends.
async function triggerSalesTrainingNow() {
  if (!SALES_TRAINING_ENABLED) {
    return { disabled: true, eligible: 0, started: 0, alreadyStarted: 0, failed: 0, failures: [] };
  }

  const users = await User.find({
    role: "user",
    userNumber: { $exists: true, $ne: null, $ne: "", $nin: EXCLUDED_NUMBERS },
  }).lean();
  users.push({ userNumber: ADMIN_TRAINING_NUMBER, username: "admin" });

  const { start, end } = istDayBounds(getISTDateString());
  const existingToday = new Set(
    (await SalesTrainingSession.find(
      { startedAt: { $gte: start, $lte: end } },
      { userNumber: 1 }
    ).lean()).map((s) => s.userNumber)
  );

  let started = 0;
  let alreadyStarted = 0;
  const failures = [];

  for (const user of users) {
    const number = normalizeIndianNumber(user.userNumber);
    if (!number) continue;
    if (existingToday.has(number)) {
      alreadyStarted++;
      continue;
    }
    try {
      await startTrainingSession(user);
      started++;
    } catch (err) {
      failures.push({ username: user.username, message: err.message });
      console.error(`⚠️ Manual sales training trigger failed for ${user.userNumber}:`, err.message);
    }
  }

  return { disabled: false, eligible: users.length, started, alreadyStarted, failed: failures.length, failures };
}

module.exports = {
  sendDailyTrainingToAllUsers,
  handleTrainingReply,
  startTrainingSession,
  triggerSalesTrainingNow,
  SALES_TRAINING_TOPICS,
};
