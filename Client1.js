require('dotenv').config();
const OpenAI = require("openai");
const ACCESS_TOKEN = process.env.WHATSAPP_TOKEN;
const Client_1 = process.env.Client_1 || "981784061688639";
const { User, Chat, Job } = require("./Schema");
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

let Knowledge_base = ``

let Placement_ofc_course = ``


async function typingindicator(to, messageId) {
  const baseUrl = `https://graph.facebook.com/v24.0/${Client_1}/messages`;

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







async function sendMessageClient1(to, text) {
  if (!ACCESS_TOKEN) {
    console.error("Missing WHATSAPP_TOKEN for Client1 message send");
    return;
  }

  await fetch(`https://graph.facebook.com/v24.0/${Client_1}/messages`, {
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


// async function demo_minmini(body) {

//   try {
//     const entry = body.entry?.[0];
//     const changes = entry?.changes?.[0];
//     const message = changes?.value?.messages?.[0];


//     if (!message) {
//       console.log("No message found");
//       return;
//     }

//     const from = message.from;
//     if (!from) {
//       console.log("No sender found");
//       return;
//     }

//     const messageId = message.id;

//     await typingindicator(from,messageId);

//     let msgBody = "";
//     if (message.type === "text" && message.text?.body) {
//       msgBody = message.text.body.trim();
//     }

//     let userChat = await Chat.findOne({ userNumber: from });

//     if(userChat){
//       sendMessageClient1(from,"Please register course from Appin Technology")
//     }

//     let selected_course = userChat.course


//     if (!msgBody) {
//       console.log("Empty message body");
//       return;
//     }

//     const messages = [
//       {
//         role: "system",
//         content: `You are Ai Assitant, the friendly and persuasive AI assistant for Nuovafil & Infoteck Pvt. Ltd. use company details for answer \n ${Knowledge_base}`,
//       },
//       { role: "user", content: msgBody },
//     ];

//     const response = await openai.chat.completions.create({
//       model: "gpt-4o-mini",
//       messages,
//     });

//     const reply = response?.choices?.[0]?.message?.content || "Sorry, I could not process that right now.";
//     await sendMessageClient1(from, reply);

//   } catch (err) {
//     console.error("Webhook Error:", err);
//   }
// }


async function demo_minmini(body) {

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

    await typingindicator(from, messageId);

    let msgBody = "";
    if (message.type === "text" && message.text?.body) {
      msgBody = message.text.body.trim();
    }

    let userChat = await Chat.findOne({ userNumber: from });

    if (!userChat) {
     sendMessageClient1(from, "Hi! It looks like you are not currently registered as an Appin Technology student. Please register for a course with Appin Technology to access this service.");
      return 
    }

    let selected_course = userChat ? userChat.course : null;
    Placement_ofc_course = selected_course


    if (!msgBody) {
      console.log("Empty message body");
      return;
    }

    // --- JOB ENQUIRY LOGIC ---
    const isJobEnquiry = /\b(job|jobs|vacancy|vacancies|career|hiring|opportunities|opportunity|internship)\b/i.test(msgBody);

    if (isJobEnquiry && selected_course) {
      let skill = "";
      const courseLower = selected_course.toLowerCase();

      if (courseLower.includes("python")) {
        skill = "Python";
      } else if (courseLower.includes("java")) {
        skill = "java";
      } else if (courseLower.includes("mern")) {
        skill = "Mern";
      } else if (courseLower.includes("digital marketting") || courseLower.includes("digital marketing")) {
        skill = "Digital marketting";
      } else if (courseLower.includes("data science")) {
        skill = "data science";
      }
      else if (courseLower.includes("data analytics")) {
        skill = "data analytics";
      }

      if (skill) {
        const jobs = await Job.find({ skill: { $regex: new RegExp(`^${skill}$`, "i") } })
          .sort({ scrapedAt: -1 });
          // .limit(5);

        if (jobs.length > 0) {
          let jobText = `Here are some recent *${skill}* jobs related to your course:\n\n`;
          jobs.forEach((j, index) => {
            jobText += `*${index + 1}. ${j.title}*\n🏢 ${j.company}\n📍 ${j.location}\n🔗 ${j.url}\n\n`;
          });
          await sendMessageClient1(from, jobText);
          return; // Stop here so AI doesn't reply again
        } else {
          await sendMessageClient1(from, `Currently, we don't have new ${skill} job listings. Please check back soon!`);
          return;
        }
      }
    }
    // -------------------------

    const messages = [
      {
        role: "system",
        content: `You are Ai job Assitant for Appin technology,  \n ${Knowledge_base}`,
      },
      { role: "user", content: msgBody },
    ];

    const response = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages,
    });

    const reply = response?.choices?.[0]?.message?.content || "Sorry, I could not process that right now.";
    await sendMessageClient1(from, reply);

  } catch (err) {
    console.error("Webhook Error:", err);
  }
}

module.exports = {
  demo_minmini,
  sendMessageClient1,
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
  Knowledge_base = demo_know

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




const demo_know = `
You are the Placement Officer at Appin Technology. 
Your role is to guide students regarding placement assistance.

🎯 Primary Goal:
Inform, motivate, and guide students about placement support provided by Appin Technology.

🧭 Guidelines:
1. Never reveal that you are an AI model.
2. Always refer to Appin Technology as "we".
3. Keep responses concise, warm, and professional.
4. Only talk about Appin Technology and its offerings.
5. Personalize answers based on the student’s selected course: ${Placement_ofc_course || "Python"}.
6. Use the company information provided below when answering questions.

💬 Response Structure:
1. Politely acknowledge the user’s query.
2. Provide clear, accurate, and friendly information relevant to the current step.
3. Include a short motivational insight (for example: student success stories or career growth after the course).

📋 Information to Share (Follow this order strictly):

1. Job opportunities related to the selected course.
2. Trainer profile for the course.
3. Certification provided after completing the course.

✨ Tone:
Friendly, encouraging, and confident. 
Always focus on guiding the student toward learning and career success with Appin Technology.

`;


