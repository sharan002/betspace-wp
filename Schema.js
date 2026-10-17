const mongoose = require("mongoose");
const conversationSchema = new mongoose.Schema({
  userMsg: { type: String, required: true },
  botReply: { type: String, required: true },
  timestamp: { type: Date, default: Date.now }
});

const remarksSchema = new mongoose.Schema({
  remark: String,
  timestamp: { type: Date, default: Date.now },
});

const userSchema = new mongoose.Schema({
  username: { type: String },
  useremail: { type: String, unique: true, required: true },
  userNumber: { type: String, required: true, unique: true }, 
  password: { type: String },
  role: { 
    type: String, 
    enum: ["admin", "user"], 
    default: "user" 
  }
});


const InternSchema = new mongoose.Schema({
  salutation: { type: String, trim: true, default: "" },
  userName: { type: String, trim: true, default: "" },
  userEmail: { type: String, trim: true, lowercase: true, default: "" },
  phone: { type: String, trim: true, default: "" },
  reg_no: { type: String, trim: true, default: "" },
  year_of_study: { type: String, trim: true, default: "" },
  college: { type: String, trim: true, default: "" },
  college_location: { type: String, trim: true, default: "" },
  degree: { type: String, trim: true, default: "" },
  other_degree: { type: String, trim: true, default: "" },
  department: { type: String, trim: true, default: "" },
  internship: { type: String, trim: true, default: "" },
  other_internship: { type: String, trim: true, default: "" },
  workshop: { type: String, trim: true, default: "" },
  other_workshop: { type: String, trim: true, default: "" },
  start_date: { type: Date, default: null },
  end_date: { type: Date, default: null },
  date_scheduled: { type: Date, default: null },
  issue_date: { type: Date, default: null },
  company: { type: String, trim: true, default: "" },
  batch_code: { type: String, trim: true, default: "" }
}, { timestamps: true });



const feesSchema = new mongoose.Schema({
  billedAmount: { type: Number, default: 0 },
  installments: [
    {
      number: Number,
      dueDate: Date,
      amount: Number,
      status: {
        type: String,
        enum: ["Pending", "Paid"],
        default: "Pending"
      },
         billGenerated: { type: Boolean, default: false },
         billGeneratedAt: { type: Date, default: null },
         billUrl: { type: String, default: null }
    }
  ]
});

const chatSchema = new mongoose.Schema({
  userName: { type: String },
  userNumber: { type: String, required: true, unique: true },
  courseofintrest: { type: String },
  course: { type: String, default: null },
  leadfrom: { type: String },
  // Meta lead_id (a.k.a. leadgen_id) from the Lead Ads webhook — used by the CRM to
  // report "Qualified Lead" events back to Meta via the Conversions API for Leads.
  leadgenId: { type: String, default: null },
  assignedto: { type: String },
  batchId: { type: String, default: null },
  conversations: [conversationSchema],
  programType: { type: String },
  city: { type: String },
  profession: { type: String },
  location: { type: String },
  status: { type: String, default: "Cold" },
  pipeline: { type: String, default: "New Lead" },
  remarks: [remarksSchema],
  reminder: { type: Date, default: null },
  batchId: { type: String, default: null },
  Trainer: { type: String, default: null },
  batch_time : { type: String, default: null },
  fees: { type: feesSchema, default: () => ({}) },
  followUpCount: { type: Number, default: 0 },
  eveningFollowUpCount: { type: Number, default: 0 },
  // True once the 4th-day morning web-search follow-up has been sent, so it
  // fires exactly once per lead after the standard 3-day (10 AM) cycle ends.
  day4FollowUpSent: { type: Boolean, default: false },
  respondedAfterFollowUp: { type: Boolean, default: false },
  // True once the /email/demobooking webhook fires for this lead. Drives the
  // CRM dashboard's red "Demo Booked" dot on the lead row.
  demoBooked: { type: Boolean, default: false },
  demoBookedAt: { type: Date, default: null },
  // True once a follow-up (10 AM or 5 PM cron) has been sent and the lead has
  // not yet replied to it. Cleared in the webhook when the reply comes in.
  followUpAwaitingReply: { type: Boolean, default: false },
  lastFollowUpSentAt: { type: Date, default: null },
  noRemarkEscalationCount: { type: Number, default: 0 },
  lastNoRemarkEscalationAt: { type: Date, default: null },
  datecreated: { type: Date, default: Date.now },
  lastInteracted: { type: Date, default: Date.now },
  aiAnalysis: { type: Object, default: null },
  InternDetails : [InternSchema],
  certificates: [
    {
      success: { type: Boolean, default: false },
      fileName: { type: String, default: null },
      message: { type: String, default: null },
      s3: { type: mongoose.Schema.Types.Mixed, default: null }
    }
  ],
  is_leader : { type: Boolean, default: false },
  password: { type: String, default: null },
  campaignHistory: [{
    campaignName: { type: String, required: true },
    message: { type: String, default: null },
    sentAt: { type: Date, default: Date.now },
    repliedAt: { type: Date, default: null }
  }]
});

// Staging area for bulk-Excel WhatsApp campaign contacts that are not yet real leads.
// Promoted into a Chat doc (and removed from here) once the contact replies on WhatsApp.
const tempLeadSchema = new mongoose.Schema({
  userName: { type: String, default: "Unknown" },
  userNumber: { type: String, required: true, unique: true },
  campaignName: { type: String, required: true },
  message: { type: String, default: null },
  sentAt: { type: Date, default: Date.now },
  status: { type: String, enum: ["sent", "failed"], default: "sent" },
  source: { type: String, default: "bulk-excel" },
  createdAt: { type: Date, default: Date.now }
});

// --- Static Method for Round Robin Assignment ---
chatSchema.statics.getNextAssignee = async function() {
  const User = mongoose.model("User");

  const db = mongoose.connection.db;
  const excludedConfig = db ? await db.collection('configs').findOne({ key: 'auto_assign_excluded_staffs' }) : null;
  const excludedStaffs = excludedConfig ? excludedConfig.value : [];

  const allStaffs = await User.find({ role: 'user' }).sort({ username: 1 });
  const staffs = allStaffs.filter(s => !excludedStaffs.includes(s.username));
  if (staffs.length === 0) return null;

  const staffUsernames = staffs.map(s => s.username);

  // Find the last lead assigned to one of our active staff members
  const lastAssignedLead = await this.findOne({ assignedto: { $in: staffUsernames } })
    .sort({ datecreated: -1 });

  if (!lastAssignedLead || !lastAssignedLead.assignedto) return staffs[0].username;

  const lastIndex = staffUsernames.indexOf(lastAssignedLead.assignedto);
  const nextIndex = (lastIndex + 1) % staffs.length;
  return staffs[nextIndex].username;
};

// --- Pre-Save Hook for Auto Assignment ---
chatSchema.pre('save', async function(next) {
  console.log("pre reached")
  this.$locals.wasNew = this.isNew;

  if (this.isNew && (!this.assignedto || this.assignedto.trim() === "")) {
    try {
      const db = mongoose.connection.db;
      if (db) {
        const config = await db.collection('configs').findOne({ key: 'auto_assign_enabled' });
        const autoAssignEnabled = config ? !!config.value : true;

        if (autoAssignEnabled) {
          const assignee = await this.constructor.getNextAssignee();
          if (assignee) {
            console.log(assignee)
            this.assignedto = assignee;
          }
        }
      }
    } catch (err) {
      console.error("Auto-assign hook error:", err);
    }
  }
  next();
});


chatSchema.pre("findOneAndUpdate", async function (next) {
  // get the document BEFORE update
  const docToUpdate = await this.model.findOne(this.getQuery()).lean();

  // store previous value on the query object
  this._oldAssignedTo = docToUpdate?.assignedto;

  next();
});


chatSchema.post("findOneAndUpdate", async function (doc) {
  if (!doc) return;

  const update = this.getUpdate();

  // 🧠 safely extract assignedto
  const newAssignedTo =
    update?.$set?.assignedto ??
    update?.assignedto ??
    null;

  // 🔐 if assignedto not part of update → DO NOTHING
  if (newAssignedTo === null) return;

  const oldAssignedTo = this._oldAssignedTo;

  // 🔐 notify ONLY if value actually changed
  if (!oldAssignedTo || oldAssignedTo === newAssignedTo) return;

  try {
    await NotifyCounsellor({
      leadId: doc._id,
      assignedTo: newAssignedTo,
      userNumber: doc.userNumber,
      userName: doc.userName,
      message: `Lead assigned to ${newAssignedTo}`
    });
  } catch (err) {
    console.error("NotifyCounsellor failed:", err);
  }
});

chatSchema.post("save", async function (doc) {
  // 🔐 notify ONLY when document is newly created
  console.log("post reached")
  if (!doc.$locals.wasNew) return;
  console.log("new user post reached")
  try {
    await NotifyCounsellor({
      leadId: doc._id,
      assignedTo: doc.assignedto,
      userNumber: doc.userNumber,
      userName: doc.userName,
      message: `New lead assigned to ${doc.assignedto}`
    });
  } catch (err) {
    console.error("NotifyCounsellor failed:", err);
  }
});

async function NotifyCounsellor(body) {
  console.log("Notify payload:", body);
  console.log("AssignedTo:", body.assignedTo);

  const response = await fetch(
    `${process.env.WP_CHAT_SERVICE_URL}/notify/crm`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    }
  );

  const data = await response.json();
  console.log("Microservice response:", data);

  return data;
}


const trainerSchema = new mongoose.Schema({
  name: { type: String, required: true },
  mobile: { type: String, required: true, unique: true },
  username: { type: String, unique: true, sparse: true },
  password: { type: String, default: null },
  batchIds: [{ type: String }]
});

const trainerTaskSchema = new mongoose.Schema({
  studentId: { type: String, required: true },
  trainerId: { type: String, required: true },
  batchId: { type: String },
  title: { type: String, required: true },
  description: { type: String, default: '' },
  driveLink: { type: String, default: '' },
  studentRemark: { type: String, default: '' },
  trainerRemark: { type: String, default: '' },
  status: { type: String, enum: ['pending', 'completed'], default: 'pending' },
  createdAt: { type: Date, default: Date.now }
});

const trainerTopicSchema = new mongoose.Schema({
  title: { type: String, required: true },
  trainerId: { type: String, required: true },
  batchId: { type: String, required: true },
  // Used for syllabus progress tracking (Frontend/Backend + tech like React/Node/Java/Python)
  category: { type: String, enum: ['Frontend', 'Backend'], default: null },
  tech: { type: String, default: null },
  // New syllabus model (course -> topic -> subtopics)
  course: { type: String, default: null },
  topic: { type: String, default: null },
  subtopics: { type: [String], default: [] },
  date: { type: Date, default: Date.now }
});

const trainerTopicFeedbackSchema = new mongoose.Schema({
  topicId: { type: String, required: true },
  studentId: { type: String, required: true },
  status: { type: String, enum: ['pending', 'approved', 'denied'], default: 'pending' },
  helpComment: { type: String, default: '' },
  trainerReply: { type: String, default: '' },
  resolution: { type: String, enum: ['none', 'awaiting_trainer', 'awaiting_student', 'resolved'], default: 'none' },
  updatedAt: { type: Date, default: Date.now }
});

const announcementSchema = new mongoose.Schema({
  trainerId: { type: String, required: true },
  batchId: { type: String, required: true },
  title: { type: String, required: true },
  message: { type: String, required: true },
  createdAt: { type: Date, default: Date.now }
});

const attendanceSchema = new mongoose.Schema({
  trainerId: { type: String, required: true },
  batchId: { type: String, required: true },
  date: { type: Date, default: Date.now },
  studentId: { type: String, required: true },
  status: { type: String, enum: ['present', 'absent', 'late'], default: 'absent' },
  topicId: { type: String, default: null }
});

const studentTopicProgressSchema = new mongoose.Schema({
  studentId: { type: String, required: true },
  trainerId: { type: String, required: true },
  batchId: { type: String, required: true },
 category: { type: String, default: null },
  tech: { type: String, default: null },
  topicTitle: { type: String, required: true },
  level: { type: String, enum: ['Beginner', 'Intermediate', 'Advanced'], default: 'Beginner' },
  notes: { type: String, default: '' },
  completedAt: { type: Date, default: Date.now }
});

const studentTransferHistorySchema = new mongoose.Schema({
  studentId: { type: String, required: true },
  fromTrainerId: { type: String, default: null },
  toTrainerId: { type: String, required: true },
  fromBatchId: { type: String, default: null },
  toBatchId: { type: String, required: true },
  reason: { type: String, default: '' },
  transferredAt: { type: Date, default: Date.now }
});

// Daily WhatsApp "manager" check-in conversation with a counsellor (role: "user").
// Owned/written by CRM-app-be; wpchat-api only reads this to route an incoming
// counsellor reply to the manager conversation instead of the lead chatbot.
const managerCheckInSchema = new mongoose.Schema({
  counsellorUsername: { type: String, required: true },
  counsellorNumber: { type: String, required: true },
  date: { type: String, required: true },
  metrics: { type: Object, default: {} },
  messages: [{
    role: { type: String, enum: ["manager", "counsellor"] },
    text: String,
    timestamp: { type: Date, default: Date.now }
  }],
  questionCount: { type: Number, default: 0 },
  status: { type: String, enum: ["in_progress", "completed", "no_response"], default: "in_progress" },
  verdict: { type: String, enum: ["SATISFACTORY", "NEEDS_IMPROVEMENT", null], default: null },
  verdictReason: { type: String, default: "" },
  adminSummaryReport: { type: String, default: "" },
  escalatedToAdmin: { type: Boolean, default: false },
  lastManagerMessageAt: { type: Date, default: null },
  nudgeSentAt: { type: Date, default: null },
  startedAt: { type: Date, default: Date.now },
  completedAt: { type: Date, default: null },
});
managerCheckInSchema.index({ counsellorUsername: 1, date: 1 }, { unique: true });

// Daily sales-training quiz sent to every User (staff) as a WhatsApp voice
// note + text: a topic, a short script, and 5 questions the user answers by
// text. Owned entirely by wpchat-api (see SalesTraining.js).
const salesTrainingQuestionSchema = new mongoose.Schema({
  text: { type: String, required: true },
  answer: { type: String, default: "" },
  score: { type: Number, default: null }, // 0 or 1, set once answered
  feedback: { type: String, default: "" },
}, { _id: false });

const salesTrainingSessionSchema = new mongoose.Schema({
  userNumber: { type: String, required: true },
  username: { type: String, default: "" },
  topic: { type: String, required: true },
  script: { type: String, default: "" },
  questions: [salesTrainingQuestionSchema],
  currentQuestionIndex: { type: Number, default: 0 },
  status: { type: String, enum: ["in_progress", "completed", "abandoned"], default: "in_progress" },
  totalScore: { type: Number, default: null }, // out of 5, set once completed
  startedAt: { type: Date, default: Date.now },
  completedAt: { type: Date, default: null },
});

const User = mongoose.model("User", userSchema);
const Chat = mongoose.model("Chat", chatSchema);
const Trainer = mongoose.model("Trainer", trainerSchema);
const TrainerTask = mongoose.model("TrainerTask", trainerTaskSchema);
const TrainerTopic = mongoose.model("TrainerTopic", trainerTopicSchema);
const TrainerTopicFeedback = mongoose.model("TrainerTopicFeedback", trainerTopicFeedbackSchema);
const Announcement = mongoose.model("Announcement", announcementSchema);
const Attendance = mongoose.model("Attendance", attendanceSchema);
const StudentTopicProgress = mongoose.model("StudentTopicProgress", studentTopicProgressSchema);
const StudentTransferHistory = mongoose.model("StudentTransferHistory", studentTransferHistorySchema);
const TempLead = mongoose.model("TempLead", tempLeadSchema);
const ManagerCheckIn = mongoose.model("ManagerCheckIn", managerCheckInSchema);
const SalesTrainingSession = mongoose.model("SalesTrainingSession", salesTrainingSessionSchema);

module.exports = { User, Chat, Trainer, TrainerTask, TrainerTopic, TrainerTopicFeedback, Announcement, Attendance, StudentTopicProgress, StudentTransferHistory, TempLead, ManagerCheckIn, SalesTrainingSession };

