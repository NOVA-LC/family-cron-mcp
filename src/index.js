#!/usr/bin/env node
// Family Cron MCP for Tyler-Computer LibreChat.
//
// Custom because jolks/mcp-cron is a Go binary built against glibc and
// segfaults silently on Railway's Alpine/musl, AND liao1fan/schedule-task-mcp
// needs Node 22.5+ (node:sqlite) but LibreChat-dev image ships Node 20.
//
// Storage: Mongo `family_reminders` collection in LibreChat's existing
// `test` db. Reuses MONGODB_URI (LibreChat's SENSITIVE_ENV_VARS list blocks
// MONGO_URI substitution; MONGODB_URI is not on that list).
//
// Identity: passed per-process by LibreChat via {{LIBRECHAT_USER_ID}},
// {{LIBRECHAT_USER_USERNAME}}. One stdio child per (user, server) so env
// is stable for the process lifetime.
//
// Semantics — chatbot-shaped, not push:
//   - schedule_reminder saves a row. node-cron registers an in-process
//     callback. When the callback fires, we set status=fired+fired_at.
//   - list_reminders returns pending + fired-but-unacknowledged for the
//     calling user. The user re-engages with chat to see fired reminders.
//   - acknowledge marks fired→done. cancel_reminder removes pending.
//
// Why not push? LibreChat doesn't have a documented chat-injection API
// from MCPs. Pull-on-chat is reliable, survives stdio child restarts, and
// works without changing LibreChat core.

process.stderr.write('[family-cron-mcp] booting v0.1.3\n');

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { MongoClient, ObjectId } from 'mongodb';
import cron from 'node-cron';
import { z } from 'zod';

process.stderr.write('[family-cron-mcp] imports loaded\n');

// Strip Railway's literal-quoted-password template wart.
// Some templates wrap the password in double quotes inside the URI which
// MongoClient won't parse: `mongodb://user:"PASS"@host:port`.
function normalizeMongoUri(uri) {
  if (!uri) return uri;
  return uri.replace(/:"([^"@]+)"@/, ':$1@');
}

const RAW_URI = process.env.MONGODB_URI || process.env.MONGO_URI || 'mongodb://mongo.railway.internal:27017';
const MONGODB_URI = normalizeMongoUri(RAW_URI);
const DB_NAME = process.env.CRON_DB || 'test';
const COLLECTION_NAME = process.env.CRON_COLLECTION || 'family_reminders';

// LibreChat does TWO kinds of MCP spawns:
//   1. INSPECTION on startup — to enumerate tools. No user context.
//      `{{LIBRECHAT_USER_ID}}` env passes through UNSUBSTITUTED as a literal.
//   2. PER-CHAT — when a logged-in user opens the MCP toggle. SHOULD have
//      real values, but in practice LibreChat's substitution is unreliable
//      (only works for certain "blessed" MCPs that were configured at boot).
//      family-cron in real chat sessions ALSO receives literal {{...}}.
// Falls back to a shared "family" user when the substitution doesn't happen.
// That breaks per-user scoping (everyone sees everyone's reminders) but the
// MCP works, which is better than silently failing every call.
function isTemplateLiteral(s) {
  return typeof s === 'string' && s.startsWith('{{') && s.endsWith('}}');
}

const RAW_USER_ID = process.env.LIBRECHAT_USER_ID || '';
const RAW_USERNAME = process.env.LIBRECHAT_USER_USERNAME || process.env.LIBRECHAT_USER_EMAIL || '';
const RAW_ROLE = process.env.LIBRECHAT_USER_ROLE || 'USER';

// Detect inspection mode = NO env vars set at all (not even template literals).
const IS_INSPECTION = !RAW_USER_ID && !RAW_USERNAME;

// Did substitution actually happen, or did we get the literal template?
const SUBSTITUTION_BROKEN = isTemplateLiteral(RAW_USER_ID);

// If substitution failed but we're clearly in per-chat mode (env vars present
// but unsubstituted), fall back to a shared family pseudo-user.
const USER_ID = SUBSTITUTION_BROKEN
  ? 'family-shared'
  : (RAW_USER_ID || '');
const USER_NAME = isTemplateLiteral(RAW_USERNAME)
  ? 'family-shared'
  : (RAW_USERNAME || 'unknown');
const USER_ROLE = (isTemplateLiteral(RAW_ROLE) ? 'USER' : RAW_ROLE).toUpperCase();
const IS_ADMIN = USER_ROLE === 'ADMIN';

function redactUri(uri) {
  if (!uri) return '(empty)';
  return uri.replace(/\/\/([^:]+):[^@]+@/, '//$1:***@');
}

process.stderr.write(
  `[family-cron-mcp] env check:\n` +
  `  MONGODB_URI (redacted): ${redactUri(MONGODB_URI)}\n` +
  `  DB_NAME: ${DB_NAME}\n` +
  `  USER_ID: ${USER_ID || '(unset)'}\n` +
  `  USER_NAME: ${USER_NAME}\n` +
  `  USER_ROLE: ${USER_ROLE}\n`,
);

let mongo;
try {
  mongo = new MongoClient(MONGODB_URI, {
    serverSelectionTimeoutMS: 5000,
    connectTimeoutMS: 5000,
  });
} catch (err) {
  process.stderr.write(`[family-cron-mcp] FATAL: MongoClient ctor: ${err.message}\n`);
  process.exit(2);
}

// In-memory fallback used when Mongo auth fails (a project-wide issue right
// now — Tyler-Computer's Mongo got initialized with a literal template-string
// password). Reminders in memory don't survive redeploys; we surface this
// caveat in tool responses so users know.
const memoryStore = new Map(); // id (string) -> reminder doc
let mongoBroken = false;
let collection = null;
const activeJobs = new Map();  // reminderId -> { stop() } handle

async function getCollection() {
  if (mongoBroken) return null;
  if (collection) return collection;
  try {
    await mongo.connect();
    collection = mongo.db(DB_NAME).collection(COLLECTION_NAME);
    await Promise.all([
      collection.createIndex({ user_id: 1, status: 1, fire_at: 1 }),
      collection.createIndex({ status: 1 }),
      collection.createIndex({ created_at: -1 }),
    ]).catch((err) => process.stderr.write(`[family-cron-mcp] index warning: ${err.message}\n`));
    return collection;
  } catch (err) {
    process.stderr.write(`[family-cron-mcp] Mongo unavailable, using in-memory store: ${err.message}\n`);
    mongoBroken = true;
    return null;
  }
}

// Generate a Mongo-ObjectId-like id without requiring a Mongo connection.
function fakeId() {
  return Math.floor(Date.now() / 1000).toString(16).padStart(8, '0')
    + Math.random().toString(16).slice(2, 18).padStart(16, '0');
}

// Storage abstraction: try Mongo, fall back to memory.
const store = {
  async insertOne(doc) {
    const col = await getCollection();
    if (col) {
      const r = await col.insertOne(doc);
      return r.insertedId;
    }
    const id = fakeId();
    memoryStore.set(id, { _id: id, ...doc });
    return id;
  },
  async updateOne(filter, update) {
    const col = await getCollection();
    if (col) return col.updateOne(filter, update);
    const id = filter._id?.toString?.() || filter._id;
    const doc = memoryStore.get(id);
    if (!doc) return { matchedCount: 0 };
    if (update.$set) Object.assign(doc, update.$set);
    if (update.$inc) {
      for (const [k, v] of Object.entries(update.$inc)) doc[k] = (doc[k] || 0) + v;
    }
    if (update.$push) {
      for (const [k, v] of Object.entries(update.$push)) {
        if (!Array.isArray(doc[k])) doc[k] = [];
        doc[k].push(v);
      }
    }
    return { matchedCount: 1 };
  },
  async findById(idStr) {
    const col = await getCollection();
    if (col) {
      try { return await col.findOne({ _id: new ObjectId(idStr) }); }
      catch { return null; }
    }
    return memoryStore.get(idStr) || null;
  },
  async findUserPending(user_id) {
    const col = await getCollection();
    if (col) {
      return col.find({ user_id, status: 'pending' }).toArray();
    }
    return [...memoryStore.values()].filter(d => d.user_id === user_id && d.status === 'pending');
  },
  async listForUser(user_id, statuses, limit) {
    const col = await getCollection();
    if (col) {
      return col.find({ user_id, status: { $in: statuses } }).sort({ created_at: -1 }).limit(limit).toArray();
    }
    return [...memoryStore.values()]
      .filter(d => d.user_id === user_id && statuses.includes(d.status))
      .sort((a, b) => b.created_at - a.created_at)
      .slice(0, limit);
  },
};

// Parse a "when" string into either { cron: '0 7 * * *' } (recurring) or
// { fire_at: Date } (one-shot). Accepts:
//   - cron expressions: "0 7 * * *", "*/15 * * * *" (5 or 6 fields)
//   - "in N minutes|hours|days"
//   - "tomorrow at HH:MM", "today at HH:MM"
//   - ISO-8601: "2026-05-21T09:00:00Z"
function parseWhen(when) {
  const trimmed = when.trim();

  // Cron expression: 5 or 6 space-separated fields, each from the cron alphabet.
  if (/^[\d*/,\-?LW#]+(\s+[\d*/,\-?LW#A-Za-z]+){4,5}$/.test(trimmed)) {
    if (!cron.validate(trimmed)) throw new Error(`Invalid cron expression: ${trimmed}`);
    return { cron: trimmed, recurring: true };
  }

  // "in N <unit>"
  const inMatch = trimmed.match(/^in\s+(\d+)\s+(second|minute|hour|day|week)s?$/i);
  if (inMatch) {
    const n = parseInt(inMatch[1], 10);
    const unit = inMatch[2].toLowerCase();
    const ms = { second: 1e3, minute: 60e3, hour: 36e5, day: 864e5, week: 7 * 864e5 }[unit];
    return { fire_at: new Date(Date.now() + n * ms), recurring: false };
  }

  // "today at HH:MM" / "tomorrow at HH:MM"
  const dayMatch = trimmed.match(/^(today|tomorrow)\s+at\s+(\d{1,2}):(\d{2})$/i);
  if (dayMatch) {
    const dt = new Date();
    if (dayMatch[1].toLowerCase() === 'tomorrow') dt.setDate(dt.getDate() + 1);
    dt.setHours(parseInt(dayMatch[2], 10), parseInt(dayMatch[3], 10), 0, 0);
    return { fire_at: dt, recurring: false };
  }

  // ISO-8601 datetime
  const iso = new Date(trimmed);
  if (!Number.isNaN(iso.getTime()) && trimmed.length >= 10) {
    return { fire_at: iso, recurring: false };
  }

  throw new Error(
    `Unparseable "when": "${trimmed}". Accepted forms: cron ("0 7 * * *"), ` +
    `"in N minutes|hours|days", "today at HH:MM", "tomorrow at HH:MM", ISO-8601.`,
  );
}

function requireIdentity() {
  if (!USER_ID) {
    return {
      content: [{
        type: 'text',
        text: 'family-cron: missing LIBRECHAT_USER_ID env var. The MCP must be configured with {{LIBRECHAT_USER_ID}} in env so it can scope reminders to the calling user.',
      }],
      isError: true,
    };
  }
  return null;
}

// Mark a reminder as fired. Called from cron callbacks.
async function fireReminder(reminderId, isRecurring) {
  try {
    const now = new Date();
    const idStr = reminderId.toString();
    if (isRecurring) {
      await store.updateOne(
        { _id: reminderId },
        { $set: { last_fired_at: now }, $push: { fire_history: now }, $inc: { fire_count: 1 } },
      );
    } else {
      await store.updateOne(
        { _id: reminderId },
        { $set: { status: 'fired', fired_at: now }, $push: { fire_history: now } },
      );
      const job = activeJobs.get(idStr);
      if (job) { try { job.stop(); } catch {} activeJobs.delete(idStr); }
    }
    process.stderr.write(`[family-cron-mcp] fired reminder ${idStr} (${isRecurring ? 'recurring' : 'one-shot'})\n`);
  } catch (err) {
    process.stderr.write(`[family-cron-mcp] fire error for ${reminderId}: ${err.message}\n`);
  }
}

// Register a Mongo reminder doc with node-cron so it actually fires.
function registerJob(doc) {
  const id = doc._id;
  const idStr = id.toString();
  if (activeJobs.has(idStr)) return; // already registered

  if (doc.cron_expr) {
    const task = cron.schedule(doc.cron_expr, () => fireReminder(id, true), { scheduled: true });
    activeJobs.set(idStr, task);
  } else if (doc.fire_at) {
    const delay = doc.fire_at.getTime() - Date.now();
    if (delay <= 0) {
      // Fire immediately if we boot after the scheduled time and it never fired.
      fireReminder(id, false);
      return;
    }
    const timer = setTimeout(() => fireReminder(id, false), Math.min(delay, 2147483647));
    // Wrap setTimeout in a fake "task" object with stop() so cancel() can clean it up.
    activeJobs.set(idStr, { stop: () => clearTimeout(timer) });
  }
}

const server = new McpServer(
  { name: 'family-cron', version: '0.1.0' },
  { capabilities: { tools: {} } },
);

server.registerTool(
  'schedule_reminder',
  {
    title: 'Schedule a reminder for yourself',
    description:
      'Save a reminder that will fire at a future time and be returned by list_reminders ' +
      'next time you ask. Examples of `when`: "in 2 hours", "tomorrow at 9:00", ' +
      '"2026-05-21T09:00:00Z", "0 7 * * *" (cron: every day 7am). One-shot reminders ' +
      'auto-disappear after acknowledge. Recurring reminders fire on each cron tick — ' +
      'use list_reminders to see what fired since your last check.',
    inputSchema: {
      when: z.string().min(1).describe('When to fire. cron / "in N minutes" / "tomorrow at HH:MM" / ISO date.'),
      message: z.string().min(1).max(2000).describe('What to remind you about, in plain language.'),
    },
  },
  async ({ when, message }) => {
    const guard = requireIdentity();
    if (guard) return guard;
    let parsed;
    try { parsed = parseWhen(when); }
    catch (err) { return { content: [{ type: 'text', text: err.message }], isError: true }; }

    const doc = {
      user_id: USER_ID,
      user_name: USER_NAME,
      message,
      cron_expr: parsed.cron || null,
      fire_at: parsed.fire_at || null,
      recurring: !!parsed.recurring,
      status: 'pending',
      fired_at: null,
      fire_history: [],
      fire_count: 0,
      created_at: new Date(),
    };
    const insertedId = await store.insertOne(doc);
    doc._id = insertedId;
    registerJob(doc);

    const fireDesc = parsed.cron
      ? `cron "${parsed.cron}" (recurring)`
      : `${parsed.fire_at.toISOString()} (one-shot)`;
    const note = mongoBroken
      ? '\n  NOTE: Mongo is currently unavailable (project-wide issue). This reminder is held in-memory; it will fire normally but not survive a redeploy.'
      : '';
    return {
      content: [{
        type: 'text',
        text: `Reminder saved. id=${insertedId}\n  when: ${fireDesc}\n  message: ${message}${note}`,
      }],
    };
  },
);

server.registerTool(
  'list_reminders',
  {
    title: 'List your reminders',
    description:
      'Return all of YOUR pending reminders + any that fired since the last acknowledge. ' +
      'Always call this at the start of a chat to see what you missed. Returns up to 20 entries.',
    inputSchema: {
      include_fired: z.boolean().optional().describe('Include already-fired one-shots (default true).'),
    },
  },
  async ({ include_fired }) => {
    const guard = requireIdentity();
    if (guard) return guard;
    const includeFired = include_fired !== false;
    const statuses = includeFired ? ['pending', 'fired'] : ['pending'];
    const docs = await store.listForUser(USER_ID, statuses, 20);

    if (docs.length === 0) {
      return { content: [{ type: 'text', text: 'No reminders.' }] };
    }
    const lines = docs.map((d) => {
      const label = d.recurring
        ? `recurring (${d.cron_expr})${d.fire_count ? `, fired ${d.fire_count}×, last ${d.last_fired_at?.toISOString()}` : ', never fired'}`
        : d.status === 'fired'
          ? `FIRED at ${d.fired_at?.toISOString()}`
          : `pending → ${d.fire_at?.toISOString()}`;
      return `• [${d._id}] ${label}\n    ${d.message}`;
    });
    return { content: [{ type: 'text', text: `Your reminders (${docs.length}):\n${lines.join('\n')}` }] };
  },
);

server.registerTool(
  'acknowledge_reminder',
  {
    title: 'Acknowledge a fired reminder (hide from list)',
    description: 'Mark a fired one-shot reminder as done so it stops showing up in list_reminders.',
    inputSchema: {
      reminder_id: z.string().min(1).describe('The reminder id (as returned by list_reminders or schedule_reminder).'),
    },
  },
  async ({ reminder_id }) => {
    const guard = requireIdentity();
    if (guard) return guard;
    const doc = await store.findById(reminder_id);
    if (!doc) return { content: [{ type: 'text', text: `No reminder with id ${reminder_id}.` }], isError: true };
    if (!IS_ADMIN && doc.user_id !== USER_ID) {
      return { content: [{ type: 'text', text: `Not your reminder.` }], isError: true };
    }
    const idForUpdate = mongoBroken ? reminder_id : new ObjectId(reminder_id);
    await store.updateOne({ _id: idForUpdate }, { $set: { status: 'acknowledged', acknowledged_at: new Date() } });
    return { content: [{ type: 'text', text: `Acknowledged. id=${reminder_id}` }] };
  },
);

server.registerTool(
  'cancel_reminder',
  {
    title: 'Cancel a pending or recurring reminder',
    description: 'Delete a reminder. Stops any future fires and removes the schedule.',
    inputSchema: {
      reminder_id: z.string().min(1).describe('The reminder id to cancel.'),
    },
  },
  async ({ reminder_id }) => {
    const guard = requireIdentity();
    if (guard) return guard;
    const doc = await store.findById(reminder_id);
    if (!doc) return { content: [{ type: 'text', text: `No reminder with id ${reminder_id}.` }], isError: true };
    if (!IS_ADMIN && doc.user_id !== USER_ID) {
      return { content: [{ type: 'text', text: `Not your reminder.` }], isError: true };
    }
    const job = activeJobs.get(reminder_id);
    if (job) { try { job.stop(); } catch {} activeJobs.delete(reminder_id); }
    const idForUpdate = mongoBroken ? reminder_id : new ObjectId(reminder_id);
    await store.updateOne({ _id: idForUpdate }, { $set: { status: 'cancelled', cancelled_at: new Date() } });
    return { content: [{ type: 'text', text: `Cancelled. id=${reminder_id}` }] };
  },
);

// On boot, scan Mongo for the calling user's pending reminders and register
// them with node-cron / setTimeout. This is CRITICAL — without this, redeploys
// or stdio restarts would lose all in-process schedules.
//
// Note: LibreChat spawns one MCP child per (user, server) pair. So this loader
// only schedules the CURRENT user's reminders. If user B's reminders exist,
// they'll fire when user B opens a chat — not earlier. This is acceptable for
// a family chatbot but worth flagging if usage grows.
async function rehydrateUserJobs() {
  if (IS_INSPECTION || !USER_ID) return;
  try {
    const docs = await store.findUserPending(USER_ID);
    for (const d of docs) registerJob(d);
    process.stderr.write(`[family-cron-mcp] rehydrated ${docs.length} pending reminder(s) for ${USER_NAME}\n`);
  } catch (err) {
    process.stderr.write(`[family-cron-mcp] rehydrate error: ${err.message}\n`);
  }
}

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  if (IS_INSPECTION) {
    process.stderr.write('[family-cron-mcp] inspection mode (no user) — tools advertised, no Mongo touched\n');
  } else {
    await rehydrateUserJobs();
    process.stderr.write(`[family-cron-mcp] ready (user=${USER_NAME}, role=${USER_ROLE})\n`);
  }
}

main().catch((err) => {
  process.stderr.write(`[family-cron-mcp] fatal: ${err && err.stack ? err.stack : err}\n`);
  process.exit(1);
});

process.on('SIGINT', async () => { try { await mongo.close(); } catch {} process.exit(0); });
process.on('SIGTERM', async () => { try { await mongo.close(); } catch {} process.exit(0); });
