#!/usr/bin/env bun
/**
 * claude-peers broker daemon
 *
 * A singleton HTTP server on localhost:7899 backed by SQLite.
 * Tracks all registered Claude Code peers and routes messages between them.
 *
 * Auto-launched by the MCP server if not already running.
 * Run directly: bun broker.ts
 */

import { Database } from "bun:sqlite";
import type {
  RegisterRequest,
  RegisterResponse,
  HeartbeatRequest,
  SetSummaryRequest,
  ListPeersRequest,
  SendMessageRequest,
  PollMessagesRequest,
  PollMessagesResponse,
  Peer,
  Message,
} from "./shared/types.ts";

const PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const DB_PATH = process.env.CLAUDE_PEERS_DB ?? `${process.env.HOME}/.claude-peers.db`;

// --- Database setup ---

const db = new Database(DB_PATH);
db.run("PRAGMA journal_mode = WAL");
db.run("PRAGMA busy_timeout = 3000");

db.run(`
  CREATE TABLE IF NOT EXISTS peers (
    id TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    parent_pid INTEGER,
    cwd TEXT NOT NULL,
    git_root TEXT,
    tty TEXT,
    summary TEXT NOT NULL DEFAULT '',
    registered_at TEXT NOT NULL,
    last_seen TEXT NOT NULL
  )
`);

// Migration: add parent_pid column if missing (existing DBs)
try {
  db.run("ALTER TABLE peers ADD COLUMN parent_pid INTEGER");
} catch {
  // Column already exists — ignore
}

// Migration: add is_print column if missing (existing DBs)
try {
  db.run("ALTER TABLE peers ADD COLUMN is_print INTEGER NOT NULL DEFAULT 0");
} catch {
  // Column already exists — ignore
}

db.run(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    text TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (from_id) REFERENCES peers(id),
    FOREIGN KEY (to_id) REFERENCES peers(id)
  )
`);

// Heartbeat staleness: peers that haven't heartbeated in this window are considered dead
const HEARTBEAT_STALE_MS = 120_000; // 2 minutes (heartbeat interval is 15s, so 8 missed = dead)

/**
 * Check if a peer is still alive using PID, parent PID, and heartbeat.
 */
function isPeerAlive(peer: { pid: number; parent_pid: number | null; last_seen: string }): boolean {
  // Check 1: Is the peer's own process dead?
  try {
    process.kill(peer.pid, 0);
  } catch {
    return false;
  }

  // Check 2: Is the parent process (Claude Code) dead?
  if (peer.parent_pid && peer.parent_pid > 1) {
    try {
      process.kill(peer.parent_pid, 0);
    } catch {
      return false;
    }
  }

  // Check 3: Has the peer missed too many heartbeats?
  const lastSeen = new Date(peer.last_seen).getTime();
  if (Date.now() - lastSeen > HEARTBEAT_STALE_MS) {
    return false;
  }

  return true;
}

// Clean up stale peers on startup and periodically
function cleanStalePeers() {
  const peers = db.query("SELECT id, pid, parent_pid, last_seen FROM peers").all() as {
    id: string;
    pid: number;
    parent_pid: number | null;
    last_seen: string;
  }[];

  for (const peer of peers) {
    if (!isPeerAlive(peer)) {
      db.run("DELETE FROM peers WHERE id = ?", [peer.id]);
      db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [peer.id]);
    }
  }
}

cleanStalePeers();

// Periodically clean stale peers (every 30s)
setInterval(cleanStalePeers, 30_000);

// --- Prepared statements ---

const insertPeer = db.prepare(`
  INSERT INTO peers (id, pid, parent_pid, cwd, git_root, tty, summary, registered_at, last_seen, is_print)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const updateLastSeen = db.prepare(`
  UPDATE peers SET last_seen = ? WHERE id = ?
`);

const updateSummary = db.prepare(`
  UPDATE peers SET summary = ? WHERE id = ?
`);

const deletePeer = db.prepare(`
  DELETE FROM peers WHERE id = ?
`);

const selectAllPeers = db.prepare(`
  SELECT * FROM peers
`);

const selectPeersByDirectory = db.prepare(`
  SELECT * FROM peers WHERE cwd = ?
`);

const selectPeersByGitRoot = db.prepare(`
  SELECT * FROM peers WHERE git_root = ?
`);

const insertMessage = db.prepare(`
  INSERT INTO messages (from_id, to_id, text, sent_at, delivered)
  VALUES (?, ?, ?, ?, 0)
`);

const selectUndelivered = db.prepare(`
  SELECT * FROM messages WHERE to_id = ? AND delivered = 0 ORDER BY sent_at ASC
`);

const markDelivered = db.prepare(`
  UPDATE messages SET delivered = 1 WHERE id = ?
`);

// --- Generate peer ID ---

function generateId(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let id = "";
  for (let i = 0; i < 8; i++) {
    id += chars[Math.floor(Math.random() * chars.length)];
  }
  return id;
}

// --- Request handlers ---

function handleRegister(body: RegisterRequest): RegisterResponse {
  const id = generateId();
  const now = new Date().toISOString();
  const isPrint = body.is_print ? 1 : 0;

  // Remove any existing registration for this PID (re-registration)
  const existing = db.query("SELECT id FROM peers WHERE pid = ?").get(body.pid) as { id: string } | null;
  if (existing) {
    deletePeer.run(existing.id);
  }

  insertPeer.run(id, body.pid, body.parent_pid ?? null, body.cwd, body.git_root, body.tty, body.summary, now, now, isPrint);
  return { id };
}

function handleHeartbeat(body: HeartbeatRequest): void {
  updateLastSeen.run(new Date().toISOString(), body.id);
}

function handleSetSummary(body: SetSummaryRequest): void {
  updateSummary.run(body.summary, body.id);
}

function handleListPeers(body: ListPeersRequest): Peer[] {
  let peers: Peer[];

  switch (body.scope) {
    case "machine":
      peers = selectAllPeers.all() as Peer[];
      break;
    case "directory":
      peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      break;
    case "repo":
      if (body.git_root) {
        peers = selectPeersByGitRoot.all(body.git_root) as Peer[];
      } else {
        // No git root, fall back to directory
        peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      }
      break;
    default:
      peers = selectAllPeers.all() as Peer[];
  }

  // Exclude the requesting peer
  if (body.exclude_id) {
    peers = peers.filter((p) => p.id !== body.exclude_id);
  }

  // Filter out --print sessions (non-interactive, can't receive messages)
  peers = peers.filter((p) => !p.is_print);

  // Verify each peer is still alive using full liveness check
  return peers.filter((p) => {
    const alive = isPeerAlive(p);
    if (!alive) {
      deletePeer.run(p.id);
      db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [p.id]);
    }
    return alive;
  });
}

function handleSendMessage(body: SendMessageRequest): { ok: boolean; error?: string } {
  // Verify target exists AND is still alive
  const target = db.query("SELECT id, pid, parent_pid, last_seen, is_print FROM peers WHERE id = ?").get(body.to_id) as {
    id: string;
    pid: number;
    parent_pid: number | null;
    last_seen: string;
    is_print: number;
  } | null;

  if (!target) {
    return { ok: false, error: `Peer ${body.to_id} not found` };
  }

  if (target.is_print) {
    return { ok: false, error: `Peer ${body.to_id} is a non-interactive session` };
  }

  if (!isPeerAlive(target)) {
    deletePeer.run(target.id);
    db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [target.id]);
    return { ok: false, error: `Peer ${body.to_id} is no longer alive` };
  }

  insertMessage.run(body.from_id, body.to_id, body.text, new Date().toISOString());
  return { ok: true };
}

function handlePollMessages(body: PollMessagesRequest): PollMessagesResponse {
  const messages = selectUndelivered.all(body.id) as Message[];

  // Mark them as delivered
  for (const msg of messages) {
    markDelivered.run(msg.id);
  }

  return { messages };
}

function handleUnregister(body: { id: string }): void {
  deletePeer.run(body.id);
}

// --- HTTP Server ---

Bun.serve({
  port: PORT,
  hostname: "127.0.0.1",
  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname;

    if (req.method !== "POST") {
      if (path === "/health") {
        return Response.json({ status: "ok", peers: (selectAllPeers.all() as Peer[]).length });
      }
      return new Response("claude-peers broker", { status: 200 });
    }

    try {
      const body = await req.json();

      switch (path) {
        case "/register":
          return Response.json(handleRegister(body as RegisterRequest));
        case "/heartbeat":
          handleHeartbeat(body as HeartbeatRequest);
          return Response.json({ ok: true });
        case "/set-summary":
          handleSetSummary(body as SetSummaryRequest);
          return Response.json({ ok: true });
        case "/list-peers":
          return Response.json(handleListPeers(body as ListPeersRequest));
        case "/send-message":
          return Response.json(handleSendMessage(body as SendMessageRequest));
        case "/poll-messages":
          return Response.json(handlePollMessages(body as PollMessagesRequest));
        case "/unregister":
          handleUnregister(body as { id: string });
          return Response.json({ ok: true });
        default:
          return Response.json({ error: "not found" }, { status: 404 });
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return Response.json({ error: msg }, { status: 500 });
    }
  },
});

console.error(`[claude-peers broker] listening on 127.0.0.1:${PORT} (db: ${DB_PATH})`);
