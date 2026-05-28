// One-time migration: seed MongoDB Proxy collection from a raw list.
//
// Usage:
//   node scripts/seedProxies.js                     # seeds from helpers/proxy.js.back (or current proxy.js if no backup)
//   node scripts/seedProxies.js --file=path.txt     # seeds from a file with one "IP:PORT:USER:PASS" per line
//   node scripts/seedProxies.js --client=clientA    # tag the inserted rows with a clientId (default: "default")
//   node scripts/seedProxies.js --replace           # delete existing rows for that clientId first
//
// Safe to re-run: uses upsert on (ip, port).

import dotenv from "dotenv";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import connectDB, { closeConnections } from "../config/db.js";
import { Proxy } from "../models/proxyModel.js";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);

const clientId = args.client || "default";
const replace = !!args.replace;

function parseRawList(text) {
  // Accept either JS source (extracts quoted "ip:port:user:pass") or plain-text one-per-line.
  const quoted = [...text.matchAll(/"([\d.]+:\d+:[^":]+:[^":]+)"/g)].map((m) => m[1]);
  if (quoted.length) return quoted;
  return text
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l && !l.startsWith("//") && l.split(":").length === 4);
}

function loadRawProxies() {
  if (args.file) {
    return parseRawList(fs.readFileSync(args.file, "utf8"));
  }
  const back = path.join(__dirname, "..", "helpers", "proxy.js.back");
  const current = path.join(__dirname, "..", "helpers", "proxy.js");
  const source = fs.existsSync(back) ? back : current;
  console.log(`[seed] Reading proxies from ${source}`);
  return parseRawList(fs.readFileSync(source, "utf8"));
}

(async () => {
  await connectDB();

  const raw = loadRawProxies();
  if (!raw.length) {
    console.error("[seed] No proxies parsed from source. Aborting.");
    process.exit(1);
  }
  console.log(`[seed] Parsed ${raw.length} proxies. clientId=${clientId} replace=${replace}`);

  if (replace) {
    const del = await Proxy.deleteMany({ clientId });
    console.log(`[seed] Deleted ${del.deletedCount} existing rows for clientId=${clientId}`);
  }

  let upserted = 0;
  let skipped = 0;
  const ops = raw.map((line) => {
    const [ip, port, username, password] = line.split(":");
    if (!ip || !port || !username || !password) {
      skipped++;
      return null;
    }
    return {
      updateOne: {
        filter: { ip, port },
        update: {
          $set: { ip, port, username, password, clientId, enabled: true },
        },
        upsert: true,
      },
    };
  }).filter(Boolean);

  if (ops.length) {
    const res = await Proxy.bulkWrite(ops, { ordered: false });
    upserted = (res.upsertedCount || 0) + (res.modifiedCount || 0);
  }

  console.log(`[seed] Done. Upserted/modified: ${upserted}, skipped malformed: ${skipped}`);
  const total = await Proxy.countDocuments({ clientId });
  console.log(`[seed] Total proxies in DB for clientId=${clientId}: ${total}`);

  await closeConnections();
  process.exit(0);
})().catch(async (err) => {
  console.error("[seed] Failed:", err);
  try { await closeConnections(); } catch {}
  process.exit(1);
});
