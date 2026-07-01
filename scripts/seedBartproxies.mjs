// Replace the burned static-IP proxy rows in MongoDB with ONE bartproxies
// rotating-residential GATEWAY credential. On load, helpers/proxy.js
// expandDbProxy() fans this single row out into N sticky `_ss-` sessions
// (each pinning its own exit IP) — so production keeps sourcing proxies from
// the DB, but from a live rotating gateway instead of dead pinned IPs.
//
// Usage:
//   node scripts/seedBartproxies.mjs                 # replace clientId=default
//   node scripts/seedBartproxies.mjs --client=foo    # target another clientId
//   node scripts/seedBartproxies.mjs --dry           # show what would change, no writes
//
// Credentials default to the bartproxies account under test (override via env).

import dotenv from "dotenv";
import connectDB, { closeConnections } from "../config/db.js";
import { Proxy } from "../models/proxyModel.js";

dotenv.config();

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);

const clientId = args.client || "default";
const dry = !!args.dry;

const GATEWAY = process.env.PROXY_GATEWAY || "resipro.bartproxies.com:7778";
const [ip, port] = GATEWAY.split(":");
const username = process.env.PROXY_USER || "B_52593_US_1080_32701_30";
const password = process.env.PROXY_PASS || "061t6o";

(async () => {
  await connectDB();

  const existing = await Proxy.countDocuments({ clientId });
  console.log(`[seed] clientId=${clientId}: ${existing} existing row(s) in DB`);
  console.log(`[seed] gateway → ${ip}:${port}  user=${username}  (token appended as _ss-<t> per session)`);

  if (dry) {
    console.log("[seed] --dry: would DELETE all rows above and INSERT 1 bartproxies gateway row. No changes made.");
    await closeConnections();
    process.exit(0);
  }

  const del = await Proxy.deleteMany({ clientId });
  console.log(`[seed] Deleted ${del.deletedCount} burned row(s) for clientId=${clientId}`);

  await Proxy.updateOne(
    { ip, port },
    {
      $set: {
        ip,
        port,
        username,
        password,
        clientId,
        enabled: true,
        notes: "bartproxies rotating-residential gateway; fans out to _ss- sticky sessions",
      },
    },
    { upsert: true }
  );
  console.log(`[seed] Inserted bartproxies gateway row.`);

  const total = await Proxy.countDocuments({ clientId });
  console.log(`[seed] Done. clientId=${clientId} now has ${total} row(s) (expands to IPROYAL_SESSIONS sessions on load).`);

  await closeConnections();
  process.exit(0);
})().catch(async (err) => {
  console.error("[seed] Failed:", err.message);
  try { await closeConnections(); } catch {}
  process.exit(1);
});
