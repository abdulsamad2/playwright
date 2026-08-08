// READ-ONLY inventory of seed_jars: how many, what status, useCount distribution,
// TTL/age, and what the jar doc actually looks like.
import "dotenv/config";
import mongoose from "mongoose";

await mongoose.connect(process.env.MONGODB_URI);
const sj = mongoose.connection.db.collection("seed_jars");
const now = new Date();

const total = await sj.countDocuments({});
console.log(`seed_jars total docs: ${total}\n`);

const byStatus = await sj.aggregate([
  { $group: { _id: "$status", n: { $sum: 1 },
    maxUse: { $max: "$useCount" }, avgUse: { $avg: "$useCount" },
    unexpired: { $sum: { $cond: [{ $gt: ["$expiresAt", now] }, 1, 0] } } } },
  { $sort: { n: -1 } },
]).toArray();
console.log("status        docs   unexpired  maxUseCount  avgUseCount");
for (const s of byStatus) {
  console.log(
    `${String(s._id).padEnd(13)} ${String(s.n).padEnd(6)} ${String(s.unexpired).padEnd(10)} ` +
    `${String(s.maxUse ?? "-").padEnd(12)} ${s.avgUse != null ? s.avgUse.toFixed(1) : "-"}`
  );
}

const sample = await sj.find({}).sort({ mintedAt: -1, createdAt: -1 }).limit(1).toArray();
if (sample[0]) {
  const d = sample[0];
  console.log("\n--- newest doc shape (cookies elided) ---");
  const shape = { ...d, cookies: `[${d.cookies?.length} cookies]` };
  console.log(JSON.stringify(shape, null, 2).slice(0, 1800));
  const tm = (d.cookies || []).find((c) => c.name === "tmpt");
  if (tm) {
    console.log("\ntmpt cookie meta:", JSON.stringify({ ...tm, value: tm.value.slice(0, 24) + "…" }));
    if (tm.expires) console.log("tmpt cookie expires in (min):", ((tm.expires * 1000 - Date.now()) / 60000).toFixed(1));
  }
  console.log("\ncookie names:", (d.cookies || []).map((c) => c.name).join(", "));
}

console.log("\n--- last 40 jars: age / ttl / useCount / status ---");
console.log("slot  status     useCount  ageMin  ttlLeftMin  mintedAt");
const recent = await sj.find({}).sort({ mintedAt: -1, createdAt: -1 }).limit(40).toArray();
for (const d of recent) {
  const minted = d.mintedAt || d.createdAt;
  const age = minted ? ((now - new Date(minted)) / 60000).toFixed(1) : "?";
  const ttl = d.expiresAt ? ((new Date(d.expiresAt) - now) / 60000).toFixed(1) : "?";
  console.log(
    `${String(d.slot ?? "-").padEnd(5)} ${String(d.status).padEnd(10)} ${String(d.useCount ?? 0).padEnd(9)} ` +
    `${String(age).padEnd(7)} ${String(ttl).padEnd(11)} ${minted ? new Date(minted).toISOString() : "?"}`
  );
}

// mint rate: how many jars created per hour over the last 24h
console.log("\n--- mint rate (jars created per hour, last 24h) ---");
const mr = await sj.aggregate([
  { $match: { $expr: { $gt: [{ $ifNull: ["$mintedAt", "$createdAt"] }, new Date(Date.now() - 24 * 3600e3)] } } },
  { $group: { _id: { $dateToString: { format: "%Y-%m-%d %H:00", date: { $ifNull: ["$mintedAt", "$createdAt"] } } }, n: { $sum: 1 } } },
  { $sort: { _id: -1 } },
]).toArray();
for (const h of mr) console.log(`  ${h._id}  ${h.n}`);
if (!mr.length) console.log("  (no jars minted in the last 24h — farm is not running)");

await mongoose.disconnect();
process.exit(0);
