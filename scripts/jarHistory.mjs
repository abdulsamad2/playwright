// READ-ONLY forensic pass over seed_jars: reconstruct each jar's production life from
// mintedAt -> updatedAt (the moment it was flagged) and the useCount it reached, then
// classify WHY it left rotation. Answers "how many calls did a real jar actually serve,
// and what took it out" from live data rather than from the code's intent.
import "dotenv/config";
import mongoose from "mongoose";

const BUDGET = parseInt(process.env.JAR_CALL_BUDGET, 10) || 400;
await mongoose.connect(process.env.MONGODB_URI);
const docs = await mongoose.connection.db.collection("seed_jars").find({}).sort({ mintedAt: 1 }).toArray();
const now = Date.now();

console.log(`${docs.length} jars on record, JAR_CALL_BUDGET=${BUDGET}\n`);
console.log("mintedAt(UTC)      slot status   useCount  lifeMin  ttlWindowMin  callsPerMin  classified as");
console.log("-".repeat(112));

const buckets = {};
const lives = [];
for (const d of docs) {
  const minted = new Date(d.mintedAt || d.createdAt).getTime();
  const flagged = new Date(d.updatedAt || d.mintedAt).getTime();
  const expires = d.expiresAt ? new Date(d.expiresAt).getTime() : null;
  const lifeMin = (flagged - minted) / 60000;      // mint -> last write (death for dead jars)
  const ttlWin = expires ? (expires - minted) / 60000 : null; // how long we ALLOW it to live
  const use = d.useCount || 0;
  const cpm = lifeMin > 0.2 ? use / lifeMin : null;

  let why;
  if (d.status === "healthy") why = expires && expires < now ? "expired in rotation (never re-flagged)" : "STILL LIVE";
  else if (use >= BUDGET * 0.95) why = `retired: hit call budget (${use})`;
  else if (expires && flagged >= expires - 60000) why = "retired: TTL expiry";
  else if (use === 0) why = "flagged dead having served 0 calls";
  else why = `flagged dead early at ${use} calls (${lifeMin.toFixed(0)}min)`;

  const key = why.replace(/\(.*\)/, "").trim();
  buckets[key] = (buckets[key] || 0) + 1;
  if (d.status !== "healthy" && use > 0) lives.push({ use, lifeMin, cpm });

  console.log(
    `${new Date(minted).toISOString().slice(0, 16)}  ${String(d.slot ?? "-").padEnd(4)} ` +
    `${String(d.status).padEnd(8)} ${String(use).padEnd(9)} ${lifeMin.toFixed(1).padEnd(8)} ` +
    `${(ttlWin != null ? ttlWin.toFixed(1) : "-").padEnd(13)} ${(cpm != null ? cpm.toFixed(1) : "-").padEnd(12)} ${why}`
  );
}

console.log("\n--- why jars left rotation ---");
for (const [k, v] of Object.entries(buckets).sort((a, b) => b[1] - a[1])) console.log(`  ${String(v).padStart(3)}  ${k}`);

if (lives.length) {
  const useArr = lives.map((l) => l.use).sort((a, b) => a - b);
  const lifeArr = lives.map((l) => l.lifeMin).sort((a, b) => a - b);
  const cpmArr = lives.filter((l) => l.cpm != null).map((l) => l.cpm).sort((a, b) => a - b);
  const p = (arr, q) => arr[Math.min(arr.length - 1, Math.floor(arr.length * q))];
  console.log("\n--- retired jars that did real work (n=" + lives.length + ") ---");
  console.log(`  useCount   min ${useArr[0]}  p50 ${p(useArr, 0.5)}  p90 ${p(useArr, 0.9)}  max ${useArr[useArr.length - 1]}`);
  console.log(`  lifeMin    min ${lifeArr[0].toFixed(1)}  p50 ${p(lifeArr, 0.5).toFixed(1)}  p90 ${p(lifeArr, 0.9).toFixed(1)}  max ${lifeArr[lifeArr.length - 1].toFixed(1)}`);
  console.log(`  calls/min  min ${cpmArr[0].toFixed(1)}  p50 ${p(cpmArr, 0.5).toFixed(1)}  p90 ${p(cpmArr, 0.9).toFixed(1)}  max ${cpmArr[cpmArr.length - 1].toFixed(1)}`);
  const totalCalls = docs.reduce((a, d) => a + (d.useCount || 0), 0);
  console.log(`\n  total facets calls ever attributed to jars: ${totalCalls} across ${docs.length} jars ` +
    `(mean ${(totalCalls / docs.length).toFixed(0)}/jar)`);
}

await mongoose.disconnect();
process.exit(0);
