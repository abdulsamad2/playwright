// Cookie-side audit with the proxy variable REMOVED.
//
// Every unexpired jar in seed_jars is replayed against facets on the DIRECT host IP
// (known clean). No proxy anywhere in the path, so the only thing under test is the
// token. Cross-referenced against our own status flag, this says exactly how much of
// the "all jars dead" state is real vs. our own bookkeeping killing good tokens.
//
// Read-only. Usage: node scripts/auditJars.mjs [eventId]
import "dotenv/config";
import mongoose from "mongoose";
import { initApiBrowserContext, cleanupApiBrowser } from "../browser-cookies.js";

const eventId = process.argv[2] || "0A006396BA615BBE";
await mongoose.connect(process.env.MONGODB_URI);
const sj = mongoose.connection.db.collection("seed_jars");
const now = new Date();

const docs = await sj
  .find({ expiresAt: { $gt: now } })
  .sort({ mintedAt: -1, createdAt: -1 })
  .toArray();

const url = () =>
  `https://services.ticketmaster.com/api/ismds/event/${eventId}/facets` +
  `?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description` +
  `&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places` +
  `&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us` +
  `&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse` +
  `&_=${Date.now()}&t=${Math.random().toString(36).slice(2)}`;

async function hitDirect(cookies) {
  try {
    const { page } = await initApiBrowserContext(null, cookies); // null = no proxy
    const resp = await page.context().request.get(url(), {
      headers: {
        "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe",
        Accept: "application/json",
        "tmps-correlation-id": `audit-${Date.now()}`,
      },
      timeout: 25000,
    });
    return resp.status();
  } catch (e) {
    return `ERR`;
  } finally {
    await cleanupApiBrowser().catch(() => {});
  }
}

console.log(`auditing ${docs.length} unexpired jars on the DIRECT IP (no proxy)\n`);
console.log("dbStatus  useCount  age    tmpt              direct");
console.log("-".repeat(62));

const tally = { healthyAlive: 0, healthyDead: 0, deadAlive: 0, deadDead: 0 };
for (const d of docs) {
  const age = Math.round((now - (d.mintedAt || d.createdAt)) / 60000);
  const tmpt = (d.cookies.find((c) => c.name === "tmpt") || {}).value;
  const s = await hitDirect(d.cookies);
  const alive = s === 200;
  if (d.status === "healthy") alive ? tally.healthyAlive++ : tally.healthyDead++;
  else alive ? tally.deadAlive++ : tally.deadDead++;
  console.log(
    `${String(d.status).padEnd(9)} ${String(d.useCount ?? 0).padEnd(9)} ${String(age + "m").padEnd(6)} ` +
      `${(tmpt || "none").slice(0, 16).padEnd(17)} ${s}${alive ? "  ALIVE" : "  dead"}`
  );
}

console.log("\n--- VERDICT ---");
console.log(`  flagged healthy AND actually alive : ${tally.healthyAlive}`);
console.log(`  flagged healthy BUT actually dead  : ${tally.healthyDead}   (farm minted a bad token)`);
console.log(`  flagged dead BUT actually alive    : ${tally.deadAlive}   (WE killed a good token)`);
console.log(`  flagged dead AND actually dead     : ${tally.deadDead}`);
const alive = tally.healthyAlive + tally.deadAlive;
console.log(`\n  tokens TM still accepts: ${alive}/${docs.length}`);
if (tally.deadAlive + tally.deadDead > 0) {
  const fp = (tally.deadAlive / (tally.deadAlive + tally.deadDead)) * 100;
  console.log(`  false-retirement rate  : ${fp.toFixed(0)}% of "dead" jars are still good`);
}

await mongoose.disconnect().catch(() => {});
process.exit(0);
