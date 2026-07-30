// Is jar QUALITY the variable, or the egress IP? Test every unexpired jar against the
// same two egresses (one proxy + direct). If jars split cleanly — some 200 on both,
// some 403 on both — then the token decides and the IP is irrelevant.
//
// Usage: node scripts/probeJarQuality.mjs [eventId] [proxy]
import "dotenv/config";
import mongoose from "mongoose";
import proxyArray, { loadProxies } from "../helpers/proxy.js";
import { initApiBrowserContext, cleanupApiBrowser } from "../browser-cookies.js";

const eventId = process.argv[2] || "0A006396BA615BBE";
const proxyArg = process.argv[3] || "209.20.216.100:15260";

await mongoose.connect(process.env.MONGODB_URI);
await loadProxies();
const proxy = (proxyArray.proxies || []).find((p) => p.proxy === proxyArg) || null;

const docs = await mongoose.connection.db
  .collection("seed_jars")
  .find({ expiresAt: { $gt: new Date() } })
  .sort({ mintedAt: -1, createdAt: -1 })
  .limit(8)
  .toArray();

const url = () =>
  `https://services.ticketmaster.com/api/ismds/event/${eventId}/facets` +
  `?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description` +
  `&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places` +
  `&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us` +
  `&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse` +
  `&_=${Date.now()}&t=${Math.random().toString(36).slice(2)}`;

async function hit(cookies, p) {
  try {
    const { page } = await initApiBrowserContext(p, cookies);
    const resp = await page.context().request.get(url(), {
      headers: {
        "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe",
        Accept: "application/json",
        "tmps-correlation-id": `q-${Date.now()}`,
      },
      timeout: 25000,
    });
    return String(resp.status());
  } catch (e) {
    return `ERR:${e.message.slice(0, 30)}`;
  } finally {
    await cleanupApiBrowser().catch(() => {});
  }
}

console.log(`event=${eventId}  proxy=${proxyArg}  jars=${docs.length}\n`);
console.log("status   useCount  age    | via PROXY | via DIRECT");
console.log("-".repeat(58));
for (const d of docs) {
  const age = Math.round((Date.now() - (d.mintedAt || d.createdAt)) / 60000);
  const viaProxy = await hit(d.cookies, proxy);
  const viaDirect = await hit(d.cookies, null);
  console.log(
    `${String(d.status).padEnd(8)} ${String(d.useCount ?? 0).padEnd(9)} ${String(age + "m").padEnd(6)} | ` +
      `${viaProxy.padEnd(9)} | ${viaDirect}`
  );
}

await mongoose.disconnect().catch(() => {});
process.exit(0);
