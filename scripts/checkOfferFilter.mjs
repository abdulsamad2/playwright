// One-off: pull live TM facets for an event and report exactly what the
// HOLD / PACKAGE offer filters would drop, and what section they sit in.
//
// Usage: node scripts/checkOfferFilter.mjs <eventId> [sectionFilter]
//   node scripts/checkOfferFilter.mjs 0A0064C8CED8B734 BLCHL
import "dotenv/config";
import * as fs from "fs";
import mongoose from "mongoose";
import {
  browserApiRequest,
  initApiBrowserContext,
  cleanupApiBrowser,
} from "../browser-cookies.js";

const eventId = process.argv[2];
const sectionFilter = process.argv[3] || null;
if (!eventId) {
  console.error("usage: node scripts/checkOfferFilter.mjs <eventId> [section]");
  process.exit(1);
}

// Reuse a jar the cookie farm already minted rather than minting our own — the
// seed_jars collection holds live tmpt tokens, so no event-page nav is needed.
await mongoose.connect(process.env.MONGODB_URI);
const jarDocs = await mongoose.connection.db
  .collection("seed_jars")
  .find({ status: "healthy", expiresAt: { $gt: new Date() } })
  .sort({ slot: 1 })
  .toArray();
const jars = jarDocs
  .map((d) => d.cookies)
  .filter((c) => Array.isArray(c) && c.length);
console.log(`[jars] ${jars.length} healthy jar(s) in seed_jars`);
if (!jars.length) {
  const total = await mongoose.connection.db
    .collection("seed_jars")
    .countDocuments();
  console.error(`no usable jar (seed_jars has ${total} doc(s) total)`);
  await mongoose.disconnect();
  process.exit(3);
}

const facetUrl =
  `https://services.ticketmaster.com/api/ismds/event/${eventId}/facets` +
  `?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description` +
  `&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places` +
  `&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us` +
  `&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse` +
  // Cache buster, exactly as scraper.js does it. Without a unique URL TM serves
  // a cached 400 Error.NotFound even for a perfectly valid event.
  `&_=${Date.now()}&t=${Math.random().toString(36).slice(2)}`;

// The two rules exactly as they appear in helpers/seatBatch.js.
const isHold = (o) =>
  typeof o?.ticketTypeUnsoldQualifier === "string" &&
  /HOLD$/i.test(o.ticketTypeUnsoldQualifier);
const isPackage = (o) =>
  typeof o?.description === "string" && /package/i.test(o.description);

// Walk the jars: a single 403 usually means that token is burned, not that the
// event is unreachable, so try the next one before giving up.
const t0 = Date.now();
let data = null;
for (let i = 0; i < jars.length; i++) {
  const jar = jars[i];
  const tmpt = (jar.find((c) => c.name === "tmpt") || {}).value;
  try {
    data = await browserApiRequest(
      facetUrl,
      {
        "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe",
        Accept: "application/json",
        // Required: without tmps-correlation-id facets returns 400 Error.NotFound
        // even for a valid event with a valid session.
        "tmps-correlation-id": `chk-${Date.now()}-${i}`,
      },
      null,
      jar,
    );
    console.log(`[jars] jar ${i + 1}/${jars.length} OK (tmpt=${tmpt?.slice(0, 12)}…)`);
    break;
  } catch (err) {
    console.warn(
      `[jars] jar ${i + 1}/${jars.length} failed: HTTP ${err.statusCode ?? "?"} ${err.message}`,
    );
    // browserApiRequest discards the response body; re-issue once through the
    // raw context request so we can actually read why TM rejected it.
    if (i === 0) {
      try {
        const { page } = await initApiBrowserContext(null, jar);
        const resp = await page.context().request.get(facetUrl, {
          headers: {
        "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe",
        Accept: "application/json",
        // Required: without tmps-correlation-id facets returns 400 Error.NotFound
        // even for a valid event with a valid session.
        "tmps-correlation-id": `chk-${Date.now()}-${i}`,
      },
          timeout: 20000,
        });
        console.warn(`[debug] status=${resp.status()} body=${(await resp.text()).slice(0, 600)}`);
      } catch (e) {
        console.warn(`[debug] raw probe failed: ${e.message}`);
      }
    }
  }
}
if (!data) {
  console.error(`FETCH FAILED after ${jars.length} jar(s) in ${Math.round((Date.now() - t0) / 1000)}s`);
  await cleanupApiBrowser().catch(() => {});
  await mongoose.disconnect().catch(() => {});
  process.exit(2);
}

const offers = data?._embedded?.offer || [];
const facets = data?.facets || [];
fs.mkdirSync("./debug", { recursive: true });
fs.writeFileSync(`./debug/facets_${eventId}.json`, JSON.stringify(data, null, 2));

console.log(`\nevent ${eventId} — ${offers.length} offers, ${facets.length} facets`);
console.log(`raw dump: debug/facets_${eventId}.json\n`);

// Map offerId -> the sections it actually appears in, via facets.
const sectionsByOffer = new Map();
for (const f of facets) {
  const sec = f?.section;
  if (!sec) continue;
  for (const oid of f?.offers || []) {
    if (!sectionsByOffer.has(oid)) sectionsByOffer.set(oid, new Set());
    sectionsByOffer.get(oid).add(sec);
  }
}

const row = (o) => ({
  offerId: o.offerId,
  name: o.name ?? null,
  description: o.description ?? null,
  qualifier: o.ticketTypeUnsoldQualifier ?? null,
  inventoryType: o.inventoryType ?? null,
  sections: [...(sectionsByOffer.get(o.offerId) || [])].sort(),
  hold: isHold(o),
  package: isPackage(o),
});

const rows = offers.map(row);
const dropped = rows.filter((r) => r.hold || r.package);
const kept = rows.filter((r) => !r.hold && !r.package);

console.log(`WOULD DROP: ${dropped.length}   WOULD KEEP: ${kept.length}`);

const show = (label, list) => {
  if (!list.length) return;
  console.log(`\n--- ${label} (${list.length}) ---`);
  for (const r of list) {
    const why = [r.hold && "HOLD", r.package && "PACKAGE"].filter(Boolean).join("+");
    console.log(
      `  [${why || "kept"}] ${r.offerId}\n` +
        `      name=${JSON.stringify(r.name)}\n` +
        `      desc=${JSON.stringify(r.description)}\n` +
        `      qualifier=${JSON.stringify(r.qualifier)}  invType=${r.inventoryType}\n` +
        `      sections=${r.sections.length ? r.sections.join(",") : "(none)"}`,
    );
  }
};

show("DROPPED", dropped);

// Anything whose NAME says package but which survives = the name-vs-description gap.
const nameGap = kept.filter((r) => /package/i.test(r.name || ""));
show("KEPT but name contains PACKAGE (name-vs-description gap)", nameGap);

// Any qualifier containing HOLD but not ending in it.
const anchorGap = kept.filter((r) => /hold/i.test(r.qualifier || ""));
show("KEPT but qualifier contains HOLD (not end-anchored)", anchorGap);

if (sectionFilter) {
  const re = new RegExp(sectionFilter, "i");
  const inSection = rows.filter((r) => r.sections.some((s) => re.test(s)));
  console.log(`\n=== section ~ /${sectionFilter}/i : ${inSection.length} offers ===`);
  const secDropped = inSection.filter((r) => r.hold || r.package);
  console.log(
    `  dropped by current filters: ${secDropped.length} / ${inSection.length}`,
  );
  show(`section ${sectionFilter} — ALL offers`, inSection);
  const secNames = [...new Set(rows.flatMap((r) => r.sections))]
    .filter((s) => re.test(s))
    .sort();
  console.log(`  matching section names seen: ${secNames.join(", ") || "(none)"}`);
}

// Qualifier / description vocabulary, to spot codes the rules miss.
const quals = {};
for (const r of rows) if (r.qualifier) quals[r.qualifier] = (quals[r.qualifier] || 0) + 1;
console.log(`\n=== ticketTypeUnsoldQualifier vocabulary ===`);
console.log(Object.keys(quals).length ? quals : "(none present on any offer)");

await cleanupApiBrowser().catch(() => {});
await mongoose.disconnect().catch(() => {});
