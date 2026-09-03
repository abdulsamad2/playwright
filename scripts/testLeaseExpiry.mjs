// Does a crashed owner actually give its jar back?
//
// The lease used to run to the jar's own expiresAt, so a PM2 kill stranded the instance's
// whole allocation for up to 60 minutes and the restarted pid could not reclaim it (the
// owner id embeds the pid). The lease is now bounded (JAR_LEASE_TTL_MS) and pushed
// forward by a live pool's heartbeat. This asserts the properties that changed:
//
//   1. bounded    — a lease taken with a TTL is not held to the jar's expiry
//   2. renew      — renewFarmLeases extends a held lease, so a LIVE pool keeps its jar
//   3. ownership  — renew never touches a jar this owner no longer holds
//   4. blocking   — a lease that is still live blocks another page, as before
//   5. expiry     — once the TTL passes, a crashed owner's jar is leasable again
//
// Runs against its own throwaway jar, so it needs no free jar and disturbs none. The
// fixture is minted one minute in the FUTURE so it always sorts ahead of anything the
// farm mints mid-run (leaseFarmJar takes the freshest jar first).
//
// Usage: node scripts/testLeaseExpiry.mjs
import "dotenv/config";
import mongoose from "mongoose";
import { leaseFarmJar, renewFarmLeases, releaseFarmJar, newOwnerId } from "../browser-cookies.js";

await mongoose.connect(process.env.PROBE_URI || process.env.MONGODB_URI);
const coll = mongoose.connection.db.collection("seed_jars");

const TEST_ID = "__leasetest__::slot-0";
let failures = 0;
const check = (name, ok, detail = "") => {
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? " — " + detail : ""}`);
  if (!ok) failures++;
};
const isFree = async () => !!(await coll.findOne({
  _id: TEST_ID, status: "healthy", expiresAt: { $gt: new Date() },
  $or: [{ leaseUntil: { $exists: false } }, { leaseUntil: { $lte: new Date() } }],
}));

await coll.deleteOne({ _id: TEST_ID });
await coll.insertOne({
  _id: TEST_ID,
  machineId: "__leasetest__",
  slot: 0,
  status: "healthy",
  cookies: [{ name: "tmpt", value: "test-token", domain: ".ticketmaster.com", path: "/" }],
  mintedAt: new Date(Date.now() + 60 * 1000),
  expiresAt: new Date(Date.now() + 60 * 60 * 1000), // an hour of life
});

const borrowed = []; // any real jar this test leases by accident, to hand straight back

try {
  const ownerA = newOwnerId();
  const lease = await leaseFarmJar(ownerA, "proxy-a", 2000);
  const got = lease && String(lease.jarId) === TEST_ID;
  check("leases the test jar", got, got ? "" : `leased ${lease && lease.jarId}`);
  if (!got) throw new Error("could not isolate the test jar");

  let doc = await coll.findOne({ _id: TEST_ID });
  const leaseMs = new Date(doc.leaseUntil) - Date.now();
  check("lease is bounded by ttlMs, not the jar's expiry", leaseMs <= 2500,
    `${leaseMs}ms held, jar has 60min of life`);

  const r1 = await renewFarmLeases([{ jarId: TEST_ID, jarOwner: ownerA }]);
  doc = await coll.findOne({ _id: TEST_ID });
  const renewedMs = new Date(doc.leaseUntil) - Date.now();
  check("renew extends a held lease", r1.renewed === 1 && r1.lost === 0 && renewedMs > 60000,
    `renewed=${r1.renewed} lost=${r1.lost}, now ${Math.round(renewedMs / 1000)}s`);

  const r2 = await renewFarmLeases([{ jarId: TEST_ID, jarOwner: newOwnerId() }]);
  check("renew ignores a jar this owner no longer holds", r2.renewed === 0 && r2.lost === 1,
    `renewed=${r2.renewed} lost=${r2.lost}`);

  // Simulate the crash: stop renewing, with a lease that has a moment left to run.
  await coll.updateOne({ _id: TEST_ID }, { $set: { leaseUntil: new Date(Date.now() + 1500) } });
  check("a live lease still blocks another page", !(await isFree()));

  await new Promise((r) => setTimeout(r, 2000));
  check("the TTL frees the jar without the owner releasing it", await isFree());

  const ownerB = newOwnerId();
  const reclaimed = await leaseFarmJar(ownerB, "proxy-b", 2000);
  const back = reclaimed && String(reclaimed.jarId) === TEST_ID;
  if (reclaimed && !back) borrowed.push([reclaimed.jarId, ownerB]);
  check("a crashed owner's jar is leasable again by a new owner", back,
    back ? "" : `leased ${reclaimed && reclaimed.jarId} instead`);
} finally {
  for (const [jarId, owner] of borrowed) await releaseFarmJar(jarId, owner);
  await coll.deleteOne({ _id: TEST_ID });
  check("no residue left in the collection",
    (await coll.countDocuments({ _id: TEST_ID })) === 0 &&
    (await coll.countDocuments({ machineId: "__leasetest__" })) === 0);
  await mongoose.disconnect();
}

console.log(failures ? `\n${failures} check(s) FAILED` : "\nall checks passed");
process.exit(failures ? 1 : 0);
