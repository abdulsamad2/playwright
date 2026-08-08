// Does the jar lease actually guarantee one jar -> one exit IP?
//
// The whole fix rests on this: TM revokes a tmpt session permanently once it is used from
// an eleventh exit IP (scripts/jarIpBudget.mjs), so two pages must never hold the same
// jar. This exercises leaseFarmJar/releaseFarmJar against the real collection and asserts
// the properties that matter, without launching a single browser.
//
//   1. exclusivity  — N concurrent leases return N DISTINCT jars, never a duplicate
//   2. exhaustion   — once every jar is leased, the next caller gets null (so the page
//                     declines to bind, rather than sharing a jar and killing it)
//   3. release      — a released jar becomes leasable again
//   4. ownership    — a non-owner cannot release someone else's lease
//   5. no residue   — the collection is left exactly as it was found
//
// Read-only in effect: every lease it takes, it gives back.
//
// Usage: node scripts/testJarLease.mjs
import "dotenv/config";
import mongoose from "mongoose";
import { leaseFarmJar, releaseFarmJar, newOwnerId } from "../browser-cookies.js";

await mongoose.connect(process.env.MONGODB_URI);
const coll = mongoose.connection.db.collection("seed_jars");

const leasedBefore = await coll.countDocuments({ leaseUntil: { $gt: new Date() } });
const available = await coll.countDocuments({
  status: "healthy",
  expiresAt: { $gt: new Date() },
  $or: [{ leaseUntil: { $exists: false } }, { leaseUntil: { $lte: new Date() } }],
});
console.log(`${available} jar(s) free to lease, ${leasedBefore} already leased by the fleet\n`);
if (available < 2) {
  console.error("need at least 2 free jars to test exclusivity — try again when the farm has minted");
  await mongoose.disconnect();
  process.exit(2);
}

let pass = 0, fail = 0;
const check = (name, ok, detail = "") => {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${name}${detail ? ` — ${detail}` : ""}`);
  ok ? pass++ : fail++;
};

// 1. concurrent leases must not collide
const owners = Array.from({ length: available }, () => newOwnerId());
const leases = await Promise.all(owners.map((o, i) => leaseFarmJar(o, `10.0.0.${i}:8000`)));
const got = leases.filter(Boolean);
const ids = got.map((l) => String(l.jarId));
check("every concurrent lease returned a jar", got.length === available, `${got.length}/${available}`);
check("no jar was handed out twice", new Set(ids).size === ids.length, `${new Set(ids).size} distinct of ${ids.length}`);

const tmpts = got.map((l) => l.tmpt).filter(Boolean);
check("each lease carries its own tmpt", new Set(tmpts).size === tmpts.length, `${new Set(tmpts).size} distinct tokens`);

// 2. pool exhausted -> null, never a shared jar
const overflow = await leaseFarmJar(newOwnerId(), "10.0.0.255:8000");
check("lease returns null once every jar is taken", overflow === null,
  overflow ? "handed out a jar that was already leased" : "page will decline to bind, as intended");

// 4. a stranger cannot steal the lease
const victim = got[0];
await releaseFarmJar(victim.jarId, "not-the-owner");
const stillHeld = await coll.findOne({ _id: victim.jarId });
check("a non-owner cannot release a lease", !!stillHeld.leaseOwner && stillHeld.leaseOwner !== "not-the-owner");

// 3. the real owner can, and the jar comes back into circulation
const ownerOfVictim = owners[leases.indexOf(victim)];
await releaseFarmJar(victim.jarId, ownerOfVictim);
const reacquired = await leaseFarmJar(newOwnerId(), "10.0.0.254:8000");
check("a released jar can be leased again", !!reacquired);
if (reacquired) {
  got.push({ ...reacquired, ownerIdUsed: true });
  await releaseFarmJar(reacquired.jarId, reacquired.ownerId ?? null);
}

// 5. hand everything back and confirm we left no residue
for (let i = 0; i < leases.length; i++) {
  if (leases[i]) await releaseFarmJar(leases[i].jarId, owners[i]);
}
// the re-acquired one was taken under a fresh owner we didn't keep — clear it directly
if (reacquired) await coll.updateOne({ _id: reacquired.jarId }, { $unset: { leaseOwner: "", leaseProxy: "", leaseUntil: "", leasedAt: "" } });

const leasedAfter = await coll.countDocuments({ leaseUntil: { $gt: new Date() } });
check("collection left as found", leasedAfter === leasedBefore, `${leasedBefore} before, ${leasedAfter} after`);

console.log(`\n${pass} passed, ${fail} failed`);
await mongoose.disconnect();
process.exit(fail ? 1 : 0);
