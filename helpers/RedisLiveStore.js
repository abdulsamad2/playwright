/**
 * RedisLiveStore — Centralized live data store for 200+ PM2 instances
 *
 * Architecture: Write-back
 *   Reads  → Redis FIRST (sub-ms), MongoDB fallback
 *   Writes → Redis FIRST (instant), then batched to MongoDB every 5 s
 *
 * Distributed event distribution:
 *   Sorted set `evt:staleness` (score = last-updated timestamp)
 *   claimEvents(N) → atomically picks N stalest events via ZRANGEBYSCORE + SET NX
 *   releaseEvent()  → Lua script: DEL lock + ZADD new score atomically
 *
 * SLA: every active event must be scraped within 2 minutes.
 */

import os from "os";
import { getRedisClient, isRedisReady } from "../config/redis.js";
import { Event, ConsecutiveGroup } from "../models/index.js";

// ── Instance identity ───────────────────────────────────────────────────────
export const INSTANCE_ID = `${os.hostname()}-${process.pid}`;

// ── Redis key prefixes ──────────────────────────────────────────────────────
const KEY = {
  event: (id) => `evt:${id}`,
  seats: (id) => `seats:${id}`,
  active: "evt:active",
  all: "evt:all",
  map: (mid) => `evt:map:${mid}`,
  lock: (id) => `lock:processing:${id}`,
  processed: (id) => `evt:processed:${id}`,
  instance: (iid) => `inst:${iid}`,
  hydrated: "system:hydrated",
  hydrationLock: "lock:hydration",
  staleness: "evt:staleness",
  syncLock: "lock:sync:events",
};

// ── Write batcher config ────────────────────────────────────────────────────
const FLUSH_INTERVAL_MS = 5000;
const MAX_BUFFER_SIZE = 50;

class RedisLiveStore {
  constructor() {
    this.writeBuffer = new Map();
    this.flushTimer = null;
    this.isFlushing = false;
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  HELPERS
  // ══════════════════════════════════════════════════════════════════════════

  /** @returns {import('ioredis').Redis} */
  get redis() {
    return getRedisClient();
  }

  get ready() {
    return isRedisReady();
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  WRITE BATCHER — Redis-first, deferred MongoDB sync
  // ══════════════════════════════════════════════════════════════════════════

  queueWrite(eventId, update) {
    this.writeBuffer.set(eventId, {
      filter: { Event_ID: eventId },
      update: { $set: update },
    });
    if (this.writeBuffer.size >= MAX_BUFFER_SIZE) {
      this.flushWrites().catch((e) =>
        console.error("[RedisLiveStore] flush error:", e.message)
      );
    }
  }

  async flushWrites() {
    if (this.isFlushing || this.writeBuffer.size === 0) return;
    this.isFlushing = true;
    const snapshot = new Map(this.writeBuffer);
    this.writeBuffer.clear();
    try {
      const ops = [];
      for (const [, { filter, update }] of snapshot) {
        ops.push({ updateOne: { filter, update, upsert: false } });
      }
      if (ops.length) {
        await Event.bulkWrite(ops, { ordered: false });
      }
    } catch (err) {
      console.error(
        `[RedisLiveStore] bulkWrite failed (${snapshot.size} ops): ${err.message}`
      );
      for (const [id, op] of snapshot) {
        if (!this.writeBuffer.has(id)) this.writeBuffer.set(id, op);
      }
    } finally {
      this.isFlushing = false;
    }
  }

  startWriteBatcher() {
    if (this.flushTimer) return;
    this.flushTimer = setInterval(
      () => this.flushWrites().catch(() => {}),
      FLUSH_INTERVAL_MS
    );
    console.log(
      `[RedisLiveStore] Write batcher started (${FLUSH_INTERVAL_MS}ms / max ${MAX_BUFFER_SIZE})`
    );
  }

  async stopWriteBatcher() {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    await this.flushWrites();
    console.log("[RedisLiveStore] Write batcher stopped — final flush done");
  }

  getBufferStats() {
    return {
      pending: this.writeBuffer.size,
      maxSize: MAX_BUFFER_SIZE,
      flushIntervalMs: FLUSH_INTERVAL_MS,
    };
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  HYDRATION — populate Redis from MongoDB on startup
  // ══════════════════════════════════════════════════════════════════════════

  async hydrate() {
    if (!this.ready) {
      console.warn("[RedisLiveStore] Redis not ready — skipping hydration");
      return;
    }

    // Check if already hydrated (another instance finished before us).
    // GUARD: the `hydrated` flag can outlive the actual data — e.g. Redis was
    // flushed/evicted, or a sync emptied evt:active — leaving the flag set but the
    // active set / staleness queue empty. If we blindly trust the flag we skip
    // hydration and the queue stays empty forever (claimEvents finds nothing →
    // scraper freezes). So when the flag is set, verify the data is really there;
    // if it's missing, clear the flag and re-hydrate.
    const alreadyDone = await this.redis.get(KEY.hydrated);
    if (alreadyDone) {
      const activeCount = await this.redis.scard(KEY.active);
      const queueCount = await this.redis.zcard(KEY.staleness);
      if (activeCount > 0 && queueCount > 0) {
        console.log(`[RedisLiveStore] Already hydrated — skipping (${activeCount} active, ${queueCount} queued)`);
        return;
      }
      console.warn(`[RedisLiveStore] 'hydrated' flag set but data missing (active=${activeCount}, queue=${queueCount}) — clearing flag and re-hydrating`);
      await this.redis.del(KEY.hydrated);
      // fall through to hydrate from MongoDB
    }

    // Try to acquire hydration lock (30s TTL — short, re-acquirable)
    const acquired = await this.redis.set(
      KEY.hydrationLock,
      INSTANCE_ID,
      "EX",
      30,
      "NX"
    );
    if (!acquired) {
      console.log("[RedisLiveStore] Another instance is hydrating — waiting…");
      // Wait up to 45s for the other instance to finish
      for (let i = 0; i < 15; i++) {
        await new Promise((r) => setTimeout(r, 3000));
        const done = await this.redis.get(KEY.hydrated);
        if (done) {
          console.log("[RedisLiveStore] Hydration completed by another instance");
          return;
        }
        // If the lock expired (other instance crashed), try to take over
        const lockHolder = await this.redis.get(KEY.hydrationLock);
        if (!lockHolder) {
          console.log("[RedisLiveStore] Lock expired — retrying hydration ourselves");
          return this.hydrate(); // Recursive retry
        }
      }
      console.warn("[RedisLiveStore] Hydration wait timed out — proceeding anyway");
      // Force-clear the stale lock and hydrate ourselves
      await this.redis.del(KEY.hydrationLock);
      return this.hydrate();
    }

    try {
      console.log("[RedisLiveStore] Hydrating from MongoDB…");
      const events = await Event.find().lean();

      if (!events.length) {
        console.log("[RedisLiveStore] No events in MongoDB");
        await this.redis.set(KEY.hydrated, "1");
        return;
      }

      const pipeline = this.redis.pipeline();
      for (const event of events) {
        const id = event.Event_ID;
        pipeline.set(KEY.event(id), JSON.stringify(event));
        pipeline.sadd(KEY.all, id);
        if (!event.Skip_Scraping) pipeline.sadd(KEY.active, id);
        if (event.mapping_id) pipeline.set(KEY.map(event.mapping_id), id);
      }
      await pipeline.exec();

      // Seat groups are loaded LAZILY on first getSeatGroups() call.
      // With 300K+ seat groups, eager loading would take minutes and
      // block startup. Each event's seats are cached on first access.

      const activeIds = events.filter((e) => !e.Skip_Scraping).map((e) => e.Event_ID);

      // Build staleness index
      await this.buildStalenessIndex();

      await this.redis.set(KEY.hydrated, "1");
      console.log(
        `[RedisLiveStore] Hydrated ${events.length} events (${activeIds.length} active)`
      );
    } catch (err) {
      console.error(`[RedisLiveStore] Hydration error: ${err.message}`);
    } finally {
      await this.redis.del(KEY.hydrationLock);
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  READS — Redis first, MongoDB fallback
  // ══════════════════════════════════════════════════════════════════════════

  async getActiveEvents(fields) {
    if (!this.ready) {
      const events = await Event.find({ Skip_Scraping: { $ne: true } }).lean();
      return fields ? events.map((e) => pick(e, fields)) : events;
    }
    const ids = await this.redis.smembers(KEY.active);
    if (!ids.length) return [];
    const pipe = this.redis.pipeline();
    for (const id of ids) pipe.get(KEY.event(id));
    const results = await pipe.exec();
    const out = [];
    for (const [err, val] of results) {
      if (!err && val) {
        try {
          out.push(JSON.parse(val));
        } catch {}
      }
    }
    return fields ? out.map((e) => pick(e, fields)) : out;
  }

  async getAllEvents() {
    if (!this.ready) return Event.find().lean();
    const ids = await this.redis.smembers(KEY.all);
    if (!ids.length) return [];
    const pipe = this.redis.pipeline();
    for (const id of ids) pipe.get(KEY.event(id));
    const results = await pipe.exec();
    const out = [];
    for (const [err, val] of results) {
      if (!err && val) {
        try {
          out.push(JSON.parse(val));
        } catch {}
      }
    }
    return out;
  }

  async getEventById(eventId) {
    if (!this.ready) return Event.findOne({ Event_ID: eventId }).lean();
    const raw = await this.redis.get(KEY.event(eventId));
    if (raw) {
      try {
        return JSON.parse(raw);
      } catch {}
    }
    // Fallback
    const event = await Event.findOne({ Event_ID: eventId }).lean();
    if (event) await this.redis.set(KEY.event(eventId), JSON.stringify(event));
    return event;
  }

  async findEventByIdOrMapping(idOrMapping) {
    let event = await this.getEventById(idOrMapping);
    if (event) return event;
    if (this.ready) {
      const realId = await this.redis.get(KEY.map(idOrMapping));
      if (realId) {
        event = await this.getEventById(realId);
        if (event) return event;
      }
    }
    return Event.findOne({
      $or: [{ Event_ID: idOrMapping }, { mapping_id: idOrMapping }],
    }).lean();
  }

  async getRandomActiveEventIds(count = 1, excludeId = null) {
    if (!this.ready) {
      const docs = await Event.aggregate([
        { $match: { Skip_Scraping: { $ne: true } } },
        { $sample: { size: count + 1 } },
        { $project: { Event_ID: 1 } },
      ]);
      return docs
        .map((d) => d.Event_ID)
        .filter((id) => id !== excludeId)
        .slice(0, count);
    }
    const ids = await this.redis.smembers(KEY.active);
    const filtered = excludeId ? ids.filter((id) => id !== excludeId) : ids;
    // Fisher-Yates partial shuffle
    for (
      let i = filtered.length - 1;
      i > 0 && i >= filtered.length - count;
      i--
    ) {
      const j = Math.floor(Math.random() * (i + 1));
      [filtered[i], filtered[j]] = [filtered[j], filtered[i]];
    }
    return filtered.slice(0, count);
  }

  async getTotalEventCount() {
    if (!this.ready) return Event.countDocuments();
    return this.redis.scard(KEY.all);
  }

  async getActiveEventCount() {
    if (!this.ready)
      return Event.countDocuments({ Skip_Scraping: { $ne: true } });
    return this.redis.scard(KEY.active);
  }

  async getSeatGroups(eventId) {
    if (!this.ready) return ConsecutiveGroup.find({ eventId }).lean();
    const raw = await this.redis.get(KEY.seats(eventId));
    if (raw) {
      try {
        return JSON.parse(raw);
      } catch {}
    }
    const groups = await ConsecutiveGroup.find({ eventId }).lean();
    if (groups.length)
      await this.redis.set(KEY.seats(eventId), JSON.stringify(groups));
    return groups;
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  WRITES — Redis immediate, MongoDB batched
  // ══════════════════════════════════════════════════════════════════════════

  async createEvent(eventData) {
    if (!this.ready) return;
    const id = eventData.Event_ID;
    const pipe = this.redis.pipeline();
    pipe.set(KEY.event(id), JSON.stringify(eventData));
    pipe.sadd(KEY.all, id);
    if (!eventData.Skip_Scraping) {
      pipe.sadd(KEY.active, id);
      pipe.zadd(KEY.staleness, 0, id);
    }
    if (eventData.mapping_id) pipe.set(KEY.map(eventData.mapping_id), id);
    await pipe.exec();
  }

  async updateEvent(eventId, updates) {
    if (this.ready) {
      const raw = await this.redis.get(KEY.event(eventId));
      if (raw) {
        try {
          const event = JSON.parse(raw);
          Object.assign(event, updates);
          await this.redis.set(KEY.event(eventId), JSON.stringify(event));
        } catch {}
      }
    }
    this.queueWrite(eventId, updates);
  }

  async updateEventQuick(eventId, updates) {
    return this.updateEvent(eventId, updates);
  }

  async setSkipScraping(eventId, skip, extraUpdates = {}) {
    const updates = { Skip_Scraping: skip, ...extraUpdates };
    if (this.ready) {
      const raw = await this.redis.get(KEY.event(eventId));
      if (raw) {
        try {
          const event = JSON.parse(raw);
          Object.assign(event, updates);
          await this.redis.set(KEY.event(eventId), JSON.stringify(event));
        } catch {}
      }
      if (skip) {
        await this.redis.srem(KEY.active, eventId);
        await this.removeStaleness(eventId);
        await this.redis.del(KEY.lock(eventId)); // Release any processing lock
        await this.redis.del(KEY.seats(eventId)); // Clear stale seat/inventory cache
      } else {
        await this.redis.sadd(KEY.active, eventId);
        await this.updateStaleness(eventId, 0);
      }
    }
    this.queueWrite(eventId, updates);
  }

  async deleteEvent(eventId) {
    if (this.ready) {
      const raw = await this.redis.get(KEY.event(eventId));
      let mappingId = null;
      if (raw) {
        try {
          mappingId = JSON.parse(raw).mapping_id;
        } catch {}
      }
      const pipe = this.redis.pipeline();
      pipe.del(KEY.event(eventId));
      pipe.del(KEY.seats(eventId));
      pipe.srem(KEY.all, eventId);
      pipe.srem(KEY.active, eventId);
      pipe.zrem(KEY.staleness, eventId);
      pipe.del(KEY.lock(eventId));
      pipe.del(KEY.processed(eventId));
      if (mappingId) pipe.del(KEY.map(mappingId));
      await pipe.exec();
    }
    await Event.deleteOne({ Event_ID: eventId });
    await ConsecutiveGroup.deleteMany({ eventId });
  }

  async syncEventAfterTransaction(eventId) {
    try {
      const event = await Event.findOne({ Event_ID: eventId }).lean();
      if (event && this.ready) {
        await this.redis.set(KEY.event(eventId), JSON.stringify(event));
      }
    } catch (err) {
      console.error(
        `[RedisLiveStore] syncEvent error for ${eventId}: ${err.message}`
      );
    }
  }

  async refreshSeats(eventId) {
    try {
      const groups = await ConsecutiveGroup.find({ eventId }).lean();
      if (this.ready) {
        if (groups.length) {
          await this.redis.set(KEY.seats(eventId), JSON.stringify(groups));
        } else {
          await this.redis.del(KEY.seats(eventId));
        }
      }
    } catch (err) {
      console.error(
        `[RedisLiveStore] refreshSeats error for ${eventId}: ${err.message}`
      );
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  DISTRIBUTED EVENT DISTRIBUTION — sorted-set claim system
  // ══════════════════════════════════════════════════════════════════════════

  async buildStalenessIndex() {
    if (!this.ready) return;
    const ids = await this.redis.smembers(KEY.active);
    if (!ids.length) return;
    const pipe = this.redis.pipeline();
    for (const id of ids) pipe.get(KEY.event(id));
    const results = await pipe.exec();
    const zPipe = this.redis.pipeline();
    for (let i = 0; i < ids.length; i++) {
      const [err, val] = results[i];
      if (err || !val) continue;
      try {
        const event = JSON.parse(val);
        // Same rule as syncEventsFromDB: the staleness index is the TM work
        // queue, so tickets.com events are never added to it.
        if (event.source === "ticketscom") continue;
        const score = event.Last_Updated
          ? new Date(event.Last_Updated).getTime()
          : 0;
        zPipe.zadd(KEY.staleness, score, ids[i]);
      } catch {}
    }
    await zPipe.exec();
    console.log(
      `[RedisLiveStore] Staleness index built for ${ids.length} active events`
    );
  }

  async updateStaleness(eventId, score = Date.now()) {
    if (!this.ready) return;
    await this.redis.zadd(KEY.staleness, score, eventId);
  }

  async removeStaleness(eventId) {
    if (!this.ready) return;
    await this.redis.zrem(KEY.staleness, eventId);
  }

  /**
   * Fast check if an event is still active (not stopped/skipped).
   * Uses Redis SISMEMBER — O(1), sub-ms.
   *
   * Relies on syncEventsFromDB() (15s interval, distributed-locked) to keep Redis in sync
   * with MongoDB, so this stays fast without hitting the database.
   *
   * @param {string} eventId
   * @returns {Promise<boolean>}
   */
  async isEventActive(eventId) {
    if (!this.ready) {
      // Fallback to MongoDB only when Redis is down
      const ev = await Event.findOne({ Event_ID: eventId }, { Skip_Scraping: 1 }).lean();
      return ev ? !ev.Skip_Scraping : false;
    }
    return !!(await this.redis.sismember(KEY.active, eventId));
  }

  /**
   * Periodic two-way sync between MongoDB and Redis.
   *
   * The frontend writes directly to MongoDB without notifying this backend,
   * so we must poll for changes:
   *   1. Detect events STOPPED in DB → remove from Redis active + staleness
   *   2. Detect NEW/RESTARTED events in DB → add to Redis active + staleness
   *
   * Should be called on a setInterval (e.g. every 30 s).
   */
  async syncEventsFromDB() {
    if (!this.ready) return;

    // ── Distributed lock: only ONE instance out of 150+ runs this ────────
    const lockAcquired = await this.redis.set(
      KEY.syncLock,
      INSTANCE_ID,
      "EX",
      25,   // Lock expires after 25s (sync interval is 30s)
      "NX"  // Only set if not already held
    );
    if (!lockAcquired) return; // Another instance is handling the sync

    try {
      // ── Get current state from both sides ─────────────────────────────
      const [redisActiveIds, dbActiveEvents] = await Promise.all([
        this.redis.smembers(KEY.active),
        Event.find(
          { Skip_Scraping: { $ne: true } },
          { Event_ID: 1, mapping_id: 1, source: 1 }
        ).lean(),
      ]);

      const redisActiveSet = new Set(redisActiveIds);
      const dbActiveSet = new Set(dbActiveEvents.map((e) => e.Event_ID));

      // ── 1. Events stopped in DB but still active in Redis → remove ────
      const toRemove = redisActiveIds.filter((id) => !dbActiveSet.has(id));

      // ── 2. Events active in DB but missing from Redis → add ───────────
      const toAdd = dbActiveEvents.filter((e) => !redisActiveSet.has(e.Event_ID));

      if (!toRemove.length && !toAdd.length) return; // Nothing to sync

      // ── Batch-fetch all needed events from MongoDB in ONE query ────────
      const idsToFetch = [
        ...toRemove,
        ...toAdd.map((e) => e.Event_ID),
      ];
      const freshEvents = idsToFetch.length
        ? await Event.find({ Event_ID: { $in: idsToFetch } }).lean()
        : [];
      const freshMap = new Map(freshEvents.map((e) => [e.Event_ID, e]));

      const pipe = this.redis.pipeline();

      // Remove stopped events — update JSON blob + clear stale seat/lock data
      for (const id of toRemove) {
        pipe.srem(KEY.active, id);
        pipe.zrem(KEY.staleness, id);
        pipe.del(KEY.lock(id));
        pipe.del(KEY.seats(id)); // Clear stale inventory/seat data
        const freshEvent = freshMap.get(id);
        if (freshEvent) {
          pipe.set(KEY.event(id), JSON.stringify(freshEvent));
        }
      }

      // Add new/restarted events
      for (const ev of toAdd) {
        const fullEvent = freshMap.get(ev.Event_ID);
        if (fullEvent) {
          pipe.set(KEY.event(ev.Event_ID), JSON.stringify(fullEvent));
          pipe.sadd(KEY.all, ev.Event_ID);
          pipe.sadd(KEY.active, ev.Event_ID);
          // Staleness IS the Ticketmaster work queue — claimEvents() reads it.
          // tickets.com events stay out of it so TM instances never claim an
          // event they cannot scrape; they are queued by the tickets.com
          // manager in its own tc: namespace instead. They DO stay in the
          // active set so portal reads and stats still count them.
          if (fullEvent.source !== "ticketscom") {
            // Score 0 = highest priority (will be claimed immediately)
            pipe.zadd(KEY.staleness, 0, ev.Event_ID);
          }
          if (fullEvent.mapping_id) {
            pipe.set(KEY.map(fullEvent.mapping_id), ev.Event_ID);
          }
        }
      }

      await pipe.exec();

      if (toRemove.length) {
        console.log(
          `[RedisLiveStore] Sync: removed ${toRemove.length} stopped events from Redis (data + seats refreshed)`
        );
      }
      if (toAdd.length) {
        console.log(
          `[RedisLiveStore] Sync: added ${toAdd.length} new/restarted events to Redis`
        );
      }
    } catch (err) {
      console.error("[RedisLiveStore] syncEventsFromDB error:", err.message);
    } finally {
      // Release lock early so next cycle can proceed
      const current = await this.redis.get(KEY.syncLock);
      if (current === INSTANCE_ID) {
        await this.redis.del(KEY.syncLock).catch(() => {});
      }
    }
  }

  /**
   * Atomically claim the N stalest events that no other instance owns.
   *
   *   1. ZRANGEBYSCORE evt:staleness 0 (now-30s) LIMIT 0 N*3
   *   2. For each: SET lock:processing:{id} NX EX 90
   *   3. Return only the ones where NX succeeded
   *
   * @param {number} count  Max events to claim
   * @returns {Promise<string[]>}
   */
  async claimEvents(count = 5) {
    if (!this.ready) {
      // MongoDB fallback: get stalest active events. Excludes tickets.com for
      // the same reason the Redis path does — this is the TM work queue.
      const events = await Event.find(
        { Skip_Scraping: { $ne: true }, source: { $ne: "ticketscom" } },
        { Event_ID: 1, Last_Updated: 1 }
      )
        .sort({ Last_Updated: 1 })
        .limit(count)
        .lean();
      return events.map((e) => e.Event_ID);
    }

    const threshold = Date.now() - 30000;
    const candidates = await this.redis.zrangebyscore(
      KEY.staleness,
      0,
      threshold,
      "LIMIT",
      0,
      count * 3
    );
    if (!candidates.length) return [];

    // Pre-filter: skip candidates that are no longer in the active set.
    // This catches events removed by syncEventsFromDB/setSkipScraping
    // before we waste a lock attempt on them.
    const activePipe = this.redis.pipeline();
    for (const id of candidates) {
      activePipe.sismember(KEY.active, id);
    }
    const activeResults = await activePipe.exec();
    const activeCandidates = candidates.filter((_, i) => {
      const [err, isMember] = activeResults[i];
      return !err && isMember === 1;
    });

    // Clean up stale staleness entries for stopped events
    const staleEntries = candidates.filter((_, i) => {
      const [err, isMember] = activeResults[i];
      return !err && isMember === 0;
    });
    if (staleEntries.length) {
      const cleanPipe = this.redis.pipeline();
      for (const id of staleEntries) {
        cleanPipe.zrem(KEY.staleness, id);
      }
      cleanPipe.exec().catch(() => {}); // Fire-and-forget cleanup
    }

    if (!activeCandidates.length) return [];

    const toTry = activeCandidates.slice(0, Math.min(activeCandidates.length, count * 2));
    const pipe = this.redis.pipeline();
    for (const id of toTry) {
      pipe.set(KEY.lock(id), INSTANCE_ID, "EX", 90, "NX");
    }
    const results = await pipe.exec();

    const won = [];
    for (let i = 0; i < toTry.length; i++) {
      if (won.length >= count) break;
      const [err, result] = results[i];
      if (!err && result === "OK") {
        won.push(toTry[i]);
      }
    }
    return won;
  }

  /**
   * Release a claimed event after processing.
   *
   * Lua script atomically: DEL lock (if we own it) + ZADD new staleness score.
   */
  async releaseEvent(eventId, success = true) {
    if (!this.ready) {
      // MongoDB fallback: update Last_Updated so it moves to the back of the queue
      if (success) {
        await Event.updateOne({ Event_ID: eventId }, { $set: { Last_Updated: new Date() } }).catch(() => {});
      }
      return;
    }
    const newScore = success ? Date.now() : Date.now() - 60000;
    // Atomically: DEL lock (if we own it), then:
    //   - If event is still active → ZADD to staleness (re-queue)
    //   - If event was stopped   → ZREM from staleness (prevent re-claim)
    const lua = `
      local lock = redis.call('GET', KEYS[1])
      if lock == ARGV[1] then
        redis.call('DEL', KEYS[1])
      end
      if redis.call('SISMEMBER', KEYS[3], ARGV[3]) == 1 then
        redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
      else
        redis.call('ZREM', KEYS[2], ARGV[3])
      end
      return 1
    `;
    await this.redis.eval(
      lua,
      3,
      KEY.lock(eventId),
      KEY.staleness,
      KEY.active,
      INSTANCE_ID,
      newScore,
      eventId
    );
  }

  async getOverdueEvents(thresholdMs = 120000) {
    if (!this.ready) return [];
    const cutoff = Date.now() - thresholdMs;
    return this.redis.zrangebyscore(KEY.staleness, 0, cutoff);
  }

  async getStalenessStats() {
    if (!this.ready) return null;
    const total = await this.redis.zcard(KEY.staleness);
    if (!total) return { total: 0, onTime: 0, overdue: 0, compliance: 100 };
    const cutoff = Date.now() - 120000;
    const overdue = await this.redis.zcount(KEY.staleness, 0, cutoff);
    const onTime = total - overdue;
    const oldest = await this.redis.zrangebyscore(
      KEY.staleness,
      0,
      "+inf",
      "WITHSCORES",
      "LIMIT",
      0,
      1
    );
    let oldestAge = null;
    if (oldest.length >= 2) {
      oldestAge = Math.round((Date.now() - parseInt(oldest[1], 10)) / 1000);
    }
    return {
      total,
      onTime,
      overdue,
      compliance: Math.round((onTime / total) * 10000) / 100,
      oldestEventAgeSec: oldestAge,
    };
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  LEGACY COMPAT
  // ══════════════════════════════════════════════════════════════════════════

  async acquireProcessingLock(eventId) {
    if (!this.ready) return true;
    const result = await this.redis.set(
      KEY.lock(eventId),
      INSTANCE_ID,
      "EX",
      90,
      "NX"
    );
    return result === "OK";
  }

  async releaseProcessingLock(eventId) {
    if (!this.ready) return;
    const current = await this.redis.get(KEY.lock(eventId));
    if (current === INSTANCE_ID) await this.redis.del(KEY.lock(eventId));
  }

  async markEventProcessed(eventId) {
    if (!this.ready) {
      await Event.updateOne({ Event_ID: eventId }, { $set: { Last_Updated: new Date() } }).catch(() => {});
      return;
    }
    const now = Date.now();
    const pipe = this.redis.pipeline();
    pipe.set(KEY.processed(eventId), now.toString(), "EX", 120);
    pipe.zadd(KEY.staleness, now, eventId);
    await pipe.exec();
  }

  async getLastProcessedTime(eventId) {
    if (!this.ready) {
      const ev = await Event.findOne({ Event_ID: eventId }, { Last_Updated: 1 }).lean();
      return ev?.Last_Updated ? new Date(ev.Last_Updated).getTime() : null;
    }
    const val = await this.redis.get(KEY.processed(eventId));
    return val ? parseInt(val, 10) : null;
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  INSTANCE MANAGEMENT
  // ══════════════════════════════════════════════════════════════════════════

  async heartbeat() {
    if (!this.ready) return;
    await this.redis.set(
      KEY.instance(INSTANCE_ID),
      Date.now().toString(),
      "EX",
      90
    );
  }

  async getActiveInstances() {
    if (!this.ready) return 0;
    let cursor = "0";
    let count = 0;
    do {
      const [next, keys] = await this.redis.scan(
        cursor,
        "MATCH",
        "inst:*",
        "COUNT",
        200
      );
      cursor = next;
      count += keys.length;
    } while (cursor !== "0");
    return count;
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  STATS
  // ══════════════════════════════════════════════════════════════════════════

  async getStats() {
    if (!this.ready) return { redis: false };
    const [totalEvents, activeEvents, stalenessStats] = await Promise.all([
      this.redis.scard(KEY.all),
      this.redis.scard(KEY.active),
      this.getStalenessStats(),
    ]);
    return {
      redis: true,
      instanceId: INSTANCE_ID,
      totalEvents,
      activeEvents,
      staleness: stalenessStats,
      writeBuffer: this.getBufferStats(),
    };
  }
}

// ── Utility ─────────────────────────────────────────────────────────────────
function pick(obj, fields) {
  const out = {};
  for (const f of fields) {
    if (obj[f] !== undefined) out[f] = obj[f];
  }
  return out;
}

// ── Singleton ───────────────────────────────────────────────────────────────
const redisLiveStore = new RedisLiveStore();
export default redisLiveStore;
