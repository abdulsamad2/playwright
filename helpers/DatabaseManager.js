import mongoose from "mongoose";
import moment from "moment";
import { Event, ConsecutiveGroup } from "../models/index.js";
import config from "../config/scraperConfig.js";

/**
 * Handles database operations for the scraper
 */
class DatabaseManager {
  constructor(logger) {
    this.logger = logger;
  }

  /**
   * Fetch an event by ID with specified fields
   */
  async getEventById(eventId, fields = ["Skip_Scraping", "inHandDate", "url"]) {
    try {
      return await Event.findOne({ Event_ID: eventId })
        .select(fields.join(" "))
        .lean();
    } catch (error) {
      this.logger.logWithTime(`Error fetching event ${eventId}: ${error.message}`, "error");
      return null;
    }
  }

  /**
   * Clean up consecutive seats for a stopped event
   * @param {string} eventId - The event ID
   * @returns {Promise<number>} Number of deleted consecutive seat groups
   */
  async cleanupConsecutiveSeats(eventId) {
    try {
      const deleteResult = await ConsecutiveGroup.deleteMany({ eventId });
      
      if (deleteResult.deletedCount > 0) {
        this.logger?.logWithTime(
          `🧹 Cleaned up ${deleteResult.deletedCount} consecutive seat groups for stopped event ${eventId}`,
          "info"
        );
      }
      
      return deleteResult.deletedCount;
    } catch (error) {
      this.logger?.logWithTime(
        `Error cleaning up consecutive seats for event ${eventId}: ${error.message}`,
        "error"
      );
      return 0;
    }
  }

  /**
   * Update event metadata and ticket info
   */
  async updateEventMetadata(eventId, scrapeResult, scheduler) {
    const LOG_LEVEL = (config && typeof config.LOG_LEVEL !== 'undefined') ? config.LOG_LEVEL : (process.env.LOG_LEVEL || 2);
    const CHUNK_SIZE = (config && config.CHUNK_SIZE) ? config.CHUNK_SIZE : 100;
    const startTime = performance.now();
    const session = await Event.startSession();

    try {
      await session.withTransaction(async () => {
        const event = await Event.findOne({ Event_ID: eventId }).session(
          session
        );
        if (!event) {
          throw new Error(
            `Event ${eventId} not found in database for metadata update`
          );
        }

        const previousTicketCount = event.Available_Seats || 0;
        const currentTicketCount = scrapeResult.length;

        const metadata = {
          lastUpdate: moment().format("YYYY-MM-DD HH:mm:ss"),
          iterationNumber: (event.metadata?.iterationNumber || 0) + 1,
          scrapeDurationMs: performance.now() - startTime,
          ticketStats: {
            totalTickets: currentTicketCount,
            ticketCountChange: currentTicketCount - previousTicketCount,
            previousTicketCount,
          },
        };

        // First update the basic event info
        await Event.updateOne(
          { Event_ID: eventId },
          {
            $set: {
              Available_Seats: currentTicketCount,
              Last_Updated: new Date(),
              "metadata.basic": metadata,
            },
          }
        ).session(session);

        if (scrapeResult?.length > 0) {
          // Optimized seat comparison with consistent property access
          const existingGroups = await ConsecutiveGroup.find(
            { eventId },
            { section: 1, row: 1, seats: 1, "inventory.listPrice": 1 }
          ).lean().session(session);

          const existingSeats = new Set(
            existingGroups.flatMap((g) =>
              g.seats.map((s) => `${g.section}-${g.row}-${s.number.toString()}-${g.inventory.listPrice}`)
            )
          );

          const newSeats = new Set(
            scrapeResult.flatMap((g) =>
              g.seats.map(
                (s) => `${g.section}-${g.row}-${s.toString()}-${g.inventory.listPrice}`
              )
            )
          );

          // ADD DIAGNOSTIC LOGGING
          if (LOG_LEVEL >= 3) { // Assuming 3 is a debug level
            this.logger.logWithTime(`[Debug ${eventId}] Existing Seats Count: ${existingSeats.size}`, "debug");
            if (existingSeats.size > 0) this.logger.logWithTime(`[Debug ${eventId}] Sample Existing Seats: ${Array.from(existingSeats).slice(0, 3).join(', ')}`, "debug");
            this.logger.logWithTime(`[Debug ${eventId}] New Seats Count: ${newSeats.size}`, "debug");
            if (newSeats.size > 0) this.logger.logWithTime(`[Debug ${eventId}] Sample New Seats: ${Array.from(newSeats).slice(0, 3).join(', ')}`, "debug");
          }

          // ROBUST CHANGE DETECTION LOGIC
          const hasChanges =
            existingSeats.size !== newSeats.size ||
            [...existingSeats].some((s) => !newSeats.has(s)) ||
            [...newSeats].some((s) => !existingSeats.has(s));

          if (LOG_LEVEL >= 3) { // Assuming 3 is a debug level
            this.logger.logWithTime(`[Debug ${eventId}] Change Detected for ConsecutiveGroup: ${hasChanges}`, "debug");
          }

          if (hasChanges) {
            // Bulk delete and insert for better performance
            await ConsecutiveGroup.deleteMany({ eventId }).session(session);

            const groupsToInsert = scrapeResult.map((group) => ({
              eventId,
              section: group.section,
              row: group.row,
              seatCount: group.inventory.quantity,
              seatRange: `${Math.min(...group.seats)}-${Math.max(
                ...group.seats
              )}`,
              seats: group.seats.map((seatNumber) => ({
                number: seatNumber.toString(),
                inHandDate: event.inHandDate,
                price: group.inventory.listPrice,
              })),
              inventory: {
                ...group.inventory,
                inHandDate: event.inHandDate,
                eventId,
                mapping_id: event.mapping_id,
                event_name: event.Event_Name,
                venue_name: event.Venue,
                event_date: event.Event_DateTime,
              },
              inHandDate: event.inHandDate,
              mapping_id: event.mapping_id,
              event_name: event.Event_Name,
              venue_name: event.Venue,
              event_date: event.Event_DateTime,
            }));

            // Insert in chunks to avoid memory issues
            for (let i = 0; i < groupsToInsert.length; i += CHUNK_SIZE) {
              const chunk = groupsToInsert.slice(i, i + CHUNK_SIZE);
              await ConsecutiveGroup.insertMany(chunk, { session });
            }

            this.logger.logWithTime(
              `Updated ${groupsToInsert.length} consecutive groups for event ${eventId}`,
              "info"
            );
          } else {
            this.logger.logWithTime(
              `No changes detected for event ${eventId} - skipping ConsecutiveGroup update`,
              "debug"
            );
          }
        } else {
          // No scrape results - clean up existing groups
          const deleteResult = await ConsecutiveGroup.deleteMany({ eventId }).session(session);
          if (deleteResult.deletedCount > 0) {
            this.logger.logWithTime(
              `Removed ${deleteResult.deletedCount} consecutive groups for event ${eventId} (no scrape results)`,
              "info"
            );
          }
        }
      });
    } catch (error) {
      this.logger.logWithTime(
        `Error updating event metadata for ${eventId}: ${error.message}`,
        "error"
      );
      throw error;
    } finally {
      await session.endSession();
    }
  }
}

export default DatabaseManager;