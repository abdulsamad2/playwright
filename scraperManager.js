import Scraper from "./scraper.js";
import { Event, ConsecutiveGroup, ErrorLog } from "./models/index.js";
import _ from "lodash";
import { Worker, isMainThread, parentPort, workerData } from "worker_threads";
import os from "os";

export class ScrapingManager {
  constructor() {
    this.activeScrapers = new Map();
    this.maxConcurrentScrapes = 600;
    this.previousConsecutiveGroups = new Map();
    this.workerPool = [];
    this.maxWorkers = os.cpus().length;
  }

  async logError(url, errorType, error, additionalInfo = {}) {
    try {
      const errorLog = new ErrorLog({
        eventUrl: url,
        errorType,
        message: error.message,
        stack: error.stack,
        metadata: {
          iteration: this.activeScrapers.get(url)?.iteration || 0,
          additionalInfo,
        },
      });
      await errorLog.save();
    } catch (logError) {
      console.error("Error logging to database:", logError);
    }
  }

  hasSeatingChanged(url, newGroups) {
    const previousGroups = this.previousConsecutiveGroups.get(url);
    if (!previousGroups) return true;

    const sortGroups = (groups) =>
      _.sortBy(groups, ["section", "row", "seatRange"]);
    const normalizeGroups = (groups) =>
      groups.map((group) => ({
        section: group.section,
        row: group.row,
        seatCount: group.seatCount,
        seatRange: group.seatRange,
        seats: _.sortBy(group.seats, "number"),
      }));

    const previousSorted = sortGroups(normalizeGroups(previousGroups));
    const newSorted = sortGroups(normalizeGroups(newGroups));

    return !_.isEqual(previousSorted, newSorted);
  }

  async saveOrUpdateEvent(eventData, scrapedData, metadata, url) {
    try {
      let event = await Event.findOne({ url });

      const parseDateString = (dateStr) => {
        const parts = dateStr.split("•").map((s) => s.trim());
        if (parts.length === 3) {
          const [dayOfWeek, date, time] = parts;
          const dateTime = new Date(`${date} ${time}`);
          if (!isNaN(dateTime.getTime())) {
            return dateTime;
          }
        }
        throw new Error(`Invalid date format: ${dateStr}`);
      };

      if (!event) {
        event = new Event({
          title: eventData.title,
          dateTime: parseDateString(eventData.dateTime),
          url: url,
          availableSeats: scrapedData.seatingInfo.availableSeats,
          metadata: metadata,
        });
      } else {
        event.metadata = metadata;
        event.lastUpdated = new Date();
        event.dateTime = parseDateString(eventData.dateTime);
        event.availableSeats = scrapedData.seatingInfo.availableSeats;
      }

      await event.save();

      // Only update consecutive groups if we have them (full scrape)
      if (
        !scrapedData.quickCheckOnly &&
        scrapedData.seatingInfo.consecutiveGroups?.length > 0
      ) {
        if (
          this.hasSeatingChanged(url, scrapedData.seatingInfo.consecutiveGroups)
        ) {
          await ConsecutiveGroup.deleteMany({ eventId: event._id });

          const groupPromises = scrapedData.seatingInfo.consecutiveGroups.map(
            (group) => {
              return new ConsecutiveGroup({
                eventId: event._id,
                section: group.section,
                row: group.row,
                seatCount: group.seatCount,
                seatRange: group.seatRange,
                seats: group.seats.map((seat) => ({
                  number: seat.number,
                  price: seat.price,
                })),
              }).save();
            }
          );

          await Promise.all(groupPromises);
          this.previousConsecutiveGroups.set(
            url,
            scrapedData.seatingInfo.consecutiveGroups
          );
        }
      }

      return event;
    } catch (error) {
      throw new Error(`Database save error: ${error.message}`);
    }
  }

  async startScraping(url) {
    try {
      if (this.activeScrapers.size >= this.maxConcurrentScrapes) {
        throw new Error("Maximum concurrent scraping limit reached");
      }

      if (this.activeScrapers.has(url)) {
        throw new Error("URL is already being scraped");
      }

      const scraper = new Scraper();
      const scrapingInfo = {
        scraper,
        status: "active",
        startTime: Date.now(),
        lastUpdate: new Date(),
        error: null,
        iteration: 1,
        consecutiveErrors: 0,
      };

      this.activeScrapers.set(url, scrapingInfo);

      this.scrapeUrlContinuously(url);
      return { status: "started", url };
    } catch (error) {
      await this.logError(url, "VALIDATION_ERROR", error);
      throw error;
    }
  }

  async scrapeUrlContinuously(url) {
    await new Promise((resolve) => setTimeout(resolve, 2000));
    const scrapingInfo = this.activeScrapers.get(url);
    if (!scrapingInfo) return;

    try {
      while (scrapingInfo.status === "active") {
        const iterationStart = Date.now();
        console.log(
          `\n=== Starting iteration ${scrapingInfo.iteration} for: ${url} ===`
        );

        let retryCount = 0;
        const maxRetries = 3;
        let success = false;

        while (retryCount < maxRetries && !success) {
          try {
            const rawData = await scrapingInfo.scraper.scrape(url);
            const scrapedData =
              typeof rawData === "string" ? JSON.parse(rawData) : rawData;

            const metadata = {
              lastUpdate: new Date().toISOString(),
              iterationNumber: scrapingInfo.iteration,
              scrapeStartTime: new Date(iterationStart),
              scrapeEndTime: new Date(),
              scrapeDuration: scrapedData.scrapeDuration,
              scrapeType: scrapedData.quickCheckOnly ? "quick" : "full",
              totalRunningTimeMinutes: (
                (Date.now() - scrapingInfo.startTime) /
                1000 /
                60
              ).toFixed(2),
              ticketStats: {
                totalTickets: scrapedData.seatingInfo.availableSeats,
                previousTicketCount:
                  scrapingInfo.lastTicketCount ||
                  scrapedData.seatingInfo.availableSeats,
                ticketCountChange:
                  scrapedData.seatingInfo.availableSeats -
                  (scrapingInfo.lastTicketCount ||
                    scrapedData.seatingInfo.availableSeats),
              },
            };

            const event = await this.saveOrUpdateEvent(
              scrapedData.eventInfo,
              scrapedData,
              metadata,
              url
            );

            success = true;
            scrapingInfo.consecutiveErrors = 0;
            scrapingInfo.lastUpdate = new Date();
            scrapingInfo.lastTicketCount =
              scrapedData.seatingInfo.availableSeats;

            console.log(`Iteration ${scrapingInfo.iteration} completed:`);
            console.log(`- Scrape type: ${metadata.scrapeType}`);
            console.log(`- Duration: ${metadata.scrapeDuration}s`);
            console.log(
              `- Available seats: ${metadata.ticketStats.totalTickets}`
            );
            console.log(
              `- Seat change: ${metadata.ticketStats.ticketCountChange}`
            );
            console.log("===============================\n");

            break;
          } catch (error) {
            retryCount++;
            console.error(`Attempt ${retryCount} failed:`, error);

            await this.logError(url, "SCRAPE_ERROR", error, {
              retryCount,
              iteration: scrapingInfo.iteration,
            });

            if (retryCount === maxRetries) {
              scrapingInfo.consecutiveErrors =
                (scrapingInfo.consecutiveErrors || 0) + 1;
              if (scrapingInfo.consecutiveErrors >= 5) {
                console.error("Too many consecutive errors, stopping scraper");
                await this.stopScraping(url);
                return;
              }
              throw error;
            }
            await new Promise((resolve) => setTimeout(resolve, 10000));
          }
        }

        await new Promise((resolve) => setTimeout(resolve, 5000));
        scrapingInfo.iteration++;
      }
    } catch (error) {
      console.error(`Error scraping URL ${url}:`, error);
      scrapingInfo.status = "error";
      scrapingInfo.error = error.message;
      await this.logError(url, "SCRAPE_ERROR", error);
    }
  }

  async stopScraping(url) {
    try {
      const scrapingInfo = this.activeScrapers.get(url);
      if (!scrapingInfo) {
        throw new Error("URL not found");
      }

      scrapingInfo.status = "stopped";
      await scrapingInfo.scraper.close();
      this.activeScrapers.delete(url);
      this.previousConsecutiveGroups.delete(url);

      return {
        status: "stopped",
        url,
        totalRunTime:
          ((Date.now() - scrapingInfo.startTime) / 1000 / 60).toFixed(2) +
          " minutes",
      };
    } catch (error) {
      await this.logError(url, "VALIDATION_ERROR", error);
      throw error;
    }
  }

  async getStatus(url) {
    try {
      if (url) {
        const scrapingInfo = this.activeScrapers.get(url);
        const event = await Event.findOne({ url }).sort({ lastUpdated: -1 });
        const recentErrors = await ErrorLog.find({ eventUrl: url })
          .sort({ createdAt: -1 })
          .limit(5);

        if (!scrapingInfo && !event) {
          throw new Error("URL not found");
        }

        return {
          status: scrapingInfo?.status || "inactive",
          lastUpdate: scrapingInfo?.lastUpdate || null,
          iteration: scrapingInfo?.iteration || 0,
          uptime: scrapingInfo
            ? ((Date.now() - scrapingInfo.startTime) / 1000 / 60).toFixed(2) +
              " minutes"
            : null,
          event: event || null,
          recentErrors: recentErrors,
          consecutiveErrors: scrapingInfo?.consecutiveErrors || 0,
        };
      }

      return {
        activeScrapers: await Promise.all(
          Array.from(this.activeScrapers.entries()).map(
            async ([activeUrl, info]) => {
              const recentErrors = await ErrorLog.find({ eventUrl: activeUrl })
                .sort({ createdAt: -1 })
                .limit(1);

              return {
                url: activeUrl,
                status: info.status,
                lastUpdate: info.lastUpdate,
                iteration: info.iteration,
                uptime:
                  ((Date.now() - info.startTime) / 1000 / 60).toFixed(2) +
                  " minutes",
                lastError: recentErrors[0] || null,
                consecutiveErrors: info.consecutiveErrors || 0,
              };
            }
          )
        ),
      };
    } catch (error) {
      await this.logError(url, "VALIDATION_ERROR", error);
      throw error;
    }
  }

  async getErrors(url, limit = 10) {
    try {
      const query = url ? { eventUrl: url } : {};
      return await ErrorLog.find(query).sort({ createdAt: -1 }).limit(limit);
    } catch (error) {
      console.error("Error fetching error logs:", error);
      throw error;
    }
  }
}

export default ScrapingManager;