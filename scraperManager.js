import Scraper from "./scraper.js";
import seatService from "./services/seatService.js";
import { Event, ErrorLog } from "./models/index.js";
import _ from "lodash";
import { Worker, isMainThread, parentPort, workerData } from "worker_threads";
import os from "os";

export class ScrapingManager {
  constructor() {
    this.activeScrapers = new Map();
    this.maxConcurrentScrapes = 600;
    this.previousTicketCounts = new Map();
    this.previousScrapedData = new Map();
    this.workerPool = [];
    this.maxWorkers = os.cpus().length; // Use number of CPU cores
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

  hasDataChanged(url, newData) {
    const previousData = this.previousScrapedData.get(url);
    if (!previousData) return true;

    const previousTickets = _.sortBy(previousData.tickets, [
      "section",
      "ticketType",
      "price",
    ]);
    const newTickets = _.sortBy(newData.tickets, [
      "section",
      "ticketType",
      "price",
    ]);

    return !_.isEqual(previousTickets, newTickets);
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
      this.previousTicketCounts.set(url, 0);

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
          `Starting scrape iteration ${scrapingInfo.iteration} for: ${url}`
        );

        let retryCount = 0;
        const maxRetries = 3;
        let success = false;

        while (retryCount < maxRetries && !success) {
          try {
            const scrapedData = JSON.parse(
              await scrapingInfo.scraper.scrape(url)
            );

            if (!this.hasDataChanged(url, scrapedData)) {
              console.log("No changes detected, skipping database update");
              success = true;
              scrapingInfo.consecutiveErrors = 0;
              break;
            }

            const processedSeats = seatService.processSeats(
              scrapedData.tickets
            );
            const currentTicketCount = scrapedData.tickets?.length || 0;
            const previousCount = this.previousTicketCounts.get(url);
            const ticketCountDiff = currentTicketCount - previousCount;

            const iterationEnd = Date.now();
            const iterationDuration = (iterationEnd - iterationStart) / 1000;

            const metadata = {
              lastUpdate: new Date().toLocaleTimeString(),
              iterationNumber: scrapingInfo.iteration,
              scrapeStartTime: new Date(iterationStart),
              scrapeEndTime: new Date(iterationEnd),
              scrapeDurationSeconds: iterationDuration,
              totalRunningTimeMinutes: (
                (Date.now() - scrapingInfo.startTime) /
                1000 /
                60
              ).toFixed(2),
              ticketStats: {
                totalTickets: currentTicketCount,
                ticketCountChange: ticketCountDiff,
                previousTicketCount: previousCount,
              },
            };

            await seatService.saveToDatabase(
              scrapedData.eventInfo,
              processedSeats,
              metadata,
              url
            );

            this.previousScrapedData.set(url, scrapedData);
            this.previousTicketCounts.set(url, currentTicketCount);

            console.log(`Total tickets: ${currentTicketCount}`);
            console.log(
              `Ticket change: ${
                ticketCountDiff > 0 ? "+" + ticketCountDiff : ticketCountDiff
              }`
            );
            console.log(
              `Iteration ${
                scrapingInfo.iteration
              } completed in ${iterationDuration.toFixed(2)} seconds`
            );

            success = true;
            scrapingInfo.consecutiveErrors = 0;
            break;
          } catch (error) {
            retryCount++;
            console.log(
              `Attempt ${retryCount} failed, retrying in 10 seconds...`
            );

            await this.logError(url, "SCRAPE_ERROR", error, {
              retryCount,
              iteration: scrapingInfo.iteration,
            });

            if (retryCount === maxRetries) {
              scrapingInfo.consecutiveErrors =
                (scrapingInfo.consecutiveErrors || 0) + 1;

              if (scrapingInfo.consecutiveErrors >= 5) {
                await this.logError(
                  url,
                  "VALIDATION_ERROR",
                  new Error("Too many consecutive errors"),
                  {
                    consecutiveErrors: scrapingInfo.consecutiveErrors,
                  }
                );
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
      this.previousTicketCounts.delete(url);
      this.previousScrapedData.delete(url);

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
