import express from "express";
import { Scraper } from "./scraper.js";
import morgan from "morgan";
import fs from "fs";

class ScrapingManager {
  constructor() {
    this.activeScrapers = new Map(); // URL -> {scraper, status, lastUpdate}
    this.results = new Map(); // URL -> latest results
    this.maxConcurrentScrapes = 1; // Reduced to 1 for better stability
    this.previousTicketCounts = new Map();
  }

  async startScraping(url) {
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
    };

    this.activeScrapers.set(url, scrapingInfo);
    this.previousTicketCounts.set(url, 0);

    this.scrapeUrlContinuously(url);
    return { status: "started", url };
  }

  async scrapeUrlContinuously(url) {
    // Initial delay before starting scraping
    await new Promise((resolve) => setTimeout(resolve, 2000));
    const scrapingInfo = this.activeScrapers.get(url);
    if (!scrapingInfo) return;

    try {
      while (scrapingInfo.status === "active") {
        const iterationStart = Date.now();
        console.log(
          `Starting scrape iteration ${scrapingInfo.iteration} for: ${url}`
        );

        // Add retry logic
        let retryCount = 0;
        const maxRetries = 3;
        let result;

        while (retryCount < maxRetries) {
          try {
            result = await scrapingInfo.scraper.scrape(url);
            break; // Success - exit retry loop
          } catch (error) {
            retryCount++;
            console.log(
              `Attempt ${retryCount} failed, retrying in 10 seconds...`
            );
            if (retryCount === maxRetries) throw error;
            await new Promise((resolve) => setTimeout(resolve, 10000)); // 10 second delay between retries
          }
        }

        const currentTicketCount = result.tickets?.length || 0;
        const previousCount = this.previousTicketCounts.get(url);
        const ticketCountDiff = currentTicketCount - previousCount;
        this.previousTicketCounts.set(url, currentTicketCount);

        const iterationEnd = Date.now();
        const iterationDuration = (iterationEnd - iterationStart) / 1000;

        const enrichedResult = {
          ...result,
          metadata: {
            lastUpdate: new Date().toLocaleTimeString(),
            iterationNumber: scrapingInfo.iteration,
            scrapeStartTime: new Date(iterationStart).toISOString(),
            scrapeEndTime: new Date(iterationEnd).toISOString(),
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
          },
        };

        this.results.set(url, enrichedResult);
        scrapingInfo.lastUpdate = new Date();

        // Save results to file
        fs.writeFileSync(
          "result.json",
          JSON.stringify(enrichedResult, null, 2)
        );

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

        // Wait 5 seconds between scrapes
        await new Promise((resolve) => setTimeout(resolve, 5000));
        scrapingInfo.iteration++;
      }
    } catch (error) {
      console.error(`Error scraping URL ${url}:`, error);
      scrapingInfo.status = "error";
      scrapingInfo.error = error.message;

      fs.writeFileSync(
        "error_log.json",
        JSON.stringify(
          {
            error: error.message,
            stack: error.stack,
            timestamp: new Date().toISOString(),
            totalRunTime:
              ((Date.now() - scrapingInfo.startTime) / 1000 / 60).toFixed(2) +
              " minutes",
            lastIteration: scrapingInfo.iteration,
            lastKnownTicketCount: this.previousTicketCounts.get(url),
          },
          null,
          2
        )
      );
    }
  }

  async stopScraping(url) {
    const scrapingInfo = this.activeScrapers.get(url);
    if (!scrapingInfo) {
      throw new Error("URL not found");
    }

    scrapingInfo.status = "stopped";
    await scrapingInfo.scraper.close();
    this.activeScrapers.delete(url);
    this.previousTicketCounts.delete(url);
    return {
      status: "stopped",
      url,
      totalRunTime:
        ((Date.now() - scrapingInfo.startTime) / 1000 / 60).toFixed(2) +
        " minutes",
    };
  }

  getStatus(url) {
    if (url) {
      const scrapingInfo = this.activeScrapers.get(url);
      const results = this.results.get(url);
      if (!scrapingInfo && !results) {
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
        results: results || null,
      };
    }

    return {
      activeScrapers: Array.from(this.activeScrapers.entries()).map(
        ([activeUrl, info]) => ({
          url: activeUrl,
          status: info.status,
          lastUpdate: info.lastUpdate,
          iteration: info.iteration,
          uptime:
            ((Date.now() - info.startTime) / 1000 / 60).toFixed(2) + " minutes",
        })
      ),
    };
  }
}

// Create Express server
const app = express();
const port = process.env.PORT || 3000;
const scrapingManager = new ScrapingManager();

// Middleware
app.use(express.json());
app.use(morgan("dev"));

// Routes
app.post("/scrape", async (req, res) => {
  try {
    const { url } = req.body;
    if (!url) {
      return res.status(400).json({
        error: "Missing URL",
        status: "error",
      });
    }

    const result = await scrapingManager.startScraping(url);
    res.json(result);
  } catch (error) {
    res.status(400).json({
      error: error.message,
      status: "error",
    });
  }
});

app.delete("/scrape", async (req, res) => {
  try {
    const { url } = req.body;
    if (!url) {
      return res.status(400).json({
        error: "Missing URL",
        status: "error",
      });
    }
    const result = await scrapingManager.stopScraping(url);
    res.json(result);
  } catch (error) {
    res.status(404).json({
      error: error.message,
      status: "error",
    });
  }
});

app.get("/status", (req, res) => {
  try {
    const { url } = req.query;
    const status = scrapingManager.getStatus(url);
    res.json(status);
  } catch (error) {
    res.status(404).json({
      error: error.message,
      status: "error",
    });
  }
});

app.get("/results", (req, res) => {
  try {
    const { url } = req.query;
    if (!url) {
      return res.status(400).json({
        error: "Missing URL parameter",
        status: "error",
      });
    }
    const results = scrapingManager.results.get(url);
    if (!results) {
      return res.status(404).json({
        error: "No results found for this URL",
        status: "error",
      });
    }
    res.json(results);
  } catch (error) {
    res.status(500).json({
      error: error.message,
      status: "error",
    });
  }
});

// Start server
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
