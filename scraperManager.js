import SeatsAPIScraper from "./scraper.js";
import fs from "fs/promises";
import path from "path";
import { processAndGroupSeats } from "./grouping.js";
class SimpleScrapingManager {
  constructor() {
    this.scraper = null;
    this.isRunning = false;
  }

  async saveSeatsData(eventId, seatsData) {
    try {
      // Create directory if it doesn't exist
      const dataDir = "./seats_data";
      await fs.mkdir(dataDir, { recursive: true });

      // Create filename with timestamp
      const fileName = `seats_${eventId}_${Date.now()}.json`;

      // Save to file
      await fs.writeFile(
        path.join(dataDir, fileName),
        JSON.stringify(seatsData, null, 2)
      );

      console.log(`Data saved to ${fileName}`);
    } catch (error) {
      console.error("Error saving data:", error);
    }
  }

  async startScraping(eventId) {
    try {
      this.isRunning = true;
      this.scraper = new SeatsAPIScraper();
  

      let iteration = 1;

      while (this.isRunning) {
        try {
          console.log(
            `\n=== Starting iteration ${iteration} for event ${eventId} ===`
          );

          // Scrape seats data
          const seatsData = await this.scraper.scrape(eventId);

          // // Save the data
          // const result = processAndGroupSeats(seatsData);

          await this.saveSeatsData(eventId, seatsData);

          console.log(`Iteration ${iteration} completed successfully`);

          // Refresh headers after every 6 iterations
          if (iteration % 6 === 0) {
            console.log("Refreshing headers before continuing...");


            await this.scraper.refreshHeaders(eventId);
            console.log("Headers refreshed successfully.");
            iteration = 0;

            // Ensure scraping waits after headers refresh before continuing
            await new Promise((resolve) => setTimeout(resolve, 2000)); // Small delay
          }

          // Wait before next iteration
          await new Promise((resolve) => setTimeout(resolve, 5000));

          iteration++;
        } catch (error) {
          console.error(`Error in iteration ${iteration}:`, error);

          // If an error occurs, wait 10 seconds before retrying
          await new Promise((resolve) => setTimeout(resolve, 10000));
        }
      }
    } catch (error) {
      console.error("Fatal error in scraping:", error);
      await this.stop();
    }
  }

  async stop() {
    this.isRunning = false;
    if (this.scraper) {
      await this.scraper.close();
      this.scraper = null;
    }
  }
}

// Usage example
const manager = new SimpleScrapingManager();

// Start scraping for a specific event
const eventId = "170061560CA4641D"; // Example event ID

try {
  await manager.startScraping(eventId);
} catch (error) {
  console.error("Error in main:", error);
}
