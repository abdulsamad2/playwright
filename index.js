import { Scraper } from "./scraper.js";
import fs from "fs";

let previousTicketCount = 0;

async function main() {
  const scraper = new Scraper();
  let startTime = Date.now();
  let iteration = 1;

  try {
    while (true) {
      const iterationStart = Date.now();
      console.log(`Starting scrape iteration ${iteration}...`);

      const result = await scraper.scrape(
        "https://www.ticketmaster.com/wicked-touring-los-angeles-california-01-29-2025/event/0B0060CADA2A2E73"
      );

      const currentTicketCount = result.tickets?.length || 0;
      const ticketCountDiff = currentTicketCount - previousTicketCount;
      previousTicketCount = currentTicketCount;

      const iterationEnd = Date.now();
      const iterationDuration = (iterationEnd - iterationStart) / 1000;

      const enrichedResult = {
        ...result,
        metadata: {
          lastUpdate: new Date().toLocaleTimeString(),
          iterationNumber: iteration,
          scrapeStartTime: new Date(iterationStart).toISOString(),
          scrapeEndTime: new Date(iterationEnd).toISOString(),
          scrapeDurationSeconds: iterationDuration,
          totalRunningTimeMinutes: (
            (Date.now() - startTime) /
            1000 /
            60
          ).toFixed(2),
          ticketStats: {
            totalTickets: currentTicketCount,
            ticketCountChange: ticketCountDiff,
            previousTicketCount: previousTicketCount,
          },
        },
      };

      console.log("Scraping result:", enrichedResult);
      console.log(`Total tickets found: ${currentTicketCount}`);
      console.log(
        `Ticket count change: ${
          ticketCountDiff > 0 ? "+" + ticketCountDiff : ticketCountDiff
        }`
      );

      fs.writeFileSync("result.json", JSON.stringify(enrichedResult, null, 2));

      console.log(
        `Iteration ${iteration} completed in ${iterationDuration.toFixed(
          2
        )} seconds`
      );
      console.log(
        `Total running time: ${((Date.now() - startTime) / 1000 / 60).toFixed(
          2
        )} minutes`
      );

      await new Promise((resolve) => setTimeout(resolve, 1000));
      iteration++;
    }
  } catch (error) {
    const errorTime = new Date().toISOString();
    console.error(`Scraping failed at ${errorTime}:`, error);

    fs.writeFileSync(
      "error_log.json",
      JSON.stringify(
        {
          error: error.message,
          stack: error.stack,
          timestamp: errorTime,
          totalRunTime:
            ((Date.now() - startTime) / 1000 / 60).toFixed(2) + " minutes",
          lastIteration: iteration,
          lastKnownTicketCount: previousTicketCount,
        },
        null,
        2
      )
    );
  } finally {
    const endTime = Date.now();
    const totalDuration = ((endTime - startTime) / 1000 / 60).toFixed(2);
    console.log(`Scraper closed after ${totalDuration} minutes of running`);
    await scraper.close();
  }
}

main();
