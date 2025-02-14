import SeatsAPIScraper from "./scraper.js";
import fs from "fs/promises";
import path from "path";
import {
  AttachRowSection,
  GetMapSeats,
  CreateConsicutiveSeats,
  breakArray,
  CreateInventoryAndLine,
} from "./seatBatch.js";

class FormattedScrapingManager {
  constructor() {
    this.scraper = null;
    this.isRunning = false;
  }

  async processAndFormatData(seatsData, event) {
    try {
      console.log("Processing data with map data and facets...");

      // Extract the map data from seatsData.mapData
      const mapData = seatsData.mapData;

      // Get available seats from map
      const allAvailableSeats = GetMapSeats(mapData);
      console.log("Available seats from map:", allAvailableSeats?.length || 0);

      // Process facets data
      const facetsWithPlaces = seatsData.facets.map((facet) => {
        return {
          ...facet,
          places: [], // This will be populated with actual seat IDs
          offerId: facet.offers[0],
          accessibility: [],
          descriptionId: "",
          attributes: [],
          selection: facet.inventoryTypes[0],
        };
      });

      // Get the offers
      const offers = seatsData._embedded?.offer || [];
      const descriptions = seatsData._embedded?.description || [];

      // Process using AttachRowSection
      const formattedResult = AttachRowSection(
        facetsWithPlaces,
        mapData,
        offers,
        {
          eventId: event.eventId,
          eventMappingId: event.eventId,
          listCostPercentage: 0,
          inHandDate: new Date().toISOString(),
        },
        descriptions
      );

      console.log("Formatted Result:", {
        totalResults: formattedResult?.length || 0,
        totalOffers: offers.length,
        mapDataAvailable: !!mapData,
      });

      return {
        formatted: formattedResult,
        summary: {
          eventId: event.eventId,
          timestamp: new Date().toISOString(),
          totalInventory: formattedResult?.length || 0,
          offers: offers.length,
          ticketTypes: [...new Set(offers.map((o) => o.name))],
          priceRange: {
            min: Math.min(...offers.map((o) => o.faceValue)),
            max: Math.max(...offers.map((o) => o.faceValue)),
          },
        },
      };
    } catch (error) {
      console.error("Error processing data:", error);
      throw error;
    }
  }

  async saveSeatsData(event, seatsData) {
    try {
      // Create directory if it doesn't exist
      const dataDir = "./seats_data";
      await fs.mkdir(dataDir, { recursive: true });

      const timestamp = Date.now();
      const rawFileName = `raw_seats_${event.eventId}_${timestamp}.json`;
      const formattedFileName = `formatted_seats_${event.eventId}_${timestamp}.json`;

      // Save raw data
      await fs.writeFile(
        path.join(dataDir, rawFileName),
        JSON.stringify(seatsData, null, 2)
      );

      // Process and save formatted data
      const formattedData = await this.processAndFormatData(seatsData, event);
      await fs.writeFile(
        path.join(dataDir, formattedFileName),
        JSON.stringify(formattedData, null, 2)
      );

      console.log(`Raw data saved to ${rawFileName}`);
      console.log(`Formatted data saved to ${formattedFileName}`);

      return formattedData;
    } catch (error) {
      console.error("Error saving data:", error);
      throw error;
    }
  }

  async startScraping(eventId) {
    try {
      this.isRunning = true;
      this.scraper = new SeatsAPIScraper();

      let iteration = 1;
      const event = {
        eventId,
        eventMappingId: eventId,
        listCostPercentage: 0,
        inHandDate: new Date().toISOString(),
      };

      while (this.isRunning) {
        try {
          console.log(
            `\n=== Starting iteration ${iteration} for event ${eventId} ===`
          );

          // Get both map and facets data
          const seatsData = await this.scraper.scrape(eventId);
          console.log("Got seats data, processing...");

          // Save and process data
          const processedData = await this.saveSeatsData(event, seatsData);

          // Log summary
          console.log("\nIteration Summary:");
          console.log(`Event ID: ${processedData.summary.eventId}`);
          console.log(
            `Total Inventory: ${processedData.summary.totalInventory}`
          );
          console.log(`Available Offers: ${processedData.summary.offers}`);
          console.log(
            `Price Range: $${processedData.summary.priceRange.min} - $${processedData.summary.priceRange.max}`
          );
          console.log(
            `Ticket Types: ${processedData.summary.ticketTypes.join(", ")}`
          );
          console.log(`Timestamp: ${processedData.summary.timestamp}`);

          // Refresh headers after every 6 iterations
          if (iteration % 6 === 0) {
            console.log("Refreshing headers...");
            await this.scraper.refreshHeaders(eventId);
            console.log("Headers refreshed successfully.");
            iteration = 0;
            await new Promise((resolve) => setTimeout(resolve, 2000));
          }

          // Wait before next iteration
          await new Promise((resolve) => setTimeout(resolve, 5000));

          iteration++;
        } catch (error) {
          console.error(`Error in iteration ${iteration}:`, error);
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

  const manager = new FormattedScrapingManager();
    const eventId = "170061560CA4641D";  // Your event ID
    
    try {
        await manager.startScraping(eventId);
    } catch (error) {
        console.error('Error:', error);
    }

export default FormattedScrapingManager;
