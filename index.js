import SeatsAPIScraper from "./scraper.js";
import fs from "fs/promises";
import path from "path";
import { AttachRowSection } from "./grouping.js";

function GenerateNanoPlaces(facets) {
  // Transform facets into the required format
  return facets.map((facet) => {
    const offerId = facet.offers?.[0];

    return {
      places: [], // Will be populated from map data
      offerId: offerId,
      accessibility: [], // Empty array by default
      descriptionId: "", // Empty string by default
      attributes: [], // Empty array by default
      selection: facet.inventoryTypes?.[0] || "primary",
      count: facet.count,
      listPriceRange: facet.listPriceRange?.[0] || null,
    };
  });
}

class TicketProcessor {
  constructor() {
    this.isRunning = false;
  }

  processFacetsData(rawData) {
    const { eventId, facets, _embedded } = rawData;

    // Process offers
    const offers =
      _embedded?.offer?.map((offer) => ({
        offerId: offer.offerId,
        name: offer.name,
        protected: offer.protected,
        listPrice: offer.listPrice,
        faceValue: offer.faceValue,
        charges: offer.charges,
        ticketTypeUnsoldQualifier: offer.ticketTypeUnsoldQualifier,
        sellableQuantities: offer.sellableQuantities,
      })) || [];

    // Group offers by ticket type
    const offersByType = offers.reduce((acc, offer) => {
      if (!acc[offer.name]) {
        acc[offer.name] = [];
      }
      acc[offer.name].push(offer);
      return acc;
    }, {});

    // Process facets
    const processedFacets = facets.map((facet) => {
      const offerId = facet.offers?.[0];
      const offer = offers.find((o) => o.offerId === offerId);

      return {
        inventoryType: facet.inventoryTypes?.[0],
        offerId: offerId,
        count: facet.count,
        price: facet.listPriceRange?.[0]?.min,
        ticketType: offer?.name,
        protected: offer?.protected || false,
      };
    });

    // Summary statistics
    const summary = {
      eventId,
      totalInventory: facets.reduce((sum, f) => sum + f.count, 0),
      priceRange: {
        min: Math.min(
          ...facets.map((f) => f.listPriceRange?.[0]?.min || Infinity)
        ),
        max: Math.max(
          ...facets.map((f) => f.listPriceRange?.[0]?.max || -Infinity)
        ),
      },
      ticketTypes: Object.keys(offersByType),
      offerCount: offers.length,
      totalOffers: offers.reduce((sum, o) => sum + (o.protected ? 0 : 1), 0),
    };

    return {
      processedFacets,
      offers,
      summary,
    };
  }

  async processAndSaveData(rawData, outputDir = "./processed_data") {
    try {
      // Create output directory if it doesn't exist
      await fs.mkdir(outputDir, { recursive: true });

      // Process the data
      const processedData = this.processFacetsData(rawData);

      // Generate nano places
      const nanoPlaces = GenerateNanoPlaces(rawData.facets);

      // Prepare data for AttachRowSection
      const attachData = {
        processedData,
        nanoPlaces,
        event: {
          eventId: rawData.eventId,
          eventMappingId: rawData.eventId,
          listCostPercentage: 0,
          inHandDate: new Date().toISOString(),
        },
      };

      // Save raw data
      const timestamp = Date.now();
      const rawFilename = `raw_${rawData.eventId}_${timestamp}.json`;
      await fs.writeFile(
        path.join(outputDir, rawFilename),
        JSON.stringify(rawData, null, 2)
      );

      // Save processed data
      const processedFilename = `processed_${rawData.eventId}_${timestamp}.json`;
      await fs.writeFile(
        path.join(outputDir, processedFilename),
        JSON.stringify(processedData, null, 2)
      );

      return {
        attachData,
        processedData,
        filenames: {
          raw: rawFilename,
          processed: processedFilename,
        },
      };
    } catch (error) {
      console.error("Error processing data:", error);
      throw error;
    }
  }

  formatStatistics(processedData) {
    const { summary } = processedData;

    return {
      eventId: summary.eventId,
      inventory: {
        total: summary.totalInventory,
        byType: summary.ticketTypes.map((type) => ({
          type,
          count: processedData.processedFacets
            .filter((f) => f.ticketType === type)
            .reduce((sum, f) => sum + f.count, 0),
        })),
      },
      pricing: {
        range: summary.priceRange,
        averagePrice:
          processedData.processedFacets.reduce(
            (sum, f) => sum + f.price * f.count,
            0
          ) / summary.totalInventory,
      },
      offers: {
        total: summary.offerCount,
        active: summary.totalOffers,
      },
    };
  }
}

export default TicketProcessor;
