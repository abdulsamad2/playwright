// grouping.js
import _ from "lodash";

// First grouping function - groups by price level and section
function groupByPriceLevelAndSection(facets, offers) {
  // First create a map of offerId to offer details
  const offerMap = new Map(offers.map((offer) => [offer.offerId, offer]));

  // Process and group the data
  const groupedData = {};

  facets.forEach((facet) => {
    const offerId = facet.offers[0];
    const offer = offerMap.get(offerId);

    if (!offer) return;

    const priceLevel = offer.priceLevelSecname;
    const price = offer.listPrice;

    if (!groupedData[priceLevel]) {
      groupedData[priceLevel] = {
        priceLevel,
        basePrice: price,
        serviceCharge:
          offer.charges.find((c) => c.reason === "service")?.amount || 0,
        totalPrice: offer.totalPrice,
        sections: {},
        totalSeats: 0,
      };
    }

    // Group by inventory types
    facet.inventoryTypes.forEach((invType) => {
      if (!groupedData[priceLevel].sections[invType]) {
        groupedData[priceLevel].sections[invType] = {
          count: 0,
          offers: new Set(),
        };
      }

      groupedData[priceLevel].sections[invType].count += facet.count;
      groupedData[priceLevel].sections[invType].offers.add(offerId);
      groupedData[priceLevel].totalSeats += facet.count;
    });
  });

  return groupedData;
}

// Second grouping function - groups by ticket type and price range
function groupByTicketTypeAndPrice(facets, offers) {
  // Create map of offers for efficient lookup
  const offerMap = new Map(offers.map((offer) => [offer.offerId, offer]));

  // Initial grouping by ticket type
  const groupedByType = _.groupBy(facets, (facet) => {
    const offer = offerMap.get(facet.offers[0]);
    return offer?.ticketTypeId || "unknown";
  });

  // Process each group
  const processedGroups = {};

  Object.entries(groupedByType).forEach(([ticketType, items]) => {
    const priceRanges = {};
    let totalSeats = 0;

    items.forEach((facet) => {
      const offer = offerMap.get(facet.offers[0]);
      if (!offer) return;

      const priceRange = Math.floor(offer.listPrice / 50) * 50; // Group in $50 ranges
      const rangeKey = `${priceRange}-${priceRange + 49}`;

      if (!priceRanges[rangeKey]) {
        priceRanges[rangeKey] = {
          minPrice: offer.listPrice,
          maxPrice: offer.listPrice,
          count: 0,
          offers: new Set(),
        };
      }

      priceRanges[rangeKey].minPrice = Math.min(
        priceRanges[rangeKey].minPrice,
        offer.listPrice
      );
      priceRanges[rangeKey].maxPrice = Math.max(
        priceRanges[rangeKey].maxPrice,
        offer.listPrice
      );
      priceRanges[rangeKey].count += facet.count;
      priceRanges[rangeKey].offers.add(offer.offerId);
      totalSeats += facet.count;
    });

    // Get a sample offer for the ticket type
    const sampleOffer = offerMap.get(items[0].offers[0]);

    processedGroups[ticketType] = {
      ticketTypeName: sampleOffer?.name || "Unknown",
      description: sampleOffer?.description || "",
      totalSeats,
      priceRanges,
    };
  });

  return processedGroups;
}

// Function to combine and format both groupings
function processAndGroupSeats(facets, offers) {
  return {
    byPriceLevel: groupByPriceLevelAndSection(facets, offers),
    byTicketType: groupByTicketTypeAndPrice(facets, offers),
  };
}

// Generate full summary
function generateSeatingSummary(data) {
  const {
    facets,
    _embedded: { offer },
  } = data;
  const groupedData = processAndGroupSeats(facets, offer);

  // Generate summary statistics
  const summary = {
    totalSeats: _.sumBy(facets, "count"),
    priceRanges: {
      min: Math.min(...offer.map((o) => o.listPrice)),
      max: Math.max(...offer.map((o) => o.listPrice)),
    },
    ticketTypes: new Set(offer.map((o) => o.ticketTypeId)).size,
    priceLevels: new Set(offer.map((o) => o.priceLevelSecname)).size,
  };

  return {
    summary,
    groupedData,
  };
}

// Export all functions as named exports
export {
  processAndGroupSeats,
  generateSeatingSummary,
  groupByPriceLevelAndSection,
  groupByTicketTypeAndPrice,
};
