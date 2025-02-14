import GenerateNanoPlaces from "./seats.js";

class FacetsProcessor {
  constructor() {
    this.processedData = null;
  }

  processFacetsResponse(response) {
    try {
      // Extract facets and offers from response
      const { facets, _embedded } = response;
      const offers = _embedded?.offer || [];

      // Create a map of offers for quick lookup
      const offersMap = new Map(offers.map((offer) => [offer.offerId, offer]));

      // Process each facet to match the input format expected by GenerateNanoPlaces
      const processedFacets = facets.map((facet) => ({
        places: facet.places || [], // Will be populated from another endpoint
        inventoryTypes: facet.inventoryTypes || [],
        offers: facet.offers || [],
        accessibility: [], // Will be populated from another endpoint if needed
        description: "", // Will be populated from another endpoint if needed
        attributes: [], // Will be populated from another endpoint if needed
        offer: offersMap.get(facet.offers?.[0]), // Attach full offer data
      }));

      // Process through nano places generator
      const nanoPlaces = GenerateNanoPlaces(processedFacets);

      // Add additional data
      const enrichedData = nanoPlaces.map((place) => ({
        ...place,
        offer: offersMap.get(place.offerId),
        count: 1, // Default count, update if needed
        price: offersMap.get(place.offerId)?.listPrice || 0,
      }));

      return {
        raw: response,
        processed: enrichedData,
        stats: {
          totalOffers: offers.length,
          totalFacets: facets.length,
          processedPlaces: enrichedData.length,
        },
      };
    } catch (error) {
      console.error("Error processing facets:", error);
      throw error;
    }
  }

  groupPlacesBySection(processedData) {
    const groupedPlaces = {};

    processedData.forEach((place) => {
      const sectionKey = place.section || "uncategorized";
      if (!groupedPlaces[sectionKey]) {
        groupedPlaces[sectionKey] = [];
      }
      groupedPlaces[sectionKey].push(place);
    });

    return groupedPlaces;
  }

  groupPlacesByOffer(processedData) {
    const groupedPlaces = {};

    processedData.forEach((place) => {
      const offerKey = place.offerId || "uncategorized";
      if (!groupedPlaces[offerKey]) {
        groupedPlaces[offerKey] = [];
      }
      groupedPlaces[offerKey].push(place);
    });

    return groupedPlaces;
  }

  summarizeOffers(response) {
    const { facets, _embedded } = response;

    return facets.map((facet) => {
      const offer = _embedded?.offer?.find(
        (o) => o.offerId === facet.offers?.[0]
      );

      return {
        offerId: facet.offers?.[0],
        type: facet.inventoryTypes?.[0],
        count: facet.count,
        price: {
          listPrice: offer?.listPrice,
          faceValue: offer?.faceValue,
          totalPrice: offer?.totalPrice,
        },
        name: offer?.name,
        sellableQuantities: offer?.sellableQuantities,
      };
    });
  }

  processAndGroup(response) {
    const processed = this.processFacetsResponse(response);

    return {
      bySection: this.groupPlacesBySection(processed.processed),
      byOffer: this.groupPlacesByOffer(processed.processed),
      summary: this.summarizeOffers(response),
      raw: processed.raw,
      stats: processed.stats,
    };
  }
}

export default FacetsProcessor;

// Usage example:
/*
const processor = new FacetsProcessor();
const groupedData = processor.processAndGroup(apiResponse);

console.log('Grouped by section:', groupedData.bySection);
console.log('Grouped by offer:', groupedData.byOffer);
console.log('Summary:', groupedData.summary);
console.log('Stats:', groupedData.stats);
*/
