import moment from "moment";

// Function to generate unique 10-digit inventory ID
let inventoryIdCounter = 0;
function generateUniqueInventoryId() {
  // Use timestamp (last 6 digits) + counter (4 digits) for uniqueness
  const timestamp = Date.now();
  const timestampPart = parseInt(timestamp.toString().slice(-6)); // Last 6 digits of timestamp
  
  // Increment counter and reset if it exceeds 4 digits
  inventoryIdCounter = (inventoryIdCounter + 1) % 10000;
  
  // Combine timestamp part (6 digits) + counter (4 digits) = 10 digits
  const uniqueId = timestampPart * 10000 + inventoryIdCounter;
  
  // Ensure it's always 10 digits by padding if necessary
  return parseInt(uniqueId.toString().padStart(10, '1'));
}

// Global Filters
const GLOBAL_FILTERS = {
  inventoryType: ["Primary", "Official Platinum", "Aisle Seating"], // e.g., ['primary', 'resale'] - empty means no filter
  description: [
    "Standard Ticket",
    "GA Lawn",
    "General Admission Standing",
    "Standard Admission",
  ], // e.g., ['obstructed view', 'aisle'] - empty means no filter, strings to check for (case-insensitive)
  accessibility: [
    "GA Lawn",
    "General Admission Standing",
    "Standard Admission",
  ], // e.g., ['wheelchair', 'hearing'] - empty means no filter, strings to check for (case-insensitive)
  excludeWheelchair: true, // Set to true to exclude wheelchair accessible seats (sections containing 'WC')
};
//it will break map into seats
function GetMapSeats(data) {
  let seatArray = [];
  if (
    data &&
    data.pages &&
    data.pages.length > 0 &&
    data.pages[0] &&
    data.pages[0].segments
  ) {
    data.pages[0].segments.map((composit) => {
      if (composit?.segments) {
        composit.segments.map((SECTION) => {
          if (SECTION.segments && SECTION.segments.length > 0)
            SECTION.segments.map((ROW) => {
              ROW.placesNoKeys.map((seat) => {
                seatArray.push({
                  section: SECTION?.name,
                  row: ROW?.name,
                  seat: seat[1],
                  seatId: seat[0],
                });
              });
            });
          else {
            // GeneralAdmission seats - assuming they might be directly under SECTION or have a different structure
            // This is a placeholder and might need adjustment based on the actual GA data structure
            if (SECTION.placesNoKeys && Array.isArray(SECTION.placesNoKeys)) {
              SECTION.placesNoKeys.map((seat) => {
                seatArray.push({
                  section: SECTION?.name,
                  row: "GA", // General Admission typically doesn't have a specific row
                  seat: seat[1], // Assuming seat number is at index 1
                  seatId: seat[0], // Assuming seat ID is at index 0
                });
              });
            } else if (SECTION.name && SECTION.id) { // Fallback if placesNoKeys is not present but section has name and id
                seatArray.push({
                    section: SECTION?.name,
                    row: "GA",
                    seat: "GA", // Placeholder for seat number if not available
                    seatId: SECTION?.id, // Use section id as seatId if specific seatId is not available
                });
            }
            // console.log("Processing General Admission for SECTION:", SECTION);
          }
        });
      }
    });
  }

  return seatArray;
}
function breakArray(arr) {
  let result = [];
  let subarray = [arr[0]];

  for (let i = 0; i < arr.length - 1; i++) {
    if (arr[i] + 1 !== arr[i + 1]) {
      result.push(subarray);
      subarray = [arr[i + 1]];
    } else {
      subarray.push(arr[i + 1]);
    }
  }

  result.push(subarray);
  return result;
}

function CreateConsicutiveSeats(data) {
  const mergedData = [];

  data.forEach((item) => {
    const existingGroup = mergedData.find(
      (group) =>
        group.section === item.section &&
        group.row === item.row &&
        group.offerId === item.offerId &&
        group.seats[group.seats.length - 1] + 1 === item.seats[0]
    );

    if (existingGroup) {
      existingGroup.seats.push(...item.seats);
    } else {
      mergedData.push({
        amount: item.amount,
        lineItemType: item.lineItemType,
        section: item.section,
        row: item.row,
        seats: [...item.seats],
        offerId: item.offerId,
        accessibility: item?.accessibility,
        descriptionId: item?.descriptionId,
        attributes: item?.attributes,
      });
    }
  });

  return mergedData;
}
function getSplitType(arr, offer) {
  var length = arr.length;

  if (
    offer &&
    offer?.ticketTypeUnsoldQualifier &&
    (offer?.ticketTypeUnsoldQualifier == "2PACKHOLD" ||
      offer?.ticketTypeUnsoldQualifier == "222PA1HOLD" ||
      offer?.ticketTypeUnsoldQualifier == "22BOGOHOLD")
  ) {
    if (length === 2) {
      return "2";
    } else if (length === 4) {
      return "2,4";
    } else if (length >= 6) {
      var numbers = Array.from(
        { length: length % 2 == 0 ? length : length - 1 },
        (_, i) => (i % 2 == 0 ? i + 2 : undefined)
      ).filter((x) => x != undefined);
      return numbers.join(",");
    } else return "2";
  } else {
    if (length === 2) {
      return "2";
    } else if (length === 3) {
      return "3";
    } else if (length === 4) {
      return "2,4";
    } else if (length >= 5) {
      var numbers = Array.from({ length: length }, (_, i) => i + 1).filter(
        (x) => x != 1
      );
      return numbers.join(",");
    } else return "1";
  }
}

function CreateInventoryAndLine(data, offer, event, descriptions) {

  let _descriptions = descriptions.find(
    (x) => x.descriptionId == data?.descriptionId
  );
  let allDescriptions = "";
  let isNameAdded = false;

  if (data.attributes.includes("obstructed")) {
    allDescriptions += ", Obstructed View";
    isNameAdded = true;
  }

  if (
    data?.accessibility.includes("sight") ||
    data?.accessibility.includes("hearing")
  ) {
    allDescriptions += ", deaf/hard, blind/low";
    isNameAdded = true;
  }

  if (offer?.name.toLowerCase().includes("limited/obstructed")) {
    allDescriptions += ", Limted/Obstructed View";
    isNameAdded = true;
  } else if (offer?.name.toLowerCase().includes("limited view")) {
    allDescriptions += ", Limited View";
    isNameAdded = true;
  }

  if (isNameAdded == false) {
    if (_descriptions) {
      _descriptions.descriptions.map((x) => {
        if (x.toLowerCase().includes("side")) {
          allDescriptions += ", Side View";
        } else if (x.toLowerCase().includes("behind")) {
          allDescriptions += ", Behind The Stage";
        } else if (x.toLowerCase().includes("rear")) {
          allDescriptions += ", Rear View Seating";
        } else if (x.toLowerCase().includes("partial")) {
          allDescriptions += ", Partial View";
        } else if (x.toLowerCase().includes("limited")) {
          allDescriptions += ", Limited View";
        } else if (x.toLowerCase().includes("obstructed")) {
          allDescriptions += ", obstructed View";
        } else if (
          x.toLowerCase().includes("deaf") ||
          x.toLowerCase().includes("blind")
        ) {
          allDescriptions += ", deaf/hard, blind/low";
        }
      });
    }
  }

  /*
  let totalCost=parseFloat(offer?.charges.reduce((total, item) => total + item.amount, 0)+offer?.faceValue);
  let totalCostWithPercentage=totalCost+(totalCost*(event?.listCostPercentage/100));
  */
  //Get Fee which won't multiply
  let singleExtraCharges = parseFloat(
    parseFloat(
      offer?.charges
        .filter((x) => x?.reason == "order_processing")
        .reduce((total, item) => total + item.amount, 0)
    ) / data?.seats.length
  );
  //let singleExtraCharges=parseFloat(parseFloat(offer?.charges.filter(x=>x?.reason=="order_processing").reduce((total, item) => total + item.amount, 0)));
  //console.log(parseFloat(parseFloat(offer?.charges.filter(x=>x?.reason=="order_processing").reduce((total, item) => total + item.amount, 0))))
  //Remove single fee's
  let repeatExtraCharges = parseFloat(
    offer?.charges
      .filter((x) => x?.reason != "order_processing")
      .reduce((total, item) => total + item.amount, 0)
  );
  //Face Value
  let faceValue = offer?.faceValue;
  let totalCost = singleExtraCharges + repeatExtraCharges + faceValue;

  return {
    inventory: {
      inventoryId: generateUniqueInventoryId(),
      quantity: data?.seats.length,
      section: data?.section,
      hideSeatNumbers: true,
      row: data?.row,
      cost: totalCost,
      seats: data?.seats,
      eventId: event.eventMappingId,
      stockType: "MOBILE_TRANSFER",
      lineType: "PURCHASE",
      seatType: "CONSECUTIVE",
      inHandDate: moment(event?.inHandDate).format("YYYY-MM-DD"), // Format: 2024-12-22
      // "notes": "+stub +geek +tnet +vivid +tevo +pick",
      notes: "-tnow -tmplus -stub",
      tags: "AWS",
      offerId: data?.offerId,
      splitType: "NEVERLEAVEONE",
      publicNotes: "xfer" + allDescriptions,
      listPrice: totalCost,
      customSplit: getSplitType(data?.seats, offer),
      tickets: data?.seats.map((y) => {
        return {
          id: 0,
          seatNumber: y,
          notes: "string",
          cost: totalCost,
          faceValue: totalCost,
          taxedCost: totalCost,
          sellPrice: totalCost,
          stockType: "HARD",
          eventId: 0,
          accountId: 0,
          status: "AVAILABLE",
          auditNote: "string",
        };
      }),
    },
    amount: 0,
    lineItemType: "INVENTORY",
    eventId: event?.eventMappingId,
    dbId: `${data?.seats.join("")}-${data?.row}-${data?.section}-${
      event?.eventMappingId
    }`,
    seats: data?.seats,
    row: data?.row,
    section: data?.section,
  };
}

export const AttachRowSection = (
  data,
  mapData,
  offers,
  event,
  descriptions
) => {
  let allAvailableSeats = GetMapSeats(mapData);
  let mapPlacesIndex = allAvailableSeats.map((x) => x.seatId);
  let returnData = [];
  //get all seats number by seat id
  let customData = data
    .map((x) => {
      if (x.places.length > 0) {
        let placeId = x.places[0];
        if (placeId) {
          let indexToFind = mapPlacesIndex.indexOf(placeId);

          if (indexToFind != -1) {
            let found = allAvailableSeats[indexToFind];
            // let  found= allAvailableSeats.find(y=>y.seatId==placeId);
            if (found) {
              let allPlaces = x?.places
                .map((z) => {
                  let indexOfZ = mapPlacesIndex.indexOf(z);

                  let foundSeatFromMap = allAvailableSeats[indexOfZ];
                  if (foundSeatFromMap && indexOfZ != -1) {
                    return { ...foundSeatFromMap, offerId: x.offerId };
                  }
                  foundSeatFromMap = undefined;

                  //SORT BY (seat) NUMBER
                })
                .filter((y) => y != undefined);

              return {
                section: found.section,
                row: "",
                seats: allPlaces,
                eventId: event?.eventMappingId,
                offerId: x.offerId,
                accessibility: x?.accessibility,
                descriptionId: x?.descriptionId,
                attributes: x?.attributes,
              };
            }
            found = undefined;
          }
        }
      }

      return undefined;
    })
    .filter((x) => x != undefined);

  //it will check if pair has same row as some events are giving pair of different row
  let groupedSeats = [];
  customData.forEach((seatGroup) => {
    const rows = [...new Set(seatGroup.seats.map((seat) => seat.row))];
    rows.forEach((row) => {
      const seatsInRow = seatGroup.seats.filter((seat) => seat.row === row);
      groupedSeats.push({
        section: seatGroup.section,
        seats: seatsInRow,
        eventId: seatGroup.eventId,
        offerId: seatGroup.offerId,
        accessibility: seatGroup.accessibility,
        descriptionId: seatGroup.descriptionId,
        attributes: seatGroup.attributes,
      });
    });
  });

  //add row and get seats in order
  groupedSeats
    .map((x) => {
      if (x?.seats.length > 0) {
        return {
          ...x,
          row: x?.seats[0]?.row,
          seats: x?.seats
            .map((y) => parseInt(y.seat))
            .sort((a, b) => {
              return a - b;
            }),
        };
      } else {
        return undefined;
      }
    })
    .filter((x) => x != undefined)

    //break seats if it is not consicutive ex [1,2,3,6,7] => [1,2,3],[6,7]
    .map((x) => {
      let breakOBJ = breakArray(x.seats);

      if (breakOBJ.length > 1) {
        breakOBJ.map((y) => {
          returnData.push({
            ...x,
            seats: y,
          });
        });
      } else {
        returnData.push(x);
      }
    });

  //it will make consicutive seats ex [2],[4],[3] => [2,3,4]
  returnData = CreateConsicutiveSeats(returnData);

  //attach offer

  return (
    returnData
      .map((x) => {
        let offerGet = offers.find((e) => e.offerId == x.offerId);

        // Check wheelchair exclusion filter first
        if (GLOBAL_FILTERS.excludeWheelchair && x.section && x.section.toUpperCase().includes('WC')) {
          // console.log(`Filtering out wheelchair seat. Section: ${x.section}`);
          return undefined;
        }

        // New Global Filtering Logic: Item must match at least one active global filter category.
        let keepItemBasedOnGlobalFilters = false;
        const inventoryFilterActive = GLOBAL_FILTERS.inventoryType.length > 0;
        const descriptionFilterActive = GLOBAL_FILTERS.description.length > 0;
        const accessibilityFilterActive = GLOBAL_FILTERS.accessibility.length > 0;

        const anyGlobalFilterActive = inventoryFilterActive || descriptionFilterActive || accessibilityFilterActive;

        if (!anyGlobalFilterActive) {
            keepItemBasedOnGlobalFilters = true; // No global filters are active, so item passes this stage
        } else {
            // Check Inventory Type Filter
            if (inventoryFilterActive) {
                if (offerGet && GLOBAL_FILTERS.inventoryType.includes(offerGet.inventoryType?.toLowerCase())) {
                    keepItemBasedOnGlobalFilters = true;
                }
            }

            // Check Description Filter (only if not already marked to keep)
            if (!keepItemBasedOnGlobalFilters && descriptionFilterActive) {
                let descriptionMatched = false;
                const offerNameLower = offerGet?.name?.toLowerCase() || "";
                const offerDescriptionLower = offerGet?.description?.toLowerCase() || "";
                
                if (GLOBAL_FILTERS.description.some(filterTerm => 
                    offerNameLower.includes(filterTerm.toLowerCase()) || 
                    offerDescriptionLower.includes(filterTerm.toLowerCase())
                )) {
                    descriptionMatched = true;
                }

                if (!descriptionMatched && descriptions) {
                    const relevantDescriptionDoc = descriptions.find(d => d.descriptionId === x.descriptionId);
                    if (relevantDescriptionDoc && relevantDescriptionDoc.descriptions) {
                        const descriptionsTextLower = relevantDescriptionDoc.descriptions.join(' ').toLowerCase();
                        if (GLOBAL_FILTERS.description.some(filterTerm => descriptionsTextLower.includes(filterTerm.toLowerCase()))) {
                            descriptionMatched = true;
                        }
                    }
                }
                
                if (!descriptionMatched && x.attributes && x.attributes.length > 0){
                    const attributesTextLower = x.attributes.join(' ').toLowerCase();
                    if (GLOBAL_FILTERS.description.some(filterTerm => attributesTextLower.includes(filterTerm.toLowerCase()))) {
                        descriptionMatched = true;
                    }
                }

                if (descriptionMatched) {
                    keepItemBasedOnGlobalFilters = true;
                }
            }

            // Check Accessibility Filter (only if not already marked to keep)
            if (!keepItemBasedOnGlobalFilters && accessibilityFilterActive) {
                if (x.accessibility) {
                    const accessibilityLower = x.accessibility.toLowerCase();
                    if (GLOBAL_FILTERS.accessibility.some(filterTerm => accessibilityLower.includes(filterTerm.toLowerCase()))) {
                        keepItemBasedOnGlobalFilters = true;
                    }
                }
            }
        }

        if (!keepItemBasedOnGlobalFilters) {
            // console.log(`Filtering out by global filters combination. Item: ${x.section}-${x.row}-${x.seats}, Offer: ${offerGet?.name}`);
            return undefined;
        }

        // Original offer filtering logic
        if (offerGet) {
          if (offerGet.name == "Special Offers") {
            return undefined;
          } else if (offerGet.name == "Summer's Live 4 Pack") {
            return undefined;
          } else if (offerGet.name == "Me + 3 4-Pack Offer") {
            return undefined;
          } else if (offerGet?.protected == true) {
            return undefined;
          } else {
            return CreateInventoryAndLine(x, offerGet, event, descriptions);
          }
        } else {
          return undefined;
        }
      })
      .filter((x) => x != undefined)
      .filter((obj, index, self) => {
        // Convert dbId value to string to compare
        var dbId = obj.dbId.toString();

        // Check if the current dbId is the first occurrence in the array
        return index === self.findIndex((o) => o.dbId.toString() === dbId);
      })
      .filter((x) => x.inventory.quantity > 1)

      //remove duplicate
      .filter((obj, index, self) => {
        // Check if any other object has the same row and section
        const hasDuplicate = self.some((otherObj, otherIndex) => {
          return (
            index !== otherIndex && // Exclude the current object from comparison
            obj.row === otherObj.row &&
            obj.section === otherObj.section &&
            obj.seats.some((seat) => otherObj.seats.includes(seat))
          );
        });

        return !hasDuplicate || index === 0; // Keep the first object or objects without duplicates
      })
  );
};
