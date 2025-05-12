import moment from "moment";
import fs from "fs";
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
            //GernalAdmission seats
            //console.log(SECTION,"sec")
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

  if (offer?.name?.toLowerCase().includes("limited/obstructed")) {
    allDescriptions += ", Limted/Obstructed View";
    isNameAdded = true;
  } else if (offer?.name?.toLowerCase().includes("limited view")) {
    allDescriptions += ", Limited View";
    isNameAdded = true;
  }

  if (isNameAdded == false) {
    if (_descriptions) {
      _descriptions.descriptions.map((x) => {
        if (x?.toLowerCase().includes("side")) {
          allDescriptions += ", Side View";
        } else if (x?.toLowerCase().includes("behind")) {
          allDescriptions += ", Behind The Stage";
        } else if (x?.toLowerCase().includes("rear")) {
          allDescriptions += ", Rear View Seating";
        } else if (x?.toLowerCase().includes("partial")) {
          allDescriptions += ", Partial View";
        } else if (x?.toLowerCase().includes("limited")) {
          allDescriptions += ", Limited View";
        } else if (x?.toLowerCase().includes("obstructed")) {
          allDescriptions += ", obstructed View";
        } else if (
          x?.toLowerCase().includes("deaf") ||
          x?.toLowerCase().includes("blind")
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
  fs.writeFileSync("data.json", JSON.stringify(offer, null, 2));
  let repeatExtraCharges = parseFloat(
    offer?.charges
      .filter((x) => x?.reason != "order_processing")
      .reduce((total, item) => total + item.amount, 0)
  );
  //Face Value
  let faceValue = offer?.faceValue;
  let totalCost = singleExtraCharges + repeatExtraCharges + faceValue;
  let totalCostWithPercentage =
    totalCost + totalCost * (event?.listCostPercentage / 100);
  return {
    inventory: {
      quantity: data?.seats.length,
      section: data?.section,
      hideSeatNumbers: true,
      row: data?.row,
      cost: totalCost * data?.seats.length,
      seats: data?.seats,
      eventId: event.eventMappingId,
      stockType: "MOBILE_TRANSFER",
      lineType: "PURCHASE",
      seatType: "CONSECUTIVE",
      inHandDate: moment(event?.inHandDate).format("YYYY-MM-DDTHH:mm:ss"), //"2023-06-09T16:48:09.99",
      // "notes": "+stub +geek +tnet +vivid +tevo +pick",
      notes: "-tnow -tmplus -stub",
      tags: "AWS",
      inventoryId: 0,
      offerId: data?.offerId,
      splitType: "CUSTOM",
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

        if (offerGet) {
          if (offerGet.name == "Special Offers") {
            return undefined;
          } else if (offerGet.name == "Summer's Live 4 Pack") {
            return undefined;
          } else if (offerGet.name == "Me + 3 4-Pack Offer") {
            return undefined;
          } else if (offerGet?.protected == true) {
            return undefined;
          } else if (offerGet.inventoryType == "resale") {
            return undefined;
          } else if (offerGet.name !=="Standard Admission") {
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
