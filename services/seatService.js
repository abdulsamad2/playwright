import _ from "lodash";
import { Event, SeatGroup, ErrorLog } from "../models/index.js";

class SeatService {
  processSeats(tickets) {
    if (!tickets || tickets.length === 0) {
      console.log("No tickets to process");
      return [];
    }

    console.log(`Processing ${tickets.length} tickets`);

    // First, group by section, price, and ticket type
    const groupedTickets = _.groupBy(tickets, (ticket) => {
      const [section] = ticket.section.split(" • ");
      return `${section}|${ticket.price}|${ticket.ticketType}`;
    });

    console.log(`Created ${Object.keys(groupedTickets).length} initial groups`);

    const processedGroups = [];

    for (const [key, ticketsInGroup] of Object.entries(groupedTickets)) {
      const [section, price, ticketType] = key.split("|");
      console.log(
        `Processing group: ${section} - ${price} - ${ticketType} (${ticketsInGroup.length} tickets)`
      );

      // Sort tickets by row and seat number
      console.log("Sorting tickets by row and seat number");
      const sortedTickets = _.sortBy(ticketsInGroup, (ticket) => {
        const [, rowPart] = ticket.section.split(" • ");
        const row = rowPart.replace("Row ", "");
        const seatNum = parseInt(ticket.seatNumber) || 0;
        const rowValue = this.getRowValue(row);
        return rowValue * 1000 + seatNum;
      });

      // Group by row first
      const rowGroups = _.groupBy(sortedTickets, (ticket) => {
        const rowPart = ticket.section.split(" • ")[1];
        return rowPart.replace("Row ", "").trim();
      });

      console.log(
        `Found ${Object.keys(rowGroups).length} rows in section ${section}`
      );

      // Process each row
      for (const [row, rowTickets] of Object.entries(rowGroups)) {
        console.log(`Processing row ${row} with ${rowTickets.length} tickets`);

        // Sort tickets in row by seat number
        const sortedRowTickets = _.sortBy(
          rowTickets,
          (ticket) => parseInt(ticket.seatNumber) || 0
        );

        let currentGroup = [];
        const groups = [];

        // Group consecutive seats within the row
        for (let i = 0; i < sortedRowTickets.length; i++) {
          const currentTicket = sortedRowTickets[i];
          const nextTicket = sortedRowTickets[i + 1];
          const currentSeatNum = parseInt(currentTicket.seatNumber) || 0;
          const nextSeatNum = nextTicket
            ? parseInt(nextTicket.seatNumber) || 0
            : 0;

          currentGroup.push(currentTicket);

          const isLastTicket = i === sortedRowTickets.length - 1;
          const isConsecutive =
            nextTicket && nextSeatNum - currentSeatNum === 1;

          if (isLastTicket || !isConsecutive) {
            // Always create a group regardless of size (removed minimum size check)
            groups.push([...currentGroup]);
            currentGroup = [];
          }
        }

        console.log(`Created ${groups.length} groups in row ${row}`);

        // Process seat groups within the row
        groups.forEach((group) => {
          const startSeat = parseInt(group[0].seatNumber) || 0;
          const endSeat = parseInt(group[group.length - 1].seatNumber) || 0;
          const groupPrice = parseFloat(price.replace(/[^0-9.]/g, "")) || 0;

          const seatGroup = {
            section: section.trim(),
            row: row.trim(),
            startRow: row.trim(),
            endRow: row.trim(),
            startSeat,
            endSeat,
            quantity: group.length,
            price: groupPrice,
            ticketType: ticketType.trim(),
            rowList: [row.trim()],
            seatList: group.map((t) => t.seatNumber),
          };

          console.log(
            `Adding group: ${seatGroup.section} Row ${seatGroup.row}, Seats ${seatGroup.startSeat}-${seatGroup.endSeat} (${seatGroup.quantity} tickets)`
          );
          processedGroups.push(seatGroup);
        });
      }

      // Now process consecutive rows with same seat patterns
      const mergedGroups = this.mergeConsecutiveRows(processedGroups);
      processedGroups.length = 0;
      processedGroups.push(...mergedGroups);
    }

    return processedGroups;
  }

  mergeConsecutiveRows(groups) {
    const sortedGroups = _.sortBy(groups, [
      "section",
      (g) => this.getRowValue(g.row),
      "startSeat",
    ]);

    const mergedGroups = [];
    let currentMerge = null;

    for (const group of sortedGroups) {
      if (!currentMerge) {
        currentMerge = { ...group };
        continue;
      }

      const canMerge =
        currentMerge.section === group.section &&
        currentMerge.startSeat === group.startSeat &&
        currentMerge.endSeat === group.endSeat &&
        currentMerge.price === group.price &&
        currentMerge.ticketType === group.ticketType &&
        this.areRowsConsecutive(currentMerge.endRow, group.row);

      if (canMerge) {
        currentMerge.endRow = group.row;
        currentMerge.quantity += group.quantity;
        currentMerge.rowList = [...currentMerge.rowList, group.row];
      } else {
        mergedGroups.push(currentMerge);
        currentMerge = { ...group };
      }
    }

    if (currentMerge) {
      mergedGroups.push(currentMerge);
    }

    return mergedGroups;
  }

  getRowValue(row) {
    if (/^[A-Z]+$/.test(row)) {
      return row.split("").reduce((acc, char, i) => {
        return (
          acc +
          (char.charCodeAt(0) - "A".charCodeAt(0) + 26) *
            Math.pow(26, row.length - i - 1)
        );
      }, 0);
    }
    return parseInt(row);
  }

  areRowsConsecutive(row1, row2) {
    const val1 = this.getRowValue(row1);
    const val2 = this.getRowValue(row2);
    return val2 - val1 === 1;
  }

  parseDateString(dateString) {
    try {
      if (!dateString) {
        throw new Error("Date string is empty or undefined");
      }

      const parts = dateString.split(" • ");
      if (parts.length !== 3) {
        throw new Error(`Invalid date string format: ${dateString}`);
      }

      const [dayOfWeek, date, time] = parts;
      const dateParts = date.trim().split(" ");
      if (dateParts.length !== 3) {
        throw new Error(`Invalid date format: ${date}`);
      }

      const [monthStr, dayStr, year] = dateParts;
      const dayNum = parseInt(dayStr.replace(/[^0-9]/g, ""));

      const [timeStr, period] = time.trim().split(" ");
      if (!timeStr || !period) {
        throw new Error(`Invalid time format: ${time}`);
      }

      const [hours, minutes] = timeStr.split(":");
      let hour = parseInt(hours);

      // Validate hour and period
      if (isNaN(hour) || hour < 1 || hour > 12) {
        throw new Error(`Invalid hour: ${hours}`);
      }

      // Convert to 24-hour format
      if (period.toLowerCase() === "pm") {
        hour = hour === 12 ? 12 : hour + 12;
      } else if (period.toLowerCase() === "am") {
        hour = hour === 12 ? 0 : hour;
      }

      // Get month number (1-12)
      const monthNames = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
      ];
      const monthIndex = monthNames.findIndex((m) =>
        m.toLowerCase().startsWith(monthStr.toLowerCase())
      );

      if (monthIndex === -1) {
        throw new Error(`Invalid month: ${monthStr}`);
      }

      const monthNum = monthIndex + 1;

      // Create ISO date string
      const formattedDate = new Date(
        Date.UTC(
          parseInt(year),
          monthNum - 1,
          dayNum,
          hour,
          parseInt(minutes),
          0
        )
      );

      // Validate the resulting date
      if (isNaN(formattedDate.getTime())) {
        throw new Error("Invalid date result");
      }

      return formattedDate;
    } catch (error) {
      console.error("Error parsing date string:", dateString, error);
      throw new Error(`Date parsing failed: ${error.message}`);
    }
  }

  async saveToDatabase(eventInfo, processedSeats, metadata, url) {
    try {
      // Validate inputs
      if (!eventInfo || !processedSeats || !metadata || !url) {
        throw new Error("Missing required parameters for database save");
      }

      console.log("Processing event:", eventInfo.title);
      console.log("Number of seat groups to process:", processedSeats.length);

      let eventDate;
      try {
        eventDate = this.parseDateString(eventInfo.dateTime);
      } catch (error) {
        console.error(`Could not parse date string: ${eventInfo.dateTime}`);
        await ErrorLog.create({
          eventUrl: url,
          errorType: "PARSE_ERROR",
          message: `Could not parse date string: ${eventInfo.dateTime}`,
          stack: error.stack,
          metadata: {
            iteration: metadata.iterationNumber,
            timestamp: new Date(),
            additionalInfo: { eventInfo },
          },
        });
        eventDate = new Date();
      }

      // Save or update event
      let event = await Event.findOneAndUpdate(
        { url },
        {
          title: eventInfo.title,
          dateTime: eventDate,
          url,
          lastUpdated: new Date(),
          metadata: {
            ...metadata,
            scrapeStartTime: new Date(metadata.scrapeStartTime),
            scrapeEndTime: new Date(metadata.scrapeEndTime),
          },
        },
        { upsert: true, new: true }
      );

      if (!event || !event._id) {
        throw new Error("Failed to create or update event");
      }

      // Delete existing seat groups for this event
      console.log("Deleting existing seat groups for event:", event._id);
      await SeatGroup.deleteMany({ eventId: event._id });

      // Process and validate seat groups
      const seatGroups = processedSeats
        .map((group) => {
          // Ensure all required fields are present
          if (!group.section || !group.startRow || !group.endRow) {
            console.warn("Invalid seat group:", group);
            return null;
          }

          // Clean and validate data
          const startSeat = parseInt(group.startSeat) || 1;
          const endSeat = parseInt(group.endSeat) || group.quantity;
          const price = parseFloat(group.price) || 0;

          return {
            eventId: event._id,
            section: group.section.trim(),
            row:
              group.startRow === group.endRow
                ? group.startRow.trim()
                : `${group.startRow.trim()}-${group.endRow.trim()}`,
            startSeat,
            endSeat,
            quantity: group.quantity,
            price,
            ticketType: (group.ticketType || "Standard").trim(),
            rowList: Array.isArray(group.rowList)
              ? group.rowList.join(",")
              : group.startRow,
            created: new Date(),
            lastUpdated: new Date(),
          };
        })
        .filter((group) => group !== null); // Remove any invalid groups

      console.log("Prepared seat groups for saving:", seatGroups.length);

      // Save seat groups in batches to handle large datasets
      if (seatGroups.length > 0) {
        const batchSize = 100;
        for (let i = 0; i < seatGroups.length; i += batchSize) {
          const batch = seatGroups.slice(i, i + batchSize);
          await SeatGroup.insertMany(batch, { ordered: false });
          console.log(
            `Saved batch ${i / batchSize + 1} of ${Math.ceil(
              seatGroups.length / batchSize
            )}`
          );
        }
      }

      // Verify save
      const savedCount = await SeatGroup.countDocuments({ eventId: event._id });
      console.log("Total seat groups saved:", savedCount);

      if (savedCount === 0 && processedSeats.length > 0) {
        throw new Error(
          "No seat groups were saved despite having processed seats"
        );
      }

      return {
        eventId: event._id,
        seatGroupsCount: savedCount,
      };
    } catch (error) {
      console.error("Error saving to database:", error);
      await ErrorLog.create({
        eventUrl: url,
        errorType: "DATABASE_ERROR",
        message: error.message,
        stack: error.stack,
        metadata: {
          iteration: metadata.iterationNumber,
          timestamp: new Date(),
          additionalInfo: {
            eventInfo,
            processedSeatsCount: processedSeats?.length,
            firstProcessedSeat: processedSeats?.[0],
          },
        },
      });
      throw error;
    }
  }
}

export default new SeatService();
