import fs from 'fs';
import { parse } from 'csv-parse/sync';
import { stringify } from 'csv-stringify/sync';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';

/**
 * Validates whether seats are consecutive
 * @param {string} seatsString - String containing seats separated by commas
 * @returns {Object} - Result of validation containing status and fixed seats if needed
 */
export function validateConsecutiveSeats(seatsString) {
  // Parse the seat numbers
  const seats = seatsString.split(',').map(seat => seat.trim()).map(Number);
  
  // Check if all elements are numeric
  if (seats.some(isNaN)) {
    return {
      valid: false,
      message: 'All seats must be numeric values',
      originalSeats: seatsString,
      fixedSeats: null
    };
  }
  
  // Sort the seats numerically
  const sortedSeats = [...seats].sort((a, b) => a - b);
  
  // Check if seats are consecutive
  let isConsecutive = true;
  for (let i = 1; i < sortedSeats.length; i++) {
    if (sortedSeats[i] !== sortedSeats[i-1] + 1) {
      isConsecutive = false;
      break;
    }
  }
  
  // For non-consecutive seats, generate a consecutive sequence
  let fixedSeats = seatsString;
  if (!isConsecutive) {
    // Create a consecutive sequence from min to max
    const min = sortedSeats[0];
    const max = sortedSeats[sortedSeats.length - 1];
    const consecutiveSeats = Array.from({ length: max - min + 1 }, (_, i) => min + i);
    fixedSeats = consecutiveSeats.join(',');
  }
  
  return {
    valid: isConsecutive,
    message: isConsecutive ? 'Seats are consecutive' : 'Seats are not consecutive',
    originalSeats: seatsString,
    fixedSeats: fixedSeats
  };
}

/**
 * Converts seat numbers string to a range format for display
 * @param {string} seatsString - String containing seats separated by commas
 * @returns {string} - Range representation (e.g., "1-8")
 */
export function getSeatRange(seatsString) {
  const seats = seatsString.split(',').map(seat => seat.trim()).map(Number);
  if (seats.length === 0) return '';
  
  const sortedSeats = [...seats].sort((a, b) => a - b);
  return `${sortedSeats[0]}-${sortedSeats[sortedSeats.length - 1]}`;
}

/**
 * Reads inventory data from CSV file
 * @param {string} filePath - Path to the CSV file
 * @returns {Array} - Array of parsed inventory items
 */
export function readInventoryFromCSV(filePath) {
  try {
    const content = fs.readFileSync(filePath, 'utf8');
    const records = parse(content, {
      columns: true,
      skip_empty_lines: true
    });
    return records;
  } catch (error) {
    console.error(`Error reading CSV file: ${error.message}`);
    return [];
  }
}

/**
 * Saves inventory data to CSV file
 * @param {Array} data - Array of inventory items
 * @param {string} filePath - Path to save the CSV file
 * @returns {boolean} - Success status
 */
export function saveInventoryToCSV(data, filePath) {
  try {
    const headers = Object.keys(data[0]);
    const csv = stringify(data, { header: true, columns: headers });
    fs.writeFileSync(filePath, csv);
    return true;
  } catch (error) {
    console.error(`Error saving CSV file: ${error.message}`);
    return false;
  }
}

/**
 * Validates and fixes an inventory record
 * @param {Object} record - The inventory record
 * @returns {Object|null} - The validated and fixed record, or null if invalid (e.g., single seat)
 */
export function validateAndFixInventoryRecord(record) {
  // Make a copy of the record to avoid modifying the original
  const fixedRecord = { ...record };
  
  // Validate seats are consecutive and fix if needed
  if (fixedRecord.seats) {
    const validation = validateConsecutiveSeats(fixedRecord.seats);
    if (!validation.valid) {
      fixedRecord.seats = validation.fixedSeats;
      console.log(`Fixed non-consecutive seats: ${validation.originalSeats} -> ${validation.fixedSeats}`);
    }
    
    // Update quantity based on the number of seats
    const seatCount = fixedRecord.seats.split(',').length;
    
    // Skip records with only one seat
    if (seatCount < 2) {
      console.log(`Skipping single-seat inventory record: ${fixedRecord.inventory_id}`);
      return null;
    }
    
    fixedRecord.quantity = seatCount.toString();
    
    // Handle custom_split based on seat configuration
    // Default pattern for CUSTOM split type (half, half)
    const halfPoint = Math.ceil(seatCount / 2);
    fixedRecord.custom_split = `${halfPoint},${seatCount}`;
    fixedRecord.split_type = 'CUSTOM'; // Ensure split type is set to CUSTOM for multi-seat records
  } else if (!fixedRecord.quantity || parseInt(fixedRecord.quantity) < 2) {
    // Skip records with no seats or quantity less than 2
    console.log(`Skipping inventory record with insufficient quantity: ${fixedRecord.inventory_id}`);
    return null;
  }
  
  return fixedRecord;
}

/**
 * Processes a complete inventory CSV and validates all records
 * @param {string} inputFilePath - Path to the input CSV file
 * @param {string} outputFilePath - Path to save the validated CSV file
 * @returns {Object} - Processing statistics
 */
export function processInventoryCSV(inputFilePath, outputFilePath) {
  const stats = {
    total: 0,
    fixed: 0,
    errors: 0
  };
  
  try {
    // Read the CSV file
    const records = readInventoryFromCSV(inputFilePath);
    stats.total = records.length;
    
    // Process each record
    const fixedRecords = records.map(record => {
      try {
        const fixedRecord = validateAndFixInventoryRecord(record);
        
        // Check if the record was fixed
        if (JSON.stringify(fixedRecord) !== JSON.stringify(record)) {
          stats.fixed++;
        }
        
        return fixedRecord;
      } catch (error) {
        stats.errors++;
        console.error(`Error processing record: ${JSON.stringify(record)}`);
        return record; // Return the original record if processing fails
      }
    });
    
    // Save the processed data
    saveInventoryToCSV(fixedRecords, outputFilePath);
    
    return stats;
  } catch (error) {
    console.error(`Error processing inventory CSV: ${error.message}`);
    return stats;
  }
}

/**
 * Updates a record with new content while preserving all other fields
 * @param {Object} inventory - The inventory record to update
 * @param {Object} updates - Fields to update in the record
 * @returns {Object} - The updated record
 */
export function updateInventoryRecord(inventory, updates) {
  const updatedInventory = { ...inventory };
  
  // Apply updates
  Object.keys(updates).forEach(key => {
    updatedInventory[key] = updates[key];
  });
  
  // If seats are updated, validate and fix related fields
  if (updates.seats) {
    // Validate seats are consecutive
    const validation = validateConsecutiveSeats(updatedInventory.seats);
    if (!validation.valid) {
      updatedInventory.seats = validation.fixedSeats;
    }
    
    // Update quantity based on number of seats
    const seatCount = updatedInventory.seats.split(',').length;
    updatedInventory.quantity = seatCount.toString();
    
    // Update custom_split if split_type is CUSTOM
    if (updatedInventory.split_type === 'CUSTOM' && seatCount > 1) {
      const halfPoint = Math.ceil(seatCount / 2);
      updatedInventory.custom_split = `${halfPoint},${seatCount}`;
    }
  }
  
  return updatedInventory;
}

/**
 * Converts an inventory object to the specific required format
 * @param {Object} data - The inventory data
 * @returns {Object} - Formatted inventory record
 */
export function formatInventoryForExport(data) {
  // Get seat count and format seats
  const uuid = uuidv4();
  const seats = data.seats || '';
  const seatArray = seats.split(',').map(s => s.trim()).filter(Boolean);
  const quantity = seatArray.length || (data.quantity ? parseInt(data.quantity) : 0);
  
  // Calculate split information
  let customSplit = data.custom_split || "NEVERLEAVEONE";
  let splitType = data.split_type || "NEVERLEAVEONE";
  
  // Generate custom split if not provided but split type is CUSTOM
  if (splitType === 'CUSTOM' && !customSplit && quantity > 1) {
    const halfPoint = Math.ceil(quantity/2);
    customSplit = `${halfPoint},${quantity}`;
  }
  
  // Format date as YYYY-MM-DD if it's a Date object
  const inHandDate = data.in_hand_date instanceof Date ? 
    data.in_hand_date.toISOString().split('T')[0] : 
    data.in_hand_date || '';
  
  // Ensure both event_id and mapping_id are properly populated
  const event_id = data.mapping_id || "";
  const mapping_id = data.mapping_id || data.event_id || "";
  
  // If both are missing, log a warning
  if (!event_id || !mapping_id) {
    console.warn(`WARNING: Missing event_id or mapping_id for record with section=${data.section}, row=${data.row}`);
  }
  
  // Use original event name if it was preserved, otherwise use the event_name
  const eventName = data.original_event_name || data.event_name || "";
  
  // Format the exported data in the required structure
  return {
    inventory_id: data.inventory_id || uuid,
    event_name: eventName,
    venue_name: data.venue_name || "",
    event_date: data.event_date || "",
    event_id: event_id,
    quantity: quantity.toString(),
    section: data.section || "",
    row: data.row || "",
    seats: seats,
    barcodes: "",
    internal_notes: data.internal_notes || "",
    public_notes: data.public_notes || "",
    tags: data.tags || "",
    list_price: data.list_price || "",
    face_price: data.face_price || "",
    taxed_cost: data.taxed_cost || "",
    cost: data.cost || "",
    hide_seats: data.hide_seats || "Y",
    in_hand: data.in_hand || "N",
    in_hand_date: inHandDate,
    instant_transfer: data.instant_transfer || "N",
    files_available: "Y",
    split_type: "NEVERLEAVEONE",
    custom_split: "",
    stock_type: data.stock_type || "MOBILE_TRANSFER",
    zone: data.zone || "N",
    shown_quantity: "",
    passthrough: data.passthrough || "",
 
  };
}