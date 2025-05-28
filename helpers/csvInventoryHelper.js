import fs from 'fs';
import csv from 'fast-csv';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';

/**
 * Helper function to detect and split concatenated seat numbers
 * @param {string} seatString - Potentially concatenated seat string
 * @returns {Array} - Array of individual seat numbers as strings
 */
export function splitConcatenatedSeats(seatString) {
  if (!seatString) return [];
  
  // Force string conversion and trim
  const seatStr = String(seatString).trim();
  
  // Return empty array for scientific notation
  if (/[eE][+-]?\d+/.test(seatStr)) {
    console.warn(`Rejected scientific notation in seats: ${seatStr}`);
    return [];
  }
  
  // If already properly formatted, return as is
  if (seatStr.includes(',')) {
    return seatStr.split(',').map(s => s.trim());
  }
  
  // Handle concatenated numbers by splitting into 2-digit chunks
  const numbers = [];
  for (let i = 0; i < seatStr.length; i += 2) {
    numbers.push(seatStr.substr(i, 2));
  }
  
  return numbers;
}

/**
 * Validates that seats are consecutive and fixes if needed
 * @param {string} seatsString - String containing seats
 * @returns {Object} - Validation result with fixed seats if needed
 */
export function validateConsecutiveSeats(seatsString) {
  if (!seatsString) {
    return { valid: false, originalSeats: '', fixedSeats: '' };
  }
  
  const seatArray = splitConcatenatedSeats(seatsString);
  
  if (seatArray.length === 0) {
    return { valid: false, originalSeats: seatsString, fixedSeats: '' };
  }
  
  // Convert to numbers and sort
  const seatNumbers = seatArray
    .map(s => parseInt(s))
    .filter(n => !isNaN(n))
    .sort((a, b) => a - b);
  
  if (seatNumbers.length === 0) {
    return { valid: false, originalSeats: seatsString, fixedSeats: '' };
  }
  
  // Check if seats are consecutive
  let isConsecutive = true;
  for (let i = 1; i < seatNumbers.length; i++) {
    if (seatNumbers[i] !== seatNumbers[i - 1] + 1) {
      isConsecutive = false;
      break;
    }
  }
  
  const fixedSeats = seatNumbers.join(',');
  
  return {
    valid: isConsecutive,
    originalSeats: seatsString,
    fixedSeats: fixedSeats,
    seatCount: seatNumbers.length
  };
}

/**
 * Converts seat numbers string to a range format for display
 * @param {string} seatsString - String containing seats separated by commas or concatenated
 * @returns {string} - Range representation (e.g., "1-8")
 */
export function getSeatRange(seatsString) {
  if (!seatsString) return '';
  
  // Handle potential concatenated seats
  const seatArray = splitConcatenatedSeats(seatsString);
  const seats = seatArray.map(seat => parseInt(seat.toString().trim())).filter(s => !isNaN(s));
  
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
  return new Promise((resolve, reject) => {
    const records = [];
    
    fs.createReadStream(filePath)
      .pipe(csv.parse({ headers: true }))
      .on('data', row => records.push(row))
      .on('end', () => resolve(records))
      .on('error', error => {
        console.error(`Error reading CSV file: ${error.message}`);
        reject(error);
      });
  });
}

/**
 * Saves inventory data to CSV file
 * @param {Array} data - Array of inventory items
 * @param {string} filePath - Path to save the CSV file
 * @returns {Promise<boolean>} - Success status
 */
export async function saveInventoryToCSV(data, filePath) {
  return new Promise((resolve, reject) => {
    const ws = fs.createWriteStream(filePath);
    
    // Keep data clean for programmatic reading
    // The CSV library will handle proper quoting automatically
    
    const csvStream = csv.format({ 
      headers: true,
      quoteColumns: true, // Quote columns that contain commas or special characters
      quoteHeaders: true
    });
    
    csvStream.pipe(ws)
      .on('finish', () => {
        console.log(`CSV file saved: ${filePath}`);
        resolve(true);
      })
      .on('error', error => {
        console.error(`Error saving CSV file: ${error.message}`);
        reject(error);
      });
    
    // Write data
    data.forEach(row => {
      csvStream.write(row);
    });
    
    csvStream.end();
  });
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
    if (!validation.valid && validation.fixedSeats) {
      fixedRecord.seats = validation.fixedSeats;
      console.log(`Fixed non-consecutive seats: ${validation.originalSeats} -> ${validation.fixedSeats}`);
    } else if (validation.valid) {
      // Even if valid, ensure proper comma-separated format
      fixedRecord.seats = validation.fixedSeats;
    }
    
    // Update quantity based on the number of seats
    const seatCount = fixedRecord.seats.split(',').filter(Boolean).length;
    
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
 * @returns {Promise<Object>} - Processing statistics
 */
export async function processInventoryCSV(inputFilePath, outputFilePath) {
  const stats = {
    total: 0,
    fixed: 0,
    errors: 0,
    skipped: 0
  };
  
  try {
    // Read the CSV file
    const records = await readInventoryFromCSV(inputFilePath);
    stats.total = records.length;
    
    // Process each record
    const fixedRecords = [];
    
    for (const record of records) {
      try {
        const fixedRecord = validateAndFixInventoryRecord(record);
        
        // Skip null records (single seats)
        if (!fixedRecord) {
          stats.skipped++;
          continue;
        }
        
        // Check if the record was fixed
        if (JSON.stringify(fixedRecord) !== JSON.stringify(record)) {
          stats.fixed++;
        }
        
        fixedRecords.push(fixedRecord);
      } catch (error) {
        stats.errors++;
        console.error(`Error processing record: ${JSON.stringify(record)}`);
        fixedRecords.push(record); // Add the original record if processing fails
      }
    }
    
    // Save the processed data if we have any records
    if (fixedRecords.length > 0) {
      await saveInventoryToCSV(fixedRecords, outputFilePath);
    }
    
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
    if (!validation.valid && validation.fixedSeats) {
      updatedInventory.seats = validation.fixedSeats;
    } else if (validation.valid) {
      // Even if valid, ensure proper comma-separated format
      updatedInventory.seats = validation.fixedSeats;
    }
    
    // Update quantity based on number of seats
    const seatCount = updatedInventory.seats.split(',').filter(Boolean).length;
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
  let seats = data.seats || '';
  
  // Simply ensure seats are stored as strings - no need to split if already formatted
  if (seats) {
    seats = String(seats).trim();
    
    // Only validate/fix if we detect an issue (concatenated numbers or scientific notation)
    if (!seats.includes(',') || seats.includes('E+') || seats.includes('e+')) {
      const validation = validateConsecutiveSeats(seats);
      if (validation.fixedSeats && validation.fixedSeats !== seats) {
        console.log(`🔧 CSV Export Seat Fix: "${seats}" → "${validation.fixedSeats}"`);
        seats = validation.fixedSeats;
      }
    }
  }
  
  const seatArray = seats.split(',').map(s => s.toString().trim()).filter(Boolean);
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

/**
 * Formats seats for CSV output
 * @param {string} seats - Seats string
 * @returns {string} - Formatted seats string
 */
export function formatSeatsForCSV(seats) {
  if (Array.isArray(seats)) {
    return seats.join(',');
  }
  return String(seats || ''); // Force string conversion
}