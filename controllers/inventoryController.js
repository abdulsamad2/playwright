import fs from 'fs';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';
import { 
  validateConsecutiveSeats, 
  saveInventoryToCSV, 
  readInventoryFromCSV,
  validateAndFixInventoryRecord,
  formatInventoryForExport
} from '../helpers/csvInventoryHelper.js';

// Store path for inventory data
const DATA_DIR = path.join(process.cwd(), 'data');
const COMBINED_EVENTS_FILE = path.join(DATA_DIR, 'all_events_combined.csv');
const EVENTS_DATA_FILE = path.join(DATA_DIR, 'events_data.json'); // Use JSON for faster processing

// Create data directory if it doesn't exist
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// Helper function to generate composite key for data matching
const generateCompositeKey = (record) => {
  // Composite key: event_title + venue + datetime + event_code + section + row
  const key = `${record.event_name || ''}_${record.venue_name || ''}_${record.event_date || ''}_${record.event_id || ''}_${record.section || ''}_${record.row || ''}`;
  return key.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase(); // Normalize key
};

// Helper function to check if two records are identical (excluding inventory_id)
const areRecordsIdentical = (record1, record2) => {
  const excludeFields = ['inventory_id'];
  const keys1 = Object.keys(record1).filter(k => !excludeFields.includes(k));
  const keys2 = Object.keys(record2).filter(k => !excludeFields.includes(k));
  
  if (keys1.length !== keys2.length) return false;
  
  for (const key of keys1) {
    if (String(record1[key]) !== String(record2[key])) {
      return false;
    }
  }
  
  return true;
};

// Helper function to fix seat formatting issues
const fixSeatFormatting = async (seats) => {
  if (!seats) return '';
  
  try {
    // Import the validation function
    const { validateConsecutiveSeats } = await import('../helpers/csvInventoryHelper.js');
    const validation = validateConsecutiveSeats(seats);
    
    // Log when seat formatting is applied
    if (validation.originalSeats !== validation.fixedSeats) {
      console.log(`🔧 Seat Format Fix: "${validation.originalSeats}" → "${validation.fixedSeats}"`);
    }
    
    return validation.fixedSeats || seats;
  } catch (error) {
    console.error(`Error fixing seat formatting: ${error.message}`);
    return seats;
  }
};

// Load existing CSV data for comparison (non-blocking)
const loadExistingCsvData = async () => {
  try {
    if (fs.existsSync(COMBINED_EVENTS_FILE)) {
      const csvContent = fs.readFileSync(COMBINED_EVENTS_FILE, 'utf8');
      const lines = csvContent.split('\n');
      
      if (lines.length > 1) {
        const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
        existingCsvData.clear();
        
        for (let i = 1; i < lines.length; i++) {
          const line = lines[i].trim();
          if (!line) continue;
          
          const values = line.split(',').map(v => v.trim().replace(/"/g, ''));
          const record = {};
          
          headers.forEach((header, index) => {
            record[header] = values[index] || '';
          });
          
          if (record.inventory_id) {
            const compositeKey = generateCompositeKey(record);
            existingCsvData.set(compositeKey, record);
          }
        }
        
        console.log(`Loaded ${existingCsvData.size} existing records from CSV for comparison`);
      }
    }
    lastCsvLoad = Date.now();
  } catch (error) {
    console.error(`Error loading existing CSV data: ${error.message}`);
    existingCsvData.clear();
  }
};

// In-memory storage for all events data
let allEventsData = new Map(); // eventId -> records array
let lastCombinedGeneration = 0;
let isGeneratingCSV = false; // Prevent concurrent CSV generation
let pendingUpdates = new Set(); // Track events with pending updates
let existingCsvData = new Map(); // composite_key -> existing record (for change detection)
let lastCsvLoad = 0; // Track when CSV was last loaded

// Load existing events data from JSON file (async)
const loadEventsData = async () => {
  if (fs.existsSync(EVENTS_DATA_FILE)) {
    try {
      const data = JSON.parse(fs.readFileSync(EVENTS_DATA_FILE, 'utf8'));
      allEventsData = new Map(Object.entries(data));
      console.log(`Loaded data for ${allEventsData.size} events from JSON storage`);
    } catch (error) {
      console.error(`Error loading events data: ${error.message}`);
      allEventsData = new Map();
    }
  }
};

// Save events data to JSON file (completely non-blocking)
const saveEventsData = () => {
  // Use setTimeout to ensure this runs after current execution
  setTimeout(() => {
    try {
      const data = Object.fromEntries(allEventsData);
      fs.writeFileSync(EVENTS_DATA_FILE, JSON.stringify(data));
    } catch (error) {
      console.error(`Error saving events data: ${error.message}`);
    }
  }, 0);
};

// Background CSV generation (completely separate from API responses)
const scheduleCSVGeneration = (force = false) => {
  if (isGeneratingCSV) return; // Already generating
  
  // Schedule for next tick to not block current operation
  setTimeout(() => {
    if (force || pendingUpdates.size > 0 || shouldGenerateCSV()) {
      generateCombinedCSVBackground();
    }
  }, 100); // Small delay to batch multiple updates
};

// Force CSV generation immediately (for manual triggers)
const forceCSVGeneration = () => {
  if (!isGeneratingCSV) {
    lastCombinedGeneration = 0; // Reset timer to force generation
    scheduleCSVGeneration(true);
  }
};

const shouldGenerateCSV = () => {
  const now = Date.now();
  const timeSinceLastGeneration = now - lastCombinedGeneration;
  const SIX_MINUTES = 6 * 60 * 1000; // 6 minutes in milliseconds
  return timeSinceLastGeneration > SIX_MINUTES; // Generate every 6 minutes as per behavior rules
};

const generateCombinedCSVBackground = async () => {
  if (isGeneratingCSV) return;
  
  isGeneratingCSV = true;
  pendingUpdates.clear();
  
  try {
    // Run in next tick to not block anything
    setImmediate(async () => {
      try {
        lastCombinedGeneration = Date.now();
        const now = Date.now();
        const SIX_MINUTES = 6 * 60 * 1000; // 6 minutes in milliseconds
        
        // Load existing CSV data for comparison (if not loaded recently)
        if (now - lastCsvLoad > 30000) { // Reload every 30 seconds
          await loadExistingCsvData();
        }
        
        // Import Event model to check Last_Updated from database
        const { Event } = await import('../models/index.js');
        
        // Get events updated in the last 6 minutes from database
        const recentlyUpdatedEvents = await Event.find({
          Last_Updated: { $gte: new Date(now - SIX_MINUTES) }
        })
          .select("Event_ID Event_Name Venue Event_DateTime Last_Updated")
          .lean();
        
        const recentEventIds = new Set(recentlyUpdatedEvents.map(event => event.Event_ID));
        const eventDetailsMap = new Map();
        recentlyUpdatedEvents.forEach(event => {
          eventDetailsMap.set(event.Event_ID, event);
        });
        
        console.log(`CSV Filter: Found ${recentEventIds.size} events updated in last 6 minutes (out of ${allEventsData.size} total events)`);
        
        // Collect only records from events updated in the last 6 minutes
        const allRecords = [];
        let includedEvents = 0;
        let excludedEvents = 0;
        
        for (const [eventId, records] of allEventsData) {
          if (recentEventIds.has(eventId)) {
            const eventDetails = eventDetailsMap.get(eventId);
            // Enrich records with event details for composite key generation
            const enrichedRecords = await Promise.all(records.map(async record => ({
              ...record,
              event_name: record.event_name || eventDetails?.Event_Name || `Event ${eventId}`,
              venue_name: record.venue_name || eventDetails?.Venue || "Unknown Venue",
              event_date: record.event_date || eventDetails?.Event_DateTime?.toISOString() || new Date().toISOString(),
              event_id: record.event_id || record.mapping_id || eventId,
              source_event_id: eventId,
              seats: await fixSeatFormatting(record.seats) // Fix seat formatting to prevent scientific notation
            })));
            allRecords.push(...enrichedRecords);
            includedEvents++;
          } else {
            excludedEvents++;
          }
        }
        
        console.log(`CSV Filter: Including ${includedEvents} events, excluding ${excludedEvents} events (stale > 6 min)`);
        
        if (allRecords.length === 0) {
          console.log('CSV Filter: No recent records to include in CSV - all events are stale');
          isGeneratingCSV = false;
          return;
        }

        // Process records according to behavior rules
        const finalRecords = new Map(); // composite_key -> record
        const newRecordsCount = { preserved: 0, updated: 0, created: 0 };
        
        // Process in chunks to avoid blocking
        const CHUNK_SIZE = 100; // Smaller chunks for better performance
        
        for (let i = 0; i < allRecords.length; i += CHUNK_SIZE) {
          const chunk = allRecords.slice(i, i + CHUNK_SIZE);
          
          chunk.forEach(record => {
            // Generate composite key for this record
            const compositeKey = generateCompositeKey(record);
            
            // Check if this record already exists in CSV
            const existingRecord = existingCsvData.get(compositeKey);
            
            if (existingRecord) {
              // Record exists - check if it's identical
              if (areRecordsIdentical(record, existingRecord)) {
                // Identical record - preserve existing inventory_id
                record.inventory_id = existingRecord.inventory_id;
                newRecordsCount.preserved++;
              } else {
                // Record has changes - generate new inventory_id (delete old, insert new)
                record.inventory_id = uuidv4();
                newRecordsCount.updated++;
              }
            } else {
              // New record - generate new inventory_id
              record.inventory_id = uuidv4();
              newRecordsCount.created++;
            }
            
            // Ensure required fields are properly mapped
            if (!record.event_id && record.mapping_id) {
              record.event_id = record.mapping_id;
            } else if (!record.mapping_id && record.event_id) {
              record.mapping_id = record.event_id;
            }
            
            // Use composite key to ensure no duplicates
            finalRecords.set(compositeKey, record);
          });
          
          // Yield control after each chunk
          if (i + CHUNK_SIZE < allRecords.length) {
            await new Promise(resolve => setImmediate(resolve));
          }
        }

        // Format records for export
        const formattedData = [];
        const recordsArray = Array.from(finalRecords.values());
        
        for (let i = 0; i < recordsArray.length; i += CHUNK_SIZE) {
          const chunk = recordsArray.slice(i, i + CHUNK_SIZE);
          const formattedChunk = chunk.map(record => formatInventoryForExport(record));
          formattedData.push(...formattedChunk);
          
          // Yield control after each chunk
          if (i + CHUNK_SIZE < recordsArray.length) {
            await new Promise(resolve => setImmediate(resolve));
          }
        }

        // Calculate removed records (existed in CSV but not in new data)
        const removedRecords = existingCsvData.size - newRecordsCount.preserved - newRecordsCount.updated;

        // Save to file (this is the only potentially slow operation)
        await new Promise((resolve, reject) => {
          setImmediate(() => {
            try {
              saveInventoryToCSV(formattedData, COMBINED_EVENTS_FILE);
              console.log(`✅ Background: Generated combined CSV with ${formattedData.length} records from ${includedEvents} recent events`);
              console.log(`📊 CSV Changes: ${newRecordsCount.preserved} preserved, ${newRecordsCount.updated} updated, ${newRecordsCount.created} created, ${Math.max(0, removedRecords)} removed`);
              console.log(`🚫 Excluded: ${excludedEvents} stale events (>6 min)`);
              resolve();
            } catch (error) {
              console.error(`Error in background CSV generation: ${error.message}`);
              reject(error);
            }
          });
        });
        
      } catch (error) {
        console.error(`Background CSV generation error: ${error.message}`);
      } finally {
        isGeneratingCSV = false;
      }
    });
  } catch (error) {
    console.error(`Error scheduling background CSV generation: ${error.message}`);
    isGeneratingCSV = false;
  }
};

/**
 * Optimized Controller for managing inventory data for 1000+ events (NON-BLOCKING)
 */
class InventoryController {
  constructor() {
    // Load existing data asynchronously
    this.loadingPromise = loadEventsData();
    this.processingQueue = new Set(); // Track events being processed
    this.lastUpdate = new Map(); // Track last update time per event
  }

  /**
   * Ensure data is loaded before operations
   */
  async ensureLoaded() {
    if (this.loadingPromise) {
      await this.loadingPromise;
      this.loadingPromise = null;
    }
  }

  /**
   * Delete old CSV files for a specific event (DISABLED - using JSON now)
   * @param {string} eventId - The event ID
   */
  deleteOldCsvFiles(eventId) {
    // No longer needed - using in-memory JSON storage
    return;
  }

  /**
   * Add inventory records in bulk (SUPER FAST - non-blocking)
   * @param {Array} records - Array of inventory records to add
   * @param {string} eventId - The event ID these records belong to
   */
  addBulkInventory(records, eventId) {
    try {
      if (!records || !records.length) {
        return { success: false, message: 'No records provided' };
      }

      // Fast validation (only essential checks)
      const validatedRecords = records
        .map(record => {
          // Quick validation - only check essential fields
          if (!record.section || !record.row || !record.seats) {
            return null;
          }
          record.source_event_id = eventId;
          
          // Ensure inventory_id is present (will be properly assigned during CSV generation)
          if (!record.inventory_id) {
            record.inventory_id = uuidv4(); // Temporary ID for in-memory storage
          }
          
          return record;
        })
        .filter(record => record !== null);

      if (validatedRecords.length === 0) {
        return { success: false, message: 'No valid records after filtering' };
      }

      // Fast deduplication using Map
      const uniqueRecords = new Map();
      validatedRecords.forEach(record => {
        const key = `${record.section}-${record.row}-${record.seats}`;
        uniqueRecords.set(key, record);
      });

      // Get existing records for this event (fast Map lookup)
      const existingRecords = allEventsData.get(eventId) || [];
      const newRecords = Array.from(uniqueRecords.values());

      // Quick change detection (simplified)
      const hasChanges = existingRecords.length !== newRecords.length;

      // Update in-memory storage (instant)
      allEventsData.set(eventId, newRecords);
      this.lastUpdate.set(eventId, Date.now());

      // Mark for background processing
      pendingUpdates.add(eventId);

      // Schedule background operations (non-blocking)
      saveEventsData(); // Async save
      scheduleCSVGeneration(); // Async CSV generation

      // Return immediately (no waiting for file operations)
      return { 
        success: true, 
        message: `Processed ${newRecords.length} records for event ${eventId}`,
        noChanges: !hasChanges,
        stats: {
          processed: newRecords.length,
          unique: uniqueRecords.size
        }
      };
    } catch (error) {
      return { success: false, message: error.message };
    }
  }

  /**
   * Check if we should generate the combined CSV (fast check)
   */
  shouldGenerateCombinedCSV() {
    return shouldGenerateCSV();
  }

  /**
   * Generate combined CSV from in-memory data (BACKGROUND ONLY)
   * This method should not be called directly from API endpoints
   */
  generateCombinedEventsCSV() {
    // For API compatibility, just schedule background generation
    scheduleCSVGeneration();
    
    return {
      success: true,
      message: `CSV generation scheduled in background`,
      filePath: COMBINED_EVENTS_FILE,
      recordCount: this.getTotalRecords(),
      eventCount: allEventsData.size
    };
  }

  /**
   * Force generation of combined CSV (BACKGROUND ONLY)
   */
  forceCombinedCSVGeneration() {
    // Force immediate background generation using the new behavior rules
    forceCSVGeneration();
    
    return {
      success: true,
      message: `CSV generation forced in background with behavior rules (6-minute cycle, UUID inventory_ids, composite key matching)`,
      filePath: COMBINED_EVENTS_FILE,
      recordCount: this.getTotalRecords(),
      eventCount: allEventsData.size,
      behaviorRules: {
        cycleInterval: "6 minutes",
        inventoryIdGeneration: "uuid.v4() for new/changed records",
        compositeKeyMatching: "event_title + venue + datetime + event_code + section + row",
        changeDetection: "Row-level updates with delete/insert logic",
        duplicatePrevention: "Ensured via composite keys"
      }
    };
  }

  /**
   * Get total records count (fast)
   */
  getTotalRecords() {
    let total = 0;
    for (const records of allEventsData.values()) {
      total += records.length;
    }
    return total;
  }

  /**
   * Get statistics about current data (FAST)
   */
  getStats() {
    const totalRecords = this.getTotalRecords();
    const eventStats = [];
    const now = Date.now();
    const timeSinceLastGeneration = now - lastCombinedGeneration;
    const timeUntilNextGeneration = Math.max(0, (6 * 60 * 1000) - timeSinceLastGeneration);
    
    // Only get top 10 events for performance
    let count = 0;
    for (const [eventId, records] of allEventsData) {
      if (count >= 10) break;
      eventStats.push({
        eventId,
        recordCount: records.length,
        lastUpdate: this.lastUpdate.get(eventId)
      });
      count++;
    }

    return {
      totalEvents: allEventsData.size,
      totalRecords,
      pendingUpdates: pendingUpdates.size,
      isGeneratingCSV,
      csvGenerationStats: {
        lastGeneration: new Date(lastCombinedGeneration).toISOString(),
        timeUntilNext: `${Math.floor(timeUntilNextGeneration / 60000)}m ${Math.floor((timeUntilNextGeneration % 60000) / 1000)}s`,
        existingCsvRecords: existingCsvData.size,
        lastCsvLoad: new Date(lastCsvLoad).toISOString()
      },
      behaviorRules: {
        generationInterval: "Every 6 minutes",
        inventoryIdStrategy: "UUID v4 for new/changed records, preserve for identical",
        compositeKeyFields: ["event_name", "venue_name", "event_date", "event_id", "section", "row"],
        changeDetectionLogic: "Delete existing + Insert new with new UUID",
        duplicatePrevention: "Composite key uniqueness"
      },
      eventStats: eventStats.sort((a, b) => (b.lastUpdate || 0) - (a.lastUpdate || 0))
    };
  }

  /**
   * Clear old event data (cleanup for memory management) - BACKGROUND ONLY
   */
  cleanupOldEvents(maxAgeHours = 24) {
    // Schedule cleanup in background
    setTimeout(() => {
      const cutoffTime = Date.now() - (maxAgeHours * 60 * 60 * 1000);
      let removedCount = 0;

      for (const [eventId, lastUpdate] of this.lastUpdate) {
        if (lastUpdate < cutoffTime) {
          allEventsData.delete(eventId);
          this.lastUpdate.delete(eventId);
          pendingUpdates.delete(eventId);
          removedCount++;
        }
      }

      if (removedCount > 0) {
        console.log(`Cleaned up ${removedCount} old events from memory`);
        saveEventsData();
      }
    }, 0);

    return 0; // Return immediately
  }

  // Legacy methods for compatibility (all fast/non-blocking)
  loadInventory() { return; }
  saveInventory() { return true; }
  getAllInventory() { 
    // Return a sample for performance
    const sample = [];
    let count = 0;
    for (const records of allEventsData.values()) {
      sample.push(...records.slice(0, 10)); // Max 10 per event
      count++;
      if (count >= 10 || sample.length >= 100) break; // Max 100 total
    }
    return sample;
  }
  addInventory() { return { success: false, message: 'Use addBulkInventory instead' }; }
  updateInventory() { return { success: false, message: 'Use addBulkInventory instead' }; }
  deleteInventory() { return { success: false, message: 'Use addBulkInventory instead' }; }
  exportInventory() { return this.generateCombinedEventsCSV(); }
  importInventory() { return { success: false, message: 'Not supported in optimized mode' }; }
  cleanupEventCsvFiles() { return { success: true, message: 'No CSV files to clean in optimized mode' }; }
}

// Create and export singleton instance
const inventoryController = new InventoryController();

// Export optimized methods
export const generateCombinedEventsCSV = () => {
  return inventoryController.generateCombinedEventsCSV();
};

export const forceCombinedCSVGeneration = () => {
  return inventoryController.forceCombinedCSVGeneration();
};

export const getInventoryStats = () => {
  return inventoryController.getStats();
};

export const cleanupOldEvents = (maxAgeHours = 24) => {
  return inventoryController.cleanupOldEvents(maxAgeHours);
};

// Legacy exports for compatibility
export const cleanupEventCsvFiles = () => {
  return { success: true, message: 'No CSV files to clean in optimized mode' };
};

export const startScrapeCycle = () => {
  return { success: true, message: 'Scrape cycles not needed in optimized mode' };
};

export const markEventScraped = () => {
  return { success: true, message: 'Event marking not needed in optimized mode' };
};

export default inventoryController; 