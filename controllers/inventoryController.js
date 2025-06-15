import fs from 'fs';
import path from 'path';
import { customAlphabet } from 'nanoid';
const generateNumericId10 = customAlphabet('0123456789', 10);
import { 
  validateConsecutiveSeats, 
  saveInventoryToCSV, 
  readInventoryFromCSV,
  validateAndFixInventoryRecord,
  formatInventoryForExport
} from '../helpers/csvInventoryHelper.js';
import SyncService from '../services/syncService.js';
import { ensureBlankCsvExists } from '../uploadInventory.js';
import { ENABLE_CSV_PROCESSING } from '../scraperManager.js'; // Import the flag

// Constants for SyncService (mirroring uploadInventory.js)
// Ideally, these should be in environment variables
const COMPANY_ID = '702';
const API_TOKEN = 'OaJwtlUQiriMSrnGd7cauDWtIyAMnS363icaz-7t1vJ7bjIBe9ZFjBwgPYY1Q9eKV_Jt';

const COMPREHENSIVE_CSV_HEADERS = [
  'inventory_id', 'event_name', 'venue_name', 'event_date', 'event_id',
  'quantity', 'section', 'row', 'seats', 'barcodes', 'internal_notes',
  'public_notes', 'tags', 'list_price', 'face_price', 'taxed_cost', 'cost',
  'hide_seats', 'in_hand', 'in_hand_date', 'instant_transfer', 'files_available',
  'split_type', 'custom_split', 'stock_type', 'zone', 'shown_quantity',
  'passthrough', 'mapping_id'
];

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
// AGGRESSIVE UPDATE STRATEGY: Any change triggers complete row replacement
const areRecordsIdentical = (record1, record2, changeTracker = null) => {
  // Compare ALL important fields that should trigger a change in inventory_id
  // This implements the user's requirement for aggressive updates
  const importantFields = [
    'section', 
    'row', 
    'seats',
    'quantity',
    'list_price',
    'face_price',
    'cost',
    'taxed_cost',
    'in_hand_date',
    'in_hand',
    'instant_transfer',
    'stock_type',
    'hide_seats',
    'internal_notes',
    'public_notes',
    'tags'
  ];
  
  for (const field of importantFields) {
    // Convert to string for consistent comparison, handle undefined/null
    const val1 = (record1[field] !== undefined && record1[field] !== null) ? String(record1[field]).trim() : '';
    const val2 = (record2[field] !== undefined && record2[field] !== null) ? String(record2[field]).trim() : '';
    
    if (val1 !== val2) {
      // Track change instead of logging immediately
      if (changeTracker) {
        changeTracker.changes++;
        if (changeTracker.changes <= 3) { // Only track first 3 examples
          changeTracker.examples.push({
            field,
            section: record1.section,
            row: record1.row,
            oldValue: val1,
            newValue: val2
          });
        }
      }
      return false;
    }
  }
  
  return true;
};

// Load existing CSV data for comparison (non-blocking)
const loadExistingCsvData = async () => {
  try {
    if (fs.existsSync(COMBINED_EVENTS_FILE)) {
      const records = await readInventoryFromCSV(COMBINED_EVENTS_FILE);
      existingCsvData.clear();

      if (records && records.length > 0) {
        records.forEach(record => {
          // Ensure record is valid and has an inventory_id before processing
          if (record && record.inventory_id) { 
            const compositeKey = generateCompositeKey(record);
            existingCsvData.set(compositeKey, record);
          }
        });
        console.log(`Loaded ${existingCsvData.size} existing records from CSV for comparison using readInventoryFromCSV`);
      } else {
        console.log('No records found or empty CSV when loading existing data via readInventoryFromCSV.');
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
  if (!ENABLE_CSV_PROCESSING) {
    console.log('CSV processing is disabled by ENABLE_CSV_PROCESSING flag. Skipping combined CSV generation.');
    isGeneratingCSV = false; // Ensure this is reset if we bail early
    return;
  }
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
        
        // AGGRESSIVE CLEANUP: Remove ALL records from events not updated in last 6 minutes
        // This implements the user's requirement to exclude stale events completely
        const existingRecordsToKeep = new Set();
        
        // DO NOT keep any records from events that haven't been updated in 6 minutes
        // This ensures complete removal of stale event inventory as requested
        console.log(`🗑️  AGGRESSIVE CLEANUP: Removing ALL inventory from events not updated in last 6 minutes`);
        
        // Count how many records we're removing for logging
        let removedRecordsCount = 0;
        for (const [compositeKey, record] of existingCsvData) {
          const sourceEventId = record.source_event_id;
          if (sourceEventId && !recentEventIds.has(sourceEventId)) {
            removedRecordsCount++;
          }
        }
        
        console.log(`🗑️  Removing ${removedRecordsCount} records from stale events (>6 minutes old)`);
        
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
              event_date: record.event_date || (eventDetails?.Event_DateTime instanceof Date ? eventDetails.Event_DateTime.toISOString() : new Date().toISOString()),
              event_id: record.event_id || record.mapping_id || eventId,
              source_event_id: eventId,
              seats: record.seats // Removed fixSeatFormatting call
            })));
            allRecords.push(...enrichedRecords);
            includedEvents++;
          } else {
            excludedEvents++;
          }
        }
        
        console.log(`CSV Filter: Including ${includedEvents} events, excluding ${excludedEvents} events (stale > 6 min)`);
        
        if (allRecords.length === 0 && existingRecordsToKeep.size === 0) {
          console.log('CSV Filter: No records to include in CSV - all events are stale and no existing records to keep');
          isGeneratingCSV = false;
          return;
        }

        // Process records according to AGGRESSIVE UPDATE behavior rules
        const finalRecords = new Map(); // composite_key -> record
        const newRecordsCount = { preserved: 0, updated: 0, created: 0, removed: removedRecordsCount };
        
        // AGGRESSIVE STRATEGY: Only include records from recently updated events
        // No existing records are preserved from stale events (>6 minutes)
        console.log(`🔄 AGGRESSIVE UPDATE: Processing only records from ${recentEventIds.size} recently updated events`);
        
        // Process in chunks to avoid blocking
        const CHUNK_SIZE = 100; // Smaller chunks for better performance
        
        // Track changes for summary
        const changeTracker = { changes: 0, examples: [] };
        
        for (let i = 0; i < allRecords.length; i += CHUNK_SIZE) {
          const chunk = allRecords.slice(i, i + CHUNK_SIZE);
          
          // Track which composite keys we've seen for each event
          const processedKeysPerEvent = new Map();
          
          chunk.forEach(record => {
            // Generate composite key for this record
            const compositeKey = generateCompositeKey(record);
            const eventId = record.source_event_id;
            
            // Track this key for the event
            if (!processedKeysPerEvent.has(eventId)) {
              processedKeysPerEvent.set(eventId, new Set());
            }
            processedKeysPerEvent.get(eventId).add(compositeKey);
            
            // Check if this record already exists in CSV
            const existingRecord = existingCsvData.get(compositeKey);
            
            if (existingRecord) {
              // Record exists - check if it's identical
              if (areRecordsIdentical(record, existingRecord, changeTracker)) {
                // Identical record - preserve existing inventory_id
                record.inventory_id = existingRecord.inventory_id;
                newRecordsCount.preserved++;
              } else {
                // Record has changes - generate new inventory_id (delete old, insert new)
                record.inventory_id = generateNumericId10();
                newRecordsCount.updated++;
              }
            } else {
              // New record - generate new inventory_id
              record.inventory_id = generateNumericId10();
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
          
          // Remove records that no longer exist for each updated event
          for (const [eventId, processedKeys] of processedKeysPerEvent) {
            // Find all existing records for this event
            for (const [compositeKey, record] of existingCsvData) {
              if (record.source_event_id === eventId && !processedKeys.has(compositeKey)) {
                // This record is from an updated event but wasn't in the new data
                // It should be removed (by not adding it to finalRecords)
                newRecordsCount.removed++;
              }
            }
          }
          
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

        // Save to file (this is the only potentially slow operation)
        await new Promise((resolve, reject) => {
          setImmediate(async () => {
            try {
              await saveInventoryToCSV(formattedData, COMBINED_EVENTS_FILE);
              console.log(`✅ AGGRESSIVE UPDATE: Generated combined CSV with ${formattedData.length} records from ${includedEvents} recently updated events ONLY`);
              console.log(`📊 CSV Changes: ${newRecordsCount.preserved} preserved, ${newRecordsCount.updated} updated, ${newRecordsCount.created} created, ${newRecordsCount.removed} removed from stale events`);
              console.log(`🗑️  AGGRESSIVE CLEANUP: Completely removed ${excludedEvents} stale events (>6 min) and all their inventory`);
              
              // Show change summary instead of individual logs
              if (changeTracker.changes > 0) {
                console.log(`🔄 CHANGE SUMMARY: ${changeTracker.changes} field changes detected`);
                if (changeTracker.examples.length > 0) {
                  console.log(`📝 Examples (first ${changeTracker.examples.length}):`);
                  changeTracker.examples.forEach((example, index) => {
                    console.log(`   ${index + 1}. ${example.field} change in ${example.section}/${example.row}: "${example.oldValue}" → "${example.newValue}"`);
                  });
                  if (changeTracker.changes > changeTracker.examples.length) {
                    console.log(`   ... and ${changeTracker.changes - changeTracker.examples.length} more changes`);
                  }
                }
              } else {
                console.log(`🔄 No field changes detected - all records identical`);
              }
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
            record.inventory_id = generateNumericId10(); // Temporary ID for in-memory storage
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
      message: `CSV generation forced in background with AGGRESSIVE UPDATE strategy (6-minute cycle, complete stale event removal)`,
      filePath: COMBINED_EVENTS_FILE,
      recordCount: this.getTotalRecords(),
      eventCount: allEventsData.size,
      behaviorRules: {
        cycleInterval: "6 minutes",
        inventoryIdGeneration: "New numeric ID for ANY change in quantity, price, or attributes",
        compositeKeyMatching: "event_title + venue + datetime + event_code + section + row",
        changeDetection: "AGGRESSIVE: Any field change triggers complete row replacement",
        staleEventHandling: "COMPLETE REMOVAL: Events >6min old are entirely excluded from CSV",
        duplicatePrevention: "Ensured via composite keys",
        aggressiveUpdates: "Quantity, price, seat changes = delete old + insert new with new ID"
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
        inventoryIdStrategy: "AGGRESSIVE: New numeric ID for ANY change, preserve only for identical records",
        compositeKeyFields: ["event_name", "venue_name", "event_date", "event_id", "section", "row"],
        changeDetectionLogic: "AGGRESSIVE: Delete existing + Insert new with new ID on ANY field change",
        staleEventHandling: "COMPLETE REMOVAL: Events >6min old entirely excluded from CSV",
        duplicatePrevention: "Composite key uniqueness",
        sensitiveFields: "quantity, price, seats, cost, notes, dates - ANY change triggers replacement"
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

  async uploadBlankCsv (req, res) {
    const DATA_DIR = path.join(process.cwd(), 'data'); 
    const BLANK_CSV_PATH = path.join(DATA_DIR, 'blank.csv');

    try {
      // Ensure data directory exists
      if (!fs.existsSync(DATA_DIR)) {
        fs.mkdirSync(DATA_DIR, { recursive: true });
      }

      const syncService = new SyncService(COMPANY_ID, API_TOKEN);

      // Create the blank CSV with comprehensive headers
      ensureBlankCsvExists(COMPREHENSIVE_CSV_HEADERS);
      console.log(`Blank CSV with comprehensive headers ensured at ${BLANK_CSV_PATH}`);

      // Upload it
      const uploadResult = await syncService.uploadCsvToSync(BLANK_CSV_PATH);
      console.log('Comprehensive blank CSV upload result:', uploadResult);

      if (uploadResult && uploadResult.success) {
        res.json({ success: true, message: 'Comprehensive blank CSV uploaded successfully.', uploadId: uploadResult.uploadId });
      } else {
        // If uploadResult itself indicates failure but doesn't throw
        throw new Error(uploadResult?.message || 'Failed to upload comprehensive blank CSV.');
      }
    } catch (error) {
      console.error('Error in uploadComprehensiveBlankCsv:', error.message);
      res.status(500).json({ success: false, message: error.message || 'Internal server error during blank CSV upload.' });
    } finally {
      // Clean up the temporary file in both success and error cases (after response is sent)
      if (fs.existsSync(BLANK_CSV_PATH)) {
        try {
          fs.unlinkSync(BLANK_CSV_PATH);
          console.log(`Temporary blank CSV deleted from ${BLANK_CSV_PATH}`);
        } catch (cleanupError) {
          console.error('Error cleaning up temp CSV:', cleanupError);
        }
      }
    }
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