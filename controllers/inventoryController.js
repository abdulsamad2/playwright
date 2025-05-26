import fs from 'fs';
import path from 'path';
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

// In-memory storage for all events data
let allEventsData = new Map(); // eventId -> records array
let lastCombinedGeneration = 0;

// Load existing events data from JSON file
const loadEventsData = () => {
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

// Save events data to JSON file (non-blocking)
const saveEventsData = () => {
  setImmediate(() => {
    try {
      const data = Object.fromEntries(allEventsData);
      fs.writeFileSync(EVENTS_DATA_FILE, JSON.stringify(data));
    } catch (error) {
      console.error(`Error saving events data: ${error.message}`);
    }
  });
};

/**
 * Optimized Controller for managing inventory data for 1000+ events
 */
class InventoryController {
  constructor() {
    // Load existing data
    loadEventsData();
    this.processingQueue = new Set(); // Track events being processed
    this.lastUpdate = new Map(); // Track last update time per event
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
   * Add inventory records in bulk (OPTIMIZED for 1000+ events)
   * @param {Array} records - Array of inventory records to add
   * @param {string} eventId - The event ID these records belong to
   */
  addBulkInventory(records, eventId) {
    try {
      if (!records || !records.length) {
        return { success: false, message: 'No records provided' };
      }

      // Validate all records in parallel
      const validatedRecords = records
        .map(record => validateAndFixInventoryRecord(record))
        .filter(record => record !== null); // Remove invalid records

      if (validatedRecords.length === 0) {
        return { success: false, message: 'No valid records after filtering' };
      }

      // Create unique records map for deduplication
      const uniqueRecords = new Map();
      validatedRecords.forEach(record => {
        const key = `${record.section}-${record.row}-${record.seats}`;
        record.source_event_id = eventId;
        uniqueRecords.set(key, record);
      });

      // Get existing records for this event
      const existingRecords = allEventsData.get(eventId) || [];
      const existingMap = new Map();
      existingRecords.forEach(record => {
        const key = `${record.section}-${record.row}-${record.seats}`;
        existingMap.set(key, record);
      });

      // Calculate changes
      const newRecords = Array.from(uniqueRecords.values());
      const recordsToAdd = newRecords.filter(record => {
        const key = `${record.section}-${record.row}-${record.seats}`;
        return !existingMap.has(key);
      });

      const recordsToUpdate = newRecords.filter(record => {
        const key = `${record.section}-${record.row}-${record.seats}`;
        const existing = existingMap.get(key);
        if (!existing) return false;
        
        // Quick comparison of key fields
        return existing.list_price !== record.list_price || 
               existing.quantity !== record.quantity;
      });

      const recordsToDelete = existingRecords.filter(record => {
        const key = `${record.section}-${record.row}-${record.seats}`;
        return !uniqueRecords.has(key);
      });

      // Update in-memory storage
      allEventsData.set(eventId, newRecords);
      this.lastUpdate.set(eventId, Date.now());

      // Save to JSON (non-blocking)
      saveEventsData();

      // Check if we should generate combined CSV (every 10 events or 30 seconds)
      const shouldGenerate = this.shouldGenerateCombinedCSV();
      if (shouldGenerate) {
        setImmediate(() => this.generateCombinedEventsCSV());
      }

      const hasChanges = recordsToAdd.length > 0 || recordsToUpdate.length > 0 || recordsToDelete.length > 0;

      return { 
        success: true, 
        message: hasChanges 
          ? `Processed ${newRecords.length} records for event ${eventId}: ${recordsToAdd.length} added, ${recordsToUpdate.length} updated, ${recordsToDelete.length} deleted`
          : `No changes for event ${eventId}`,
        noChanges: !hasChanges,
        stats: {
          added: recordsToAdd.length,
          updated: recordsToUpdate.length,
          deleted: recordsToDelete.length
        }
      };
    } catch (error) {
      return { success: false, message: error.message };
    }
  }

  /**
   * Check if we should generate the combined CSV
   */
  shouldGenerateCombinedCSV() {
    const now = Date.now();
    const timeSinceLastGeneration = now - lastCombinedGeneration;
    
    // Generate every 30 seconds or when we have 10+ updated events
    const recentUpdates = Array.from(this.lastUpdate.values())
      .filter(time => now - time < 30000).length;
    
    return timeSinceLastGeneration > 30000 || recentUpdates >= 10;
  }

  /**
   * Generate combined CSV from in-memory data (OPTIMIZED)
   */
  generateCombinedEventsCSV() {
    try {
      lastCombinedGeneration = Date.now();
      
      console.log(`Generating combined CSV from ${allEventsData.size} events in memory`);
      
      // Collect all records from memory
      const allRecords = [];
      let totalRecords = 0;
      
      for (const [eventId, records] of allEventsData) {
        totalRecords += records.length;
        allRecords.push(...records);
      }

      console.log(`Processing ${totalRecords} total records from ${allEventsData.size} events`);

      if (allRecords.length === 0) {
        return {
          success: false,
          message: 'No records found in memory to generate CSV',
          filePath: null
        };
      }

      // Deduplicate records using Map for O(1) lookup
      const uniqueRecords = new Map();
      allRecords.forEach(record => {
        const key = `${record.section}-${record.row}-${record.seats}`;
        
        // Keep the most recent record if duplicates exist
        if (!uniqueRecords.has(key) || 
            new Date(record.in_hand_date) > new Date(uniqueRecords.get(key).in_hand_date)) {
          uniqueRecords.set(key, record);
        }
      });

      const finalRecords = Array.from(uniqueRecords.values());
      console.log(`Deduplicated to ${finalRecords.length} unique records`);

      // Format for export in parallel chunks
      const CHUNK_SIZE = 1000;
      const formattedData = [];
      
      for (let i = 0; i < finalRecords.length; i += CHUNK_SIZE) {
        const chunk = finalRecords.slice(i, i + CHUNK_SIZE);
        const formattedChunk = chunk.map(record => {
          // Ensure required fields
          if (!record.event_id && record.mapping_id) {
            record.event_id = record.mapping_id;
          } else if (!record.mapping_id && record.event_id) {
            record.mapping_id = record.event_id;
          }
          
          return formatInventoryForExport(record);
        });
        formattedData.push(...formattedChunk);
      }

      // Save to combined CSV file
      saveInventoryToCSV(formattedData, COMBINED_EVENTS_FILE);

      // Verify the file
      const missingEventId = formattedData.filter(r => !r.event_id).length;
      const missingMappingId = formattedData.filter(r => !r.mapping_id).length;
      
      if (missingEventId > 0 || missingMappingId > 0) {
        console.warn(`WARNING: ${missingEventId} records missing event_id, ${missingMappingId} missing mapping_id`);
      }

      console.log(`✅ Generated combined CSV with ${formattedData.length} records from ${allEventsData.size} events`);

      return {
        success: true,
        message: `Generated combined CSV with ${formattedData.length} records from ${allEventsData.size} events`,
        filePath: COMBINED_EVENTS_FILE,
        recordCount: formattedData.length,
        eventCount: allEventsData.size
      };
    } catch (error) {
      console.error(`Error generating combined CSV: ${error.message}`);
      return {
        success: false,
        message: `Error generating combined CSV: ${error.message}`,
        filePath: null
      };
    }
  }

  /**
   * Force generation of combined CSV
   */
  forceCombinedCSVGeneration() {
    return this.generateCombinedEventsCSV();
  }

  /**
   * Get statistics about current data
   */
  getStats() {
    let totalRecords = 0;
    const eventStats = [];
    
    for (const [eventId, records] of allEventsData) {
      totalRecords += records.length;
      eventStats.push({
        eventId,
        recordCount: records.length,
        lastUpdate: this.lastUpdate.get(eventId)
      });
    }

    return {
      totalEvents: allEventsData.size,
      totalRecords,
      eventStats: eventStats.sort((a, b) => (b.lastUpdate || 0) - (a.lastUpdate || 0))
    };
  }

  /**
   * Clear old event data (cleanup for memory management)
   */
  cleanupOldEvents(maxAgeHours = 24) {
    const cutoffTime = Date.now() - (maxAgeHours * 60 * 60 * 1000);
    let removedCount = 0;

    for (const [eventId, lastUpdate] of this.lastUpdate) {
      if (lastUpdate < cutoffTime) {
        allEventsData.delete(eventId);
        this.lastUpdate.delete(eventId);
        removedCount++;
      }
    }

    if (removedCount > 0) {
      console.log(`Cleaned up ${removedCount} old events from memory`);
      saveEventsData();
    }

    return removedCount;
  }

  // Legacy methods for compatibility (simplified)
  loadInventory() { return; }
  saveInventory() { return true; }
  getAllInventory() { return Array.from(allEventsData.values()).flat(); }
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