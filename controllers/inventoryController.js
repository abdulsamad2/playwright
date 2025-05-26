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
let isGeneratingCSV = false; // Prevent concurrent CSV generation
let pendingUpdates = new Set(); // Track events with pending updates

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
const scheduleCSVGeneration = () => {
  if (isGeneratingCSV) return; // Already generating
  
  // Schedule for next tick to not block current operation
  setTimeout(() => {
    if (pendingUpdates.size > 0 || shouldGenerateCSV()) {
      generateCombinedCSVBackground();
    }
  }, 100); // Small delay to batch multiple updates
};

const shouldGenerateCSV = () => {
  const now = Date.now();
  const timeSinceLastGeneration = now - lastCombinedGeneration;
  return timeSinceLastGeneration > 30000; // Generate every 30 seconds max
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
        
        // Collect all records from memory (fast operation)
        const allRecords = [];
        for (const [eventId, records] of allEventsData) {
          allRecords.push(...records);
        }

        if (allRecords.length === 0) {
          isGeneratingCSV = false;
          return;
        }

        // Process in chunks to avoid blocking
        const CHUNK_SIZE = 500; // Smaller chunks for better performance
        const uniqueRecords = new Map();
        
        for (let i = 0; i < allRecords.length; i += CHUNK_SIZE) {
          const chunk = allRecords.slice(i, i + CHUNK_SIZE);
          
          chunk.forEach(record => {
            const key = `${record.section}-${record.row}-${record.seats}`;
            if (!uniqueRecords.has(key) || 
                new Date(record.in_hand_date) > new Date(uniqueRecords.get(key).in_hand_date)) {
              uniqueRecords.set(key, record);
            }
          });
          
          // Yield control after each chunk
          if (i + CHUNK_SIZE < allRecords.length) {
            await new Promise(resolve => setImmediate(resolve));
          }
        }

        const finalRecords = Array.from(uniqueRecords.values());
        
        // Format in chunks
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
          
          // Yield control after each chunk
          if (i + CHUNK_SIZE < finalRecords.length) {
            await new Promise(resolve => setImmediate(resolve));
          }
        }

        // Save to file (this is the only potentially slow operation)
        await new Promise((resolve, reject) => {
          setImmediate(() => {
            try {
              saveInventoryToCSV(formattedData, COMBINED_EVENTS_FILE);
              console.log(`✅ Background: Generated combined CSV with ${formattedData.length} records from ${allEventsData.size} events`);
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
    // Force immediate background generation
    if (!isGeneratingCSV) {
      generateCombinedCSVBackground();
    }
    
    return {
      success: true,
      message: `CSV generation forced in background`,
      filePath: COMBINED_EVENTS_FILE,
      recordCount: this.getTotalRecords(),
      eventCount: allEventsData.size
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