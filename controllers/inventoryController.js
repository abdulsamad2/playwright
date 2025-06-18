import fs from 'fs';
import path from 'path';
import { customAlphabet } from 'nanoid';
const generateNumericId10 = customAlphabet('0123456789', 10);
// CSV helper imports removed
// CSV imports removed

// Constants for SyncService (mirroring uploadInventory.js)
// Ideally, these should be in environment variables
const COMPANY_ID = '702';
const API_TOKEN = 'OaJwtlUQiriMSrnGd7cauDWtIyAMnS363icaz-7t1vJ7bjIBe9ZFjBwgPYY1Q9eKV_Jt';

// CSV headers and data directory setup removed

// All CSV utility functions removed

// CSV data loading functions removed

// CSV generation system removed

// All CSV generation code removed

/**
 * Optimized Controller for managing inventory data for 1000+ events (NON-BLOCKING)
 */
class InventoryController {
  constructor() {
    this.processingQueue = new Set(); // Track events being processed
    this.lastUpdate = new Map(); // Track last update time per event
  }

  // Data loading methods removed

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

      const newRecords = Array.from(uniqueRecords.values());
      this.lastUpdate.set(eventId, Date.now());

      // Return immediately (CSV generation removed)
      return { 
        success: true, 
        message: `Processed ${newRecords.length} records for event ${eventId} (CSV generation disabled)`,
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
   * CSV generation methods removed
   */
  shouldGenerateCombinedCSV() {
    return false; // CSV generation disabled
  }

  generateCombinedEventsCSV() {
    return {
      success: false,
      message: "CSV generation has been disabled"
    };
  }

  forceCombinedCSVGeneration() {
    return {
      success: false,
      message: "CSV generation has been disabled"
    };
  }

  /**
   * Get total records count (disabled)
   */
  getTotalRecords() {
    return 0; // CSV generation disabled
  }

  /**
   * Get statistics about current data (CSV generation disabled)
   */
  getStats() {
    return {
      totalEvents: 0,
      totalRecords: 0,
      message: "CSV generation has been disabled",
      eventStats: []
    };
  }

  /**
   * Clear old event data (cleanup disabled)
   */
  cleanupOldEvents(maxAgeHours = 24) {
    // Cleanup disabled - CSV generation removed
    return 0;
  }

  async uploadBlankCsv (req, res) {
    res.status(501).json({ 
      success: false, 
      message: 'CSV upload functionality has been disabled' 
    });
  }

  // Legacy methods for compatibility (CSV generation disabled)
  loadInventory() { return; }
  saveInventory() { return true; }
  getAllInventory() { 
    return []; // CSV generation disabled
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