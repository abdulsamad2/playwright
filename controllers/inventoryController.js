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
const DEFAULT_INVENTORY_FILE = path.join(DATA_DIR, 'inventory.csv');
const PROCESSED_EVENTS_FILE = path.join(DATA_DIR, 'processed_events.json');

// Create data directory if it doesn't exist
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// Maintain a record of processed events to avoid duplicate CSV generation
const getProcessedEvents = () => {
  if (fs.existsSync(PROCESSED_EVENTS_FILE)) {
    try {
      return JSON.parse(fs.readFileSync(PROCESSED_EVENTS_FILE, 'utf8'));
    } catch (error) {
      console.error(`Error reading processed events file: ${error.message}`);
      return {};
    }
  }
  return {};
};

const saveProcessedEvents = (processedEvents) => {
  try {
    fs.writeFileSync(PROCESSED_EVENTS_FILE, JSON.stringify(processedEvents, null, 2));
    return true;
  } catch (error) {
    console.error(`Error saving processed events file: ${error.message}`);
    return false;
  }
};

/**
 * Controller for managing inventory data
 */
class InventoryController {
  constructor() {
    // Initialize with empty data
    this.inventoryData = [];
    this.processedEvents = getProcessedEvents();
    this.loadInventory();
  }

  /**
   * Load inventory data from CSV file
   */
  loadInventory(filePath = DEFAULT_INVENTORY_FILE) {
    try {
      if (fs.existsSync(filePath)) {
        this.inventoryData = readInventoryFromCSV(filePath);
        console.log(`Loaded ${this.inventoryData.length} inventory records from ${filePath}`);
      } else {
        console.log(`No inventory file found at ${filePath}, starting with empty inventory`);
        this.inventoryData = [];
      }
    } catch (error) {
      console.error(`Error loading inventory: ${error.message}`);
      this.inventoryData = [];
    }
  }

  /**
   * Check if this is the first scrape for an event
   * @param {string} eventId - The event ID
   * @returns {boolean} - True if this is the first scrape, false otherwise
   */
  isFirstScrape(eventId) {
    return !this.processedEvents[eventId];
  }
  
  /**
   * Mark an event as processed
   * @param {string} eventId - The event ID
   */
  markEventAsProcessed(eventId) {
    this.processedEvents[eventId] = { 
      firstProcessed: new Date().toISOString(),
      lastUpdated: new Date().toISOString()
    };
    saveProcessedEvents(this.processedEvents);
  }
  
  /**
   * Update the lastUpdated timestamp for an event
   * @param {string} eventId - The event ID
   */
  updateEventTimestamp(eventId) {
    if (this.processedEvents[eventId]) {
      this.processedEvents[eventId].lastUpdated = new Date().toISOString();
      saveProcessedEvents(this.processedEvents);
    }
  }

  /**
   * Save inventory data to CSV file
   * Generates a new CSV file for each scrape
   * @param {string} filePath - Path to save the CSV file
   * @param {string} eventId - The event ID
   */
  saveInventory(filePath = DEFAULT_INVENTORY_FILE, eventId = null) {
    try {
      const formattedData = this.inventoryData.map(record => formatInventoryForExport(record));
      
      // Generate event-specific file path without timestamp
      const eventFilePath = path.join(DATA_DIR, `event_${eventId}.csv`);
      
      // Save to both the event-specific file and the default file
      saveInventoryToCSV(formattedData, eventFilePath);
      saveInventoryToCSV(formattedData, filePath);
      
      console.log(`Saved ${this.inventoryData.length} inventory records to ${eventFilePath}`);
      
      // Update the event's last processed timestamp
      if (eventId) {
        this.processedEvents[eventId] = {
          firstProcessed: this.processedEvents[eventId]?.firstProcessed || new Date().toISOString(),
          lastUpdated: new Date().toISOString()
        };
        saveProcessedEvents(this.processedEvents);
      }
      
      return true;
    } catch (error) {
      console.error(`Error saving inventory: ${error.message}`);
      return false;
    }
  }

  /**
   * Get all inventory records
   */
  getAllInventory() {
    return this.inventoryData;
  }

  /**
   * Get inventory by ID
   */
  getInventoryById(inventoryId) {
    return this.inventoryData.find(record => record.inventory_id === inventoryId);
  }

  /**
   * Add new inventory record
   * @param {Object} record - The inventory record to add
   * @param {boolean} isNewEvent - Whether this is a new event being added
   */
  addInventory(record, isNewEvent = false) {
    try {
      // Validate and fix the record
      const validatedRecord = validateAndFixInventoryRecord(record);
      
      // Check if record with this ID already exists
      const existingIndex = this.inventoryData.findIndex(
        r => r.inventory_id === validatedRecord.inventory_id
      );
      
      if (existingIndex >= 0) {
        return { success: false, message: 'Inventory ID already exists' };
      }
      
      // Add the record
      this.inventoryData.push(validatedRecord);
      
      // Save to file only if this is a new event or we don't have event context
      const eventId = validatedRecord.event_id;
      this.saveInventory(DEFAULT_INVENTORY_FILE, isNewEvent ? eventId : null);
      
      return { 
        success: true, 
        message: 'Inventory added successfully', 
        record: validatedRecord 
      };
    } catch (error) {
      return { success: false, message: error.message };
    }
  }

  /**
   * Add inventory records in bulk
   * @param {Array} records - Array of inventory records to add
   * @param {string} eventId - The event ID these records belong to
   */
  addBulkInventory(records, eventId) {
    try {
      if (!records || !records.length) {
        return { success: false, message: 'No records provided' };
      }
      
      // Validate all records
      let validatedRecords = records.map(record => validateAndFixInventoryRecord(record));
      
      // Filter out null records (insufficient seats)
      validatedRecords = validatedRecords.filter(record => record !== null);
      
      if (validatedRecords.length === 0) {
        return { success: false, message: 'No valid records after filtering (all had insufficient seats)' };
      }
      
      // Add the records
      validatedRecords.forEach(record => {
        const existingIndex = this.inventoryData.findIndex(
          r => r.inventory_id === record.inventory_id
        );
        
        if (existingIndex === -1) {
          this.inventoryData.push(record);
        }
      });
      
      // Generate event-specific CSV filename without timestamp
      const eventCsvPath = path.join(DATA_DIR, `event_${eventId}.csv`);
      
      // Save to both the event-specific file and the default file
      const formattedData = validatedRecords.map(record => formatInventoryForExport(record));
      saveInventoryToCSV(formattedData, eventCsvPath);
      this.saveInventory(DEFAULT_INVENTORY_FILE, eventId);
      
      // Update the event's last processed timestamp
      this.processedEvents[eventId] = {
        firstProcessed: this.processedEvents[eventId]?.firstProcessed || new Date().toISOString(),
        lastUpdated: new Date().toISOString()
      };
      saveProcessedEvents(this.processedEvents);
      
      return { 
        success: true, 
        message: `Added ${validatedRecords.length} records for event ${eventId}`,
        csvPath: eventCsvPath
      };
    } catch (error) {
      return { success: false, message: error.message };
    }
  }

  /**
   * Update an existing inventory record
   */
  updateInventory(inventoryId, updates) {
    try {
      // Find the record
      const index = this.inventoryData.findIndex(
        record => record.inventory_id === inventoryId
      );
      
      if (index === -1) {
        return { success: false, message: 'Inventory record not found' };
      }
      
      // Get the existing record
      const existingRecord = this.inventoryData[index];
      
      // Apply updates and validate
      const updatedRecord = validateAndFixInventoryRecord({
        ...existingRecord,
        ...updates
      });
      
      // Update the record
      this.inventoryData[index] = updatedRecord;
      
      // Update the event timestamp but don't regenerate CSV
      if (updatedRecord.event_id) {
        this.updateEventTimestamp(updatedRecord.event_id);
      }
      
      return { 
        success: true, 
        message: 'Inventory updated successfully', 
        record: updatedRecord 
      };
    } catch (error) {
      return { success: false, message: error.message };
    }
  }

  /**
   * Delete an inventory record
   */
  deleteInventory(inventoryId) {
    try {
      const initialLength = this.inventoryData.length;
      this.inventoryData = this.inventoryData.filter(
        record => record.inventory_id !== inventoryId
      );
      
      if (this.inventoryData.length === initialLength) {
        return { success: false, message: 'Inventory record not found' };
      }
      
      // Save changes
      this.saveInventory();
      
      return { success: true, message: 'Inventory deleted successfully' };
    } catch (error) {
      return { success: false, message: error.message };
    }
  }

  /**
   * Check and fix seats for an inventory record
   */
  checkAndFixSeats(inventoryId) {
    try {
      // Find the record
      const index = this.inventoryData.findIndex(
        record => record.inventory_id === inventoryId
      );
      
      if (index === -1) {
        return { success: false, message: 'Inventory record not found' };
      }
      
      const record = this.inventoryData[index];
      
      // Validate the seats
      const validation = validateConsecutiveSeats(record.seats);
      
      if (validation.valid) {
        return { 
          success: true, 
          message: 'Seat numbers are already consecutive',
          validation
        };
      }
      
      // Fix the seats
      const updatedRecord = {
        ...record,
        seats: validation.fixedSeats
      };
      
      // Validate and update related fields
      const validatedRecord = validateAndFixInventoryRecord(updatedRecord);
      
      // Update the record
      this.inventoryData[index] = validatedRecord;
      
      // Save changes
      this.saveInventory();
      
      return { 
        success: true, 
        message: 'Seat numbers have been fixed',
        validation,
        record: validatedRecord
      };
    } catch (error) {
      return { success: false, message: error.message };
    }
  }
  
  /**
   * Export inventory to CSV file
   */
  exportInventory(filePath) {
    try {
      const formattedData = this.inventoryData.map(record => formatInventoryForExport(record));
      saveInventoryToCSV(formattedData, filePath);
      return { 
        success: true, 
        message: `Exported ${this.inventoryData.length} records to ${filePath}` 
      };
    } catch (error) {
      return { success: false, message: error.message };
    }
  }
  
  /**
   * Import inventory from CSV file
   */
  importInventory(filePath, replaceExisting = false) {
    try {
      if (!fs.existsSync(filePath)) {
        return { success: false, message: `File not found: ${filePath}` };
      }
      
      const importedData = readInventoryFromCSV(filePath);
      
      if (importedData.length === 0) {
        return { success: false, message: 'No records found in import file' };
      }
      
      // Validate all records
      const validatedData = importedData.map(record => validateAndFixInventoryRecord(record));
      
      if (replaceExisting) {
        // Replace current inventory
        this.inventoryData = validatedData;
      } else {
        // Merge with existing inventory, avoid duplicates by ID
        const existingIds = new Set(this.inventoryData.map(record => record.inventory_id));
        
        for (const record of validatedData) {
          if (!existingIds.has(record.inventory_id)) {
            this.inventoryData.push(record);
            existingIds.add(record.inventory_id);
          }
        }
      }
      
      // Save changes
      this.saveInventory();
      
      return { 
        success: true, 
        message: `Imported ${validatedData.length} records from ${filePath}`
      };
    } catch (error) {
      return { success: false, message: error.message };
    }
  }
}

// Create and export singleton instance
const inventoryController = new InventoryController();
export default inventoryController; 