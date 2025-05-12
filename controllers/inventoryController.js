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
const COMBINED_EVENTS_FILE = path.join(DATA_DIR, 'all_events_combined.csv');
const SCRAPE_CYCLE_FILE = path.join(DATA_DIR, 'scrape_cycle.json');

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

// Track the current scrape cycle
const getScrapeCycle = () => {
  if (fs.existsSync(SCRAPE_CYCLE_FILE)) {
    try {
      return JSON.parse(fs.readFileSync(SCRAPE_CYCLE_FILE, 'utf8'));
    } catch (error) {
      console.error(`Error reading scrape cycle file: ${error.message}`);
      return { 
        currentCycle: 1,
        events: {},
        status: 'in_progress',
        startedAt: new Date().toISOString()
      };
    }
  }
  return { 
    currentCycle: 1,
    events: {},
    status: 'in_progress',
    startedAt: new Date().toISOString()
  };
};

const saveScrapeCycle = (cycleData) => {
  try {
    fs.writeFileSync(SCRAPE_CYCLE_FILE, JSON.stringify(cycleData, null, 2));
    return true;
  } catch (error) {
    console.error(`Error saving scrape cycle file: ${error.message}`);
    return false;
  }
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
    this.scrapeCycle = getScrapeCycle();
    this.loadInventory();
  }

  /**
   * Start a new scrape cycle
   * @param {Array} eventIds - Array of event IDs that will be part of this cycle
   */
  startScrapeCycle(eventIds) {
    if (!Array.isArray(eventIds) || eventIds.length === 0) {
      return { success: false, message: 'Please provide an array of event IDs' };
    }

    // Delete previous combined file if it exists
    if (fs.existsSync(COMBINED_EVENTS_FILE)) {
      fs.unlinkSync(COMBINED_EVENTS_FILE);
      console.log(`Deleted previous combined events file for new scrape cycle`);
    }

    // Create a new cycle object
    const events = {};
    eventIds.forEach(id => {
      events[id] = { status: 'pending', lastUpdated: null };
    });

    this.scrapeCycle = {
      currentCycle: this.scrapeCycle.currentCycle + 1,
      events,
      status: 'in_progress',
      startedAt: new Date().toISOString(),
      completedAt: null
    };

    saveScrapeCycle(this.scrapeCycle);

    return {
      success: true,
      message: `Started new scrape cycle #${this.scrapeCycle.currentCycle} with ${eventIds.length} events`
    };
  }

  /**
   * Mark an event as scraped in the current cycle
   * @param {string} eventId - The event ID that was scraped
   * @param {boolean} hadChanges - Whether there were any changes to the event data
   */
  markEventScraped(eventId, hadChanges = true) {
    if (!this.scrapeCycle.events[eventId]) {
      // Add this event to the cycle if it wasn't initially included
      this.scrapeCycle.events[eventId] = { 
        status: 'completed', 
        lastUpdated: new Date().toISOString(),
        hadChanges: hadChanges
      };
    } else {
      // Update existing event status
      this.scrapeCycle.events[eventId] = {
        status: 'completed',
        lastUpdated: new Date().toISOString(),
        hadChanges: hadChanges
      };
    }

    // Check if all events are scraped
    const allCompleted = Object.values(this.scrapeCycle.events)
      .every(event => event.status === 'completed');

    if (allCompleted) {
      this.scrapeCycle.status = 'completed';
      this.scrapeCycle.completedAt = new Date().toISOString();
      
      // Generate the combined CSV file
      const result = this.generateCombinedEventsCSV();
      console.log(`Scrape cycle #${this.scrapeCycle.currentCycle} completed - ${result.message}`);
    }

    saveScrapeCycle(this.scrapeCycle);

    return {
      success: true,
      message: `Event ${eventId} marked as scraped${hadChanges ? '' : ' (no changes)'}`,
      cycleComplete: allCompleted
    };
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
   * Delete old CSV files for a specific event
   * @param {string} eventId - The event ID
   */
  deleteOldCsvFiles(eventId) {
    try {
      const eventFilePath = path.join(DATA_DIR, `event_${eventId}.csv`);
      if (fs.existsSync(eventFilePath)) {
        // Check if file contains data from other events before deleting
        try {
          const existingData = readInventoryFromCSV(eventFilePath);
          if (existingData.length > 0) {
            // Check how many different event names exist in the file
            const eventNames = new Set(existingData.map(record => record.event_name).filter(Boolean));
            if (eventNames.size > 1) {
              console.log(`WARNING: CSV file for event ${eventId} contains ${eventNames.size} different event names: ${[...eventNames].join(', ')}`);
            }
          }
        } catch (e) {
          // Ignore errors when checking existing file
        }
        
        fs.unlinkSync(eventFilePath);
        console.log(`Deleted old CSV file: ${eventFilePath}`);
      }
    } catch (error) {
      console.error(`Error deleting old CSV file: ${error.message}`);
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
      // Format all inventory for export
      const allFormattedData = this.inventoryData.map(record => formatInventoryForExport(record));
      
      // Save all inventory to the default file
      saveInventoryToCSV(allFormattedData, filePath);
      
      // If an event ID is provided, generate an event-specific file
      if (eventId) {
        // Get only records that belong to this specific event
        const eventSpecificData = this.inventoryData
          .filter(record => record.source_event_id === eventId)
          .map(record => formatInventoryForExport(record));
        
        // Generate event-specific file path
        const eventFilePath = path.join(DATA_DIR, `event_${eventId}.csv`);
        
        // Save event-specific data to event-specific file
        saveInventoryToCSV(eventSpecificData, eventFilePath);
        
        console.log(`Saved ${eventSpecificData.length} records specific to event ${eventId} to ${eventFilePath}`);
      }
      
      console.log(`Saved ${this.inventoryData.length} total inventory records to ${filePath}`);
      
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
   * @param {string} eventId - The event ID these records belong to (external Event_ID)
   */
  addBulkInventory(records, eventId) {
    try {
      if (!records || !records.length) {
        return { success: false, message: 'No records provided' };
      }
      
      // First, ensure all records are for the same event (compare event_name)
      const eventName = records[0]?.event_name || '';
      if (eventName) {
        // Check if all records have the same event name to prevent mixing
        const differentEventNames = records.filter(r => 
          r.event_name && r.event_name !== eventName
        );
        
        if (differentEventNames.length > 0) {
          console.warn(`Warning: ${differentEventNames.length} records have different event names than the primary event "${eventName}"`);
        }
      }
      
      // Validate all records
      let validatedRecords = records.map(record => validateAndFixInventoryRecord(record));
      
      // Filter out null records (insufficient seats)
      validatedRecords = validatedRecords.filter(record => record !== null);
      
      if (validatedRecords.length === 0) {
        return { success: false, message: 'No valid records after filtering (all had insufficient seats)' };
      }
      
      // Create a Set to track unique combinations
      const uniqueKeys = new Set();
      const uniqueRecords = [];
      
      // Filter out duplicates based on section, row, and seats
      validatedRecords.forEach(record => {
        const uniqueKey = `${record.section}-${record.row}-${record.seats}`;
        if (!uniqueKeys.has(uniqueKey)) {
          uniqueKeys.add(uniqueKey);
          // Add the source eventId as a property to track which event the record belongs to
          record.source_event_id = eventId;
          record.original_event_name = record.event_name; // Preserve original event name
          uniqueRecords.push(record);
        }
      });

      // Get existing records for this event
      const existingRecords = this.inventoryData.filter(record => 
        record.source_event_id === eventId
      );

      // Create maps for quick lookup
      const existingMap = new Map(
        existingRecords.map(record => [
          `${record.section}-${record.row}-${record.seats}`,
          record
        ])
      );
      
      const newMap = new Map(
        uniqueRecords.map(record => [
          `${record.section}-${record.row}-${record.seats}`,
          record
        ])
      );

      // Find records to delete (exist in old but not in new)
      const recordsToDelete = existingRecords.filter(record => {
        const key = `${record.section}-${record.row}-${record.seats}`;
        return !newMap.has(key);
      });

      // Find records to add (exist in new but not in old)
      const recordsToAdd = uniqueRecords.filter(record => {
        const key = `${record.section}-${record.row}-${record.seats}`;
        return !existingMap.has(key);
      });

      // Find records that need updating (exist in both but have different values)
      const recordsToUpdate = uniqueRecords.filter(record => {
        const key = `${record.section}-${record.row}-${record.seats}`;
        const existingRecord = existingMap.get(key);
        if (!existingRecord) return false;

        // Compare relevant fields
        return JSON.stringify({
          price: record.price,
          quantity: record.quantity,
          status: record.status
        }) !== JSON.stringify({
          price: existingRecord.price,
          quantity: existingRecord.quantity,
          status: existingRecord.status
        });
      });

      // Remove records that need to be deleted or updated
      this.inventoryData = this.inventoryData.filter(record => {
        const key = `${record.section}-${record.row}-${record.seats}`;
        return !recordsToDelete.some(r => 
          `${r.section}-${r.row}-${r.seats}` === key
        ) && !recordsToUpdate.some(r => 
          `${r.section}-${r.row}-${r.seats}` === key
        );
      });

      // Add new and updated records
      this.inventoryData.push(...recordsToAdd, ...recordsToUpdate);
      
      // Generate event-specific CSV filename
      const eventCsvPath = path.join(DATA_DIR, `event_${eventId}.csv`);
      
      // Only include records from this specific event in the CSV
      const formattedData = this.inventoryData
        .filter(record => record.source_event_id === eventId)
        .map(record => formatInventoryForExport(record));
      
      // No matter if there were changes or not, generate a new CSV file for this event
      // This ensures the file timestamp is updated for the merge cycle
      
      // Always create/update the event CSV file, even if no changes detected
      // This is important for the merge cycle to pick up this event
      if (fs.existsSync(eventCsvPath)) {
        fs.unlinkSync(eventCsvPath);
      }
      
      // Write event-specific data to the event CSV
      saveInventoryToCSV(formattedData, eventCsvPath);
      
      // Only update the main inventory file if there were changes
      if (recordsToAdd.length > 0 || recordsToUpdate.length > 0 || recordsToDelete.length > 0) {
        const allFormattedData = this.inventoryData.map(record => formatInventoryForExport(record));
        saveInventoryToCSV(allFormattedData, DEFAULT_INVENTORY_FILE);
      }
      
      // Update the event's last processed timestamp
      this.processedEvents[eventId] = {
        firstProcessed: this.processedEvents[eventId]?.firstProcessed || new Date().toISOString(),
        lastUpdated: new Date().toISOString()
      };
      saveProcessedEvents(this.processedEvents);
      
      // Check if there were any changes
      const noChanges = recordsToAdd.length === 0 && recordsToUpdate.length === 0 && recordsToDelete.length === 0;
      
      // Mark this event as scraped in the current cycle
      this.markEventScraped(eventId, !noChanges);
      
      return { 
        success: true, 
        message: noChanges 
          ? `No changes detected for event ${eventId}, file timestamp updated for merge cycle` 
          : `Processed ${uniqueRecords.length} records for event ${eventId}: ${recordsToAdd.length} added, ${recordsToUpdate.length} updated, ${recordsToDelete.length} deleted`,
        csvPath: eventCsvPath,
        noChanges: noChanges,
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

  /**
   * Clean up cross-contaminated event data
   * Checks all event CSV files and ensures each only contains records
   * for the event it's named after
   */
  cleanupEventCsvFiles() {
    try {
      // Get all event_*.csv files in the data directory
      const files = fs.readdirSync(DATA_DIR).filter(file => 
        file.startsWith('event_') && file.endsWith('.csv')
      );
      
      let cleanedFiles = 0;
      let problemsFound = 0;
      
      for (const file of files) {
        // Extract event ID from filename
        const eventId = file.replace('event_', '').replace('.csv', '');
        const filePath = path.join(DATA_DIR, file);
        
        try {
          // Read file data
          const data = readInventoryFromCSV(filePath);
          if (data.length === 0) continue;
          
          // Check for event name contamination
          const eventNames = new Set(data.map(record => record.event_name).filter(Boolean));
          
          if (eventNames.size > 1) {
            console.log(`Fixing: CSV file for event ${eventId} contains ${eventNames.size} different event names: ${[...eventNames].join(', ')}`);
            problemsFound++;
            
            // Find legitimate records for this event
            const eventData = data.filter(record => 
              record.source_event_id === eventId || 
              (record.event_id === eventId && !record.source_event_id)
            );
            
            if (eventData.length > 0) {
              // Rewrite file with only the proper records
              saveInventoryToCSV(eventData, filePath);
              console.log(`Fixed event ${eventId} CSV - removed ${data.length - eventData.length} records from other events`);
              cleanedFiles++;
            } else {
              console.log(`No legitimate records found for event ${eventId} - file may need to be regenerated`);
            }
          }
        } catch (error) {
          console.error(`Error processing ${file}: ${error.message}`);
        }
      }
      
      return {
        success: true,
        filesChecked: files.length,
        problemsFound,
        cleanedFiles,
        message: `Checked ${files.length} event files, fixed ${cleanedFiles} contaminated files`
      };
    } catch (error) {
      return {
        success: false,
        message: `Error cleaning up event CSV files: ${error.message}`
      };
    }
  }

  /**
   * Generate a combined CSV file with all events' data
   * @param {boolean} isNewCycle - Whether this is a new scrape cycle
   */
  generateCombinedEventsCSV(isNewCycle = false) {
    try {
      // Get all event CSV files from the data directory
      const eventFiles = fs.readdirSync(DATA_DIR)
        .filter(file => file.startsWith('event_') && file.endsWith('.csv'))
        .map(file => {
          const filePath = path.join(DATA_DIR, file);
          // Get file stats to check modification time
          const stats = fs.statSync(filePath);
          // Extract eventId from filename
          const eventId = file.replace('event_', '').replace('.csv', '');
          
          // Check if this event had changes in the current cycle
          const hadChanges = this.scrapeCycle.events[eventId]?.hadChanges === true;
          
          return {
            path: filePath,
            fileName: file,
            eventId: eventId,
            lastModified: stats.mtime,
            size: stats.size,
            hadChanges: hadChanges
          };
        });
      
      console.log(`Found ${eventFiles.length} total event CSV files`);
      
      if (eventFiles.length === 0) {
        return {
          success: false,
          message: 'No event CSV files found to combine',
          filePath: null
        };
      }
      
      // Only include files that have been recently updated (within last 6 minutes)
      // This ensures only data from the current merge cycle is included
      const sixMinutesAgo = new Date(Date.now() - 6 * 60 * 1000);
      const recentlyUpdatedFiles = eventFiles.filter(file => 
        file.lastModified > sixMinutesAgo && file.size > 0
      );
      
      // Count events with and without changes
      const eventsWithChanges = recentlyUpdatedFiles.filter(file => file.hadChanges).length;
      const eventsWithoutChanges = recentlyUpdatedFiles.length - eventsWithChanges;
      
      console.log(`Found ${recentlyUpdatedFiles.length} recently updated event CSV files (within last 6 minutes)`);
      console.log(`Events with changes: ${eventsWithChanges}, Events without changes (timestamp only): ${eventsWithoutChanges}`);
      
      if (recentlyUpdatedFiles.length === 0 && !isNewCycle) {
        return {
          success: false,
          message: 'No recently updated event CSV files found to combine',
          filePath: null
        };
      }
      
      // Load existing combined file data if it exists and this is not a new cycle
      let existingRecords = [];
      let existingRecordsMap = new Map(); // Map to quickly look up existing records
      
      if (!isNewCycle && fs.existsSync(COMBINED_EVENTS_FILE)) {
        try {
          existingRecords = readInventoryFromCSV(COMBINED_EVENTS_FILE);
          console.log(`Loaded ${existingRecords.length} existing records from combined file`);
          
          // Create a map of existing records using section-row-seats as key
          existingRecords.forEach(record => {
            const key = `${record.section}-${record.row}-${record.seats}`;
            existingRecordsMap.set(key, record);
          });
        } catch (error) {
          console.error(`Error reading existing combined file: ${error.message}`);
          // Continue with empty existingRecords if there was an error
        }
      } else if (isNewCycle && fs.existsSync(COMBINED_EVENTS_FILE)) {
        fs.unlinkSync(COMBINED_EVENTS_FILE);
        console.log(`Deleted previous combined events file for new scrape cycle`);
      }
      
      // Collect all records from recently updated event files
      let allRecords = [];
      for (const fileInfo of recentlyUpdatedFiles) {
        try {
          const data = readInventoryFromCSV(fileInfo.path);
          console.log(`Loaded ${data.length} records from ${fileInfo.fileName} (last modified: ${fileInfo.lastModified.toISOString()})`);
          allRecords = allRecords.concat(data);
        } catch (error) {
          console.error(`Error reading file ${fileInfo.path}: ${error.message}`);
          // Continue with next file if there was an error
        }
      }
      
      console.log(`Loaded a total of ${allRecords.length} records from all recently updated event files`);
      
      // If no new records in this cycle, keep existing combined file
      if (allRecords.length === 0 && existingRecords.length > 0) {
        return {
          success: true,
          message: `No new records to add. Kept existing combined CSV with ${existingRecords.length} records.`,
          filePath: COMBINED_EVENTS_FILE,
          noChanges: true
        };
      }
      
      // Create a Map to track processed records and detect duplicates
      const processedKeys = new Map();
      const finalRecords = [];
      
      // Process each record
      allRecords.forEach(record => {
        // Create a unique key for this record
        const key = `${record.section}-${record.row}-${record.seats}`;
        
        // Skip duplicates within the new data
        if (processedKeys.has(key)) {
          // If we've seen this key before, compare record details to keep the most recent one
          const existingRecord = processedKeys.get(key);
          
          // Only replace if there's a actual data difference (beyond just inventory_id)
          const isNewer = new Date(record.in_hand_date) > new Date(existingRecord.in_hand_date);
          if (isNewer) {
            // Remove the old record from finalRecords
            const index = finalRecords.findIndex(r => 
              r.section === existingRecord.section && 
              r.row === existingRecord.row && 
              r.seats === existingRecord.seats
            );
            if (index !== -1) {
              finalRecords.splice(index, 1);
            }
            // Update the map with the newer record
            processedKeys.set(key, record);
            finalRecords.push(record);
          }
          return;
        }
        
        // Check if this record exists in the combined file
        if (existingRecordsMap.has(key)) {
          const existingRecord = existingRecordsMap.get(key);
          
          // Compare record fields to detect changes (excluding inventory_id)
          const fieldsToCompare = [
            'event_id', 'quantity', 'public_notes', 'list_price', 
            'face_price', 'taxed_cost', 'cost', 'hide_seats', 'in_hand',
            'in_hand_date', 'stock_type', 'split_type', 'custom_split'
          ];
          
          // Check if any field has changed
          const hasChanged = fieldsToCompare.some(field => 
            record[field] !== existingRecord[field]
          );
          
          if (hasChanged) {
            // If record has changed, use a new inventory_id
            console.log(`Record changed: ${key} - Using new inventory_id`);
            // Record is already new, so use its inventory_id
          } else {
            // If record is unchanged, preserve the existing inventory_id
            console.log(`Record unchanged: ${key} - Preserving inventory_id`);
            record.inventory_id = existingRecord.inventory_id;
          }
        }
        
        // Add record to processed map and final records
        processedKeys.set(key, record);
        finalRecords.push(record);
      });
      
      // Add all existing records that weren't in any of the updated files
      let preservedRecordsCount = 0;
      existingRecords.forEach(record => {
        const key = `${record.section}-${record.row}-${record.seats}`;
        if (!processedKeys.has(key)) {
          // Add this existing record since it wasn't updated
          finalRecords.push(record);
          processedKeys.set(key, record);
          preservedRecordsCount++;
        }
      });
      
      if (preservedRecordsCount > 0) {
        console.log(`Preserved ${preservedRecordsCount} records from existing combined file that weren't in updated event files`);
      }
      
      console.log(`Final record count after processing: ${finalRecords.length}`);
      
      // Format for export
      const formattedData = finalRecords.map(record => {
        // Ensure event_id is always present before formatting
        if (!record.event_id && record.mapping_id) {
          record.event_id = record.mapping_id;
          console.log(`Fixing record: Added missing event_id=${record.mapping_id} based on mapping_id for ${record.section}-${record.row}`);
        } else if (!record.mapping_id && record.event_id) {
          record.mapping_id = record.event_id;
          console.log(`Fixing record: Added missing mapping_id=${record.event_id} based on event_id for ${record.section}-${record.row}`);
        } else if (!record.event_id && !record.mapping_id && record.source_event_id) {
          // Use source_event_id as a fallback
          record.event_id = record.source_event_id;
          record.mapping_id = record.source_event_id;
          console.log(`Fixing record: Used source_event_id=${record.source_event_id} for missing event_id and mapping_id for ${record.section}-${record.row}`);
        }
        
        // Final check to ensure neither is empty
        if (!record.event_id || !record.mapping_id) {
          console.warn(`WARNING: Record may have missing ID fields: section=${record.section}, row=${record.row}, event_id=${record.event_id || 'MISSING'}, mapping_id=${record.mapping_id || 'MISSING'}`);
        }
        
        return formatInventoryForExport(record);
      });
      
      // Verify fields are correctly included in formatted data
      if (formattedData.length > 0) {
        const firstRecord = formattedData[0];
        console.log(`CSV Export Verification: First record contains event_id=${firstRecord.event_id || 'MISSING'}, mapping_id=${firstRecord.mapping_id || 'MISSING'}`);
        
        // Count records with missing IDs for reporting
        const missingEventId = formattedData.filter(r => !r.event_id).length;
        const missingMappingId = formattedData.filter(r => !r.mapping_id).length;
        
        if (missingEventId > 0 || missingMappingId > 0) {
          console.warn(`WARNING: Found ${missingEventId} records without event_id and ${missingMappingId} records without mapping_id`);
        }
      }
      
      // Save to combined file
      saveInventoryToCSV(formattedData, COMBINED_EVENTS_FILE);
      
      // Verify the saved file has the correct headers
      try {
        const fs = require('fs');
        const fileContent = fs.readFileSync(COMBINED_EVENTS_FILE, 'utf8');
        const firstLine = fileContent.split('\n')[0];
        
        // Check if the header line contains event_id and mapping_id
        const hasEventId = firstLine.includes('event_id');
        const hasMappingId = firstLine.includes('mapping_id');
        
        console.log(`VERIFICATION: Combined CSV file headers - event_id=${hasEventId ? 'PRESENT' : 'MISSING'}, mapping_id=${hasMappingId ? 'PRESENT' : 'MISSING'}`);
        
        if (!hasEventId || !hasMappingId) {
          console.error(`ERROR: Combined CSV file is missing required fields in headers: ${!hasEventId ? 'event_id ' : ''}${!hasMappingId ? 'mapping_id' : ''}`);
        }
      } catch (verifyError) {
        console.error(`Error verifying combined CSV file: ${verifyError.message}`);
      }
      
      return {
        success: true,
        message: `Combined ${recentlyUpdatedFiles.length} recently updated event files into a single CSV with ${finalRecords.length} records`,
        filePath: COMBINED_EVENTS_FILE
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
}

// Create and export singleton instance
const inventoryController = new InventoryController();

// Add cleanup method to be accessible from outside
export const cleanupEventCsvFiles = () => {
  return inventoryController.cleanupEventCsvFiles();
};

// Export method to generate combined events CSV
export const generateCombinedEventsCSV = (isNewCycle = false) => {
  return inventoryController.generateCombinedEventsCSV(isNewCycle);
};

// Export methods to manage scrape cycles
export const startScrapeCycle = (eventIds) => {
  return inventoryController.startScrapeCycle(eventIds);
};

export const markEventScraped = (eventId) => {
  return inventoryController.markEventScraped(eventId);
};

export default inventoryController; 