// import required modules
import path from 'path';
import fs from 'fs/promises';
import nodeFs from 'fs';
import moment from 'moment';
import SyncService from '../services/syncService.js';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

// Constants for CSV upload
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const DATA_DIR = path.join(__dirname, '..', 'data');
const COMPANY_ID = '702';
const API_TOKEN = 'OaJwtlUQiriMSrnGd7cauDWtIyAMnS363icaz-7t1vJ7bjIBe9ZFjBwgPYY1Q9eKV_Jt';

export async function runCsvUploadCycle() {
    try {
      console.log(`[${new Date().toISOString()}] Starting CSV upload cycle...`);
      
      // Force generation of combined CSV before upload
      try {
        const inventoryController = await import('../controllers/inventoryController.js');
        const result = inventoryController.forceCombinedCSVGeneration();
        
        if (result.success) {
          console.log(`[${new Date().toISOString()}] Generated fresh combined CSV: ${result.recordCount} records from ${result.eventCount} events`);
        } else {
          console.log(`[${new Date().toISOString()}] Failed to generate combined CSV: ${result.message}`);
        }
      } catch (generationError) {
        console.error(`[${new Date().toISOString()}] Error generating combined CSV: ${generationError.message}`);
      }

      // Create the path to the all_events_combined.csv file
      const allEventsCsvPath = path.join(DATA_DIR, 'all_events_combined.csv');
      
      // Check if file exists
      if (!nodeFs.existsSync(allEventsCsvPath)) {
        console.error(`[${new Date().toISOString()}] CSV upload skipped: File not found at ${allEventsCsvPath}`);
        return;
      }
      
      // Get file stats for logging
      try {
        const fileStats = await fs.stat(allEventsCsvPath);
        const fileSizeMB = (fileStats.size / (1024 * 1024)).toFixed(2);
        console.log(`[${new Date().toISOString()}] Uploading all_events_combined.csv (${fileSizeMB} MB) to Sync...`);
      } catch (statError) {
        console.log(`[${new Date().toISOString()}] Uploading all_events_combined.csv to Sync...`);
      }
      
      // Initialize the SyncService
      const syncService = new SyncService(COMPANY_ID, API_TOKEN);
      
      // Upload the file
      try {
        const uploadStartTime = moment();
        const result = await syncService.uploadCsvToSync(allEventsCsvPath);
        const uploadDuration = moment().diff(uploadStartTime, 'seconds');
        
        console.log(`[${new Date().toISOString()}] Upload complete in ${uploadDuration}s. Result:`, result);
        return result;
      } catch (error) {
        console.error(`[${new Date().toISOString()}] Upload failed: ${error.message}`);
        return { success: false, message: error.message };
      }
    } catch (error) {
      console.error(`[${new Date().toISOString()}] CSV upload cycle error: ${error.message}`);
      return { success: false, message: error.message };
    }
} 