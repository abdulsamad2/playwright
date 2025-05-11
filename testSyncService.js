import SyncService from './services/syncService.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Company ID and API token
const COMPANY_ID = '702';
const API_TOKEN = 'OaJwtlUQiriMSrnGd7cauDWtIyAMnS363icaz-7t1vJ7bjIBe9ZFjBwgPYY1Q9eKV_Jt';

// 6 minutes in milliseconds
const UPLOAD_INTERVAL = 6 * 60 * 1000;

// Path to the combined events CSV file
const dataDir = path.join(__dirname, 'data');
const combinedCsvPath = path.join(dataDir, "inventory.csv");

// Initialize the Sync service
const syncService = new SyncService(COMPANY_ID, API_TOKEN);

/**
 * Simple function to upload the combined CSV file
 */
async function uploadCombinedCsv() {
  console.log(`[${new Date().toISOString()}] Uploading all_events_combined.csv to Sync...`);
  
  try {
    // Check if file exists
    if (!fs.existsSync(combinedCsvPath)) {
      console.error(`File not found: ${combinedCsvPath}`);
      return;
    }
    
    // Upload the file
    const result = await syncService.uploadCsvToSync(combinedCsvPath);
    console.log(`Upload complete. Result:`, result);
  } catch (error) {
    console.error(`Upload failed: ${error.message}`);
  }
}

/**
 * Run a continuous loop to upload the file every 6 minutes
 */
async function startContinuousUpload() {
  console.log('Starting continuous upload of all_events_combined.csv');
  console.log(`Will upload every ${UPLOAD_INTERVAL/60000} minutes`);
  
  // Initial upload
  await uploadCombinedCsv();
  
  // Set up the interval for continuous uploads
  setInterval(uploadCombinedCsv, UPLOAD_INTERVAL);
}

// Start the continuous upload process
startContinuousUpload().catch(error => {
  console.error('Error:', error);
}); 