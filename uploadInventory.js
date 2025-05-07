import SyncService from './services/syncService.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Company ID and API token - replace with environment variables in production
const COMPANY_ID = '702';
const API_TOKEN = 'OaJwtlUQiriMSrnGd7cauDWtIyAMnS363icaz-7t1vJ7bjIBe9ZFjBwgPYY1Q9eKV_Jt';

async function uploadInventoryCsv() {
  try {
    // Initialize the service
    const syncService = new SyncService(COMPANY_ID, API_TOKEN);
    
    // Path to the inventory CSV file
    const inventoryCsvPath = path.join(__dirname, "data", "blank_csv.csv");
    
    // Check if file exists
    if (!fs.existsSync(inventoryCsvPath)) {
      console.error('Inventory CSV file not found at:', inventoryCsvPath);
      return;
    }
    
    console.log('Uploading inventory CSV file to Sync...');
    const uploadResult = await syncService.uploadCsvToSync(inventoryCsvPath);
    console.log('Upload result:', uploadResult);
    
    console.log('Upload completed successfully!');
  } catch (error) {
    console.error('Upload failed:', error.message);
  }
}

// Run the upload
uploadInventoryCsv(); 