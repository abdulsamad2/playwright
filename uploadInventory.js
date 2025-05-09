import SyncService from './services/syncService.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import inventoryController from './controllers/inventoryController.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Company ID and API token - replace with environment variables in production
const COMPANY_ID = '702';
const API_TOKEN = 'OaJwtlUQiriMSrnGd7cauDWtIyAMnS363icaz-7t1vJ7bjIBe9ZFjBwgPYY1Q9eKV_Jt';

// Directory for CSV files
const DATA_DIR = path.join(__dirname, 'data');
const BLANK_CSV_PATH = path.join(DATA_DIR, 'blank.csv');

// Ensure blank CSV exists
function ensureBlankCsvExists() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
  
  if (!fs.existsSync(BLANK_CSV_PATH)) {
    // Create empty CSV with headers
    fs.writeFileSync(BLANK_CSV_PATH, 'inventory_id,event_name,venue_name,event_date,event_id,quantity,section,row,seats,barcodes,internal_notes,public_notes,tags,list_price,face_price,taxed_cost,cost,hide_seats,in_hand,in_hand_date,instant_transfer,files_available,split_type,custom_split,stock_type,zone,shown_quantity,passthrough,mapping_id\n');
    console.log('Created blank CSV file');
  }
}

// Get all event CSV files
function getEventCsvFiles() {
  try {
    return fs.readdirSync(DATA_DIR)
      .filter(file => file.startsWith('event_') && file.endsWith('.csv'))
      .map(file => path.join(DATA_DIR, file));
  } catch (error) {
    console.error('Error reading event CSV files:', error.message);
    return [];
  }
}

// Upload a single file
async function uploadFile(syncService, filePath) {
  try {
    console.log(`Uploading ${path.basename(filePath)} to Sync...`);
    const uploadResult = await syncService.uploadCsvToSync(filePath);
    console.log(`Upload result for ${path.basename(filePath)}:`, uploadResult);
    return true;
  } catch (error) {
    console.error(`Upload failed for ${path.basename(filePath)}:`, error.message);
    return false;
  }
}

// Main upload function
async function uploadInventoryCycle() {
  try {
    console.log('Starting inventory upload cycle at', new Date().toISOString());
    
    // Ensure blank CSV exists
    ensureBlankCsvExists();
    
    // Initialize the service
    const syncService = new SyncService(COMPANY_ID, API_TOKEN);
    
    // First upload the blank CSV
    await uploadFile(syncService, BLANK_CSV_PATH);
    
    // Wait 60 seconds before continuing with event files
    await new Promise(resolve => setTimeout(resolve, 1200000));
    
    // Get all event CSV files and upload them
    const eventFiles = getEventCsvFiles();
    console.log(`Found ${eventFiles.length} event CSV files to upload`);
    
    for (const filePath of eventFiles) {
      await uploadFile(syncService, filePath);
      // Add a small delay between uploads to avoid overwhelming the API
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
    
    console.log('Upload cycle completed successfully at', new Date().toISOString());
  } catch (error) {
    console.error('Error in upload cycle:', error.message);
  }
}

// Start the continuous upload process
async function startContinuousUpload() {
  try {
    // Run immediately on startup
    await uploadInventoryCycle();
    
    // Then repeat every 5 minutes
    const intervalId = setInterval(async () => {
      try {
        await uploadInventoryCycle();
      } catch (error) {
        console.error('Error in upload cycle interval:', error);
        // Continue running despite errors
      }
    }, 5 * 60 * 1000);
    
    // Handle process termination
    process.on('SIGINT', () => {
      console.log('Stopping continuous upload process...');
      clearInterval(intervalId);
      process.exit(0);
    });
    
    console.log('Continuous upload process started. Will run every 5 minutes.');
  } catch (error) {
    console.error('Fatal error in continuous upload process:', error);
    process.exit(1);
  }
}

// Run the continuous upload process
startContinuousUpload(); 