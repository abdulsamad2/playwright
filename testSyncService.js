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

/**
 * Create a sample CSV file for testing
 * @param {string} filePath - Path where the sample CSV will be saved
 */
// async function createSampleCsv(filePath) {
//   const csvContent = 
//     'sku,quantity,price\n';

//   fs.writeFileSync(filePath, csvContent);
//   console.log(`Blank CSV created at: ${filePath}`);
// }

/**
 * Test function for uploading a CSV file and then clearing inventory
 */
async function testSyncService() {
  try {
    // Initialize the service
    const syncService = new SyncService(COMPANY_ID, API_TOKEN);
    
    // Ensure data directory exists
    const dataDir = path.join(__dirname, 'data');
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }
    
    // Create sample CSV file
    const sampleCsvPath = path.join(dataDir, "blank_csv.csv");
   
    
    // 1. Test uploading the CSV
    console.log('STEP 1: Uploading CSV file to Sync...');
    const uploadResult = await syncService.uploadCsvToSync(sampleCsvPath);
    console.log('Upload result:', uploadResult);
    
    // Wait for a moment to let the upload process on the server side
    console.log('Waiting for 3 seconds...');
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    // 2. Test clearing inventory
    console.log('\nSTEP 2: Clearing all inventory...');
    // const clearResult = await syncService.clearAllInventory();
    // console.log('Clear result:', clearResult);
    
    // // Clean up
    // console.log('\nCleaning up test files...');
    // if (fs.existsSync(sampleCsvPath)) {
    //   fs.unlinkSync(sampleCsvPath);
    //   console.log(`Deleted: ${sampleCsvPath}`);
    // } else {
    //   console.log(`File not found: ${sampleCsvPath}`);
    // }
    
    console.log('\nTest completed successfully!');
  } catch (error) {
    console.error('Test failed:', error.message);
  }
}

// Run the test
testSyncService(); 