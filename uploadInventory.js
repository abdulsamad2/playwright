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
export function ensureBlankCsvExists(headersArray) {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
  // Determine which headers to use
  const headersToUse = Array.isArray(headersArray) && headersArray.length > 0 
    ? headersArray 
    : ['sku', 'quantity', 'price'];

  // Always create/overwrite the blank.csv with the specified or default headers
  // This ensures it has the correct headers for the current operation (e.g., clearing sync)
  const csvContent = headersToUse.join(',') + '\n';
  fs.writeFileSync(BLANK_CSV_PATH, csvContent);
  console.log(`Blank CSV ensured at ${BLANK_CSV_PATH} with headers: ${headersToUse.join(', ')}`);
}



