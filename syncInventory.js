#!/usr/bin/env node

import { program } from 'commander';
import SyncService from './services/syncService.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Check if data directory exists, create if not
const dataDir = path.join(__dirname, 'data');
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

// Get company ID and API token from environment variables or user input
const COMPANY_ID = process.env.SYNC_COMPANY_ID || '702';
const API_TOKEN = process.env.SYNC_API_TOKEN || 'OaJwtlUQiriMSrnGd7cauDWtIyAMnS363icaz-7t1vJ7bjIBe9ZFjBwgPYY1Q9eKV_Jt';

// Initialize the SyncService
const syncService = new SyncService(COMPANY_ID, API_TOKEN);

program
  .name('syncInventory')
  .description('Utility for managing inventory in Sync through CSV uploads')
  .version('1.0.0');

program
  .command('upload <csvFilePath>')
  .description('Upload a CSV file to Sync')
  .option('-z, --zipped', 'Indicate if the file is zipped', false)
  .action(async (csvFilePath, options) => {
    try {
      // Resolve the file path
      const resolvedPath = path.resolve(csvFilePath);
      console.log(`Uploading CSV file: ${resolvedPath}`);
      
      // Upload the CSV
      const result = await syncService.uploadCsvToSync(resolvedPath, options.zipped);
      
      console.log('Upload successful!');
      console.log(JSON.stringify(result, null, 2));
    } catch (error) {
      console.error('Error uploading CSV:', error.message);
      process.exit(1);
    }
  });

program
  .command('clear')
  .description('Clear all inventory by uploading a blank CSV')
  .action(async () => {
    try {
      console.log('Clearing all inventory...');
      
      // Clear inventory
      const result = await syncService.clearAllInventory();
      
      console.log('Inventory cleared successfully!');
      console.log(JSON.stringify(result, null, 2));
    } catch (error) {
      console.error('Error clearing inventory:', error.message);
      process.exit(1);
    }
  });

program
  .command('create-template <outputPath>')
  .description('Create a blank CSV template with the required headers')
  .action(async (outputPath) => {
    try {
      const headers = ['sku', 'quantity', 'price'];
      const resolvedPath = path.resolve(outputPath);
      
      await syncService.createBlankCsv(resolvedPath, headers);
      
      console.log(`Template CSV created at: ${resolvedPath}`);
    } catch (error) {
      console.error('Error creating template:', error.message);
      process.exit(1);
    }
  });

program.parse(); 