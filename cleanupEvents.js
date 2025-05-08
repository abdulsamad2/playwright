#!/usr/bin/env node

import { cleanupEventCsvFiles } from './controllers/inventoryController.js';
import path from 'path';
import fs from 'fs';

/**
 * Script to clean up cross-contaminated event CSV files
 * This fixes the issue where data from multiple events appears in a single event CSV file
 */

async function main() {
  console.log('🧹 Starting event CSV cleanup process...');
  
  try {
    // Run the cleanup process
    const result = cleanupEventCsvFiles();
    
    console.log('\n===== CLEANUP RESULTS =====');
    if (result.success) {
      console.log(`✅ Files Checked: ${result.filesChecked}`);
      console.log(`⚠️ Problems Found: ${result.problemsFound}`);
      console.log(`🛠️ Files Fixed: ${result.cleanedFiles}`);
      console.log(`\n${result.message}`);
      
      if (result.problemsFound === 0) {
        console.log('\n✨ All event files are clean - no mixed event data found!');
      } else if (result.cleanedFiles === result.problemsFound) {
        console.log('\n✅ Successfully fixed all contaminated event files!');
      } else {
        console.log(`\n⚠️ Fixed ${result.cleanedFiles} out of ${result.problemsFound} contaminated files.`);
        console.log('Some files may need to be regenerated - check the logs above for details.');
      }
    } else {
      console.error(`❌ Error: ${result.message}`);
    }
  } catch (error) {
    console.error(`❌ Unhandled error: ${error.message}`);
  }
}

// Run the script
main().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
}); 