import axios from 'axios';
import fs from 'fs';
import FormData from 'form-data';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

/**
 * Service for interacting with the Sync API for CSV inventory management
 */
class SyncService {
  constructor(companyId, apiToken) {
    this.companyId = companyId;
    this.apiToken = apiToken;
    this.baseUrl = 'https://app.sync.automatiq.com/sync/api';
  }

  /**
   * Request upload credentials from Sync API
   * @param {boolean} zipped - Whether the file is zipped or not
   * @returns {Promise<Object>} - The upload credentials
   */
  async requestUploadCredentials(zipped = false) {
    try {
      const response = await axios.post(
        `${this.baseUrl}/inventories/csv_upload_request?zipped=${zipped}`,
        {},
        {
          headers: {
            'X-Company-Id': this.companyId,
            'X-Api-Token': this.apiToken
          }
        }
      );
      
      return response.data;
    } catch (error) {
      console.error('Error requesting upload credentials:', error.message);
      throw error;
    }
  }

  /**
   * Upload a CSV file to S3 using the credentials provided by Sync API
   * @param {string} filePath - Path to the CSV file
   * @param {Object} credentials - The upload credentials from requestUploadCredentials
   * @returns {Promise<boolean>} - True if upload was successful
   */
  async uploadCsvToS3(filePath, credentials) {
    try {
      if (!fs.existsSync(filePath)) {
        throw new Error(`File not found: ${filePath}`);
      }
      
      const { url, fields } = credentials.upload;
      const formData = new FormData();
      
      // Add all the fields to the form data
      Object.entries(fields).forEach(([key, value]) => {
        formData.append(key, value);
      });
      
      // Add the file
      formData.append('file', fs.createReadStream(filePath));
      
      // Upload to S3
      const response = await axios.post(url, formData, {
        headers: {
          ...formData.getHeaders()
        },
        maxContentLength: Infinity,
        maxBodyLength: Infinity
      });
      
      return response.status === 204; // S3 returns 204 No Content on successful upload
    } catch (error) {
      console.error('Error uploading CSV to S3:', error.message);
      throw error;
    }
  }
  
  /**
   * Create a blank CSV file with headers to clear inventory
   * @param {string} outputPath - Path where the blank CSV will be saved
   * @param {Array<string>} headers - CSV headers (columns)
   * @returns {Promise<string>} - Path to the created blank CSV file
   */
  async createBlankCsv(outputPath, headers = ['sku', 'quantity', 'price']) {
    try {
      // Create CSV content with just headers
      const csvContent = headers.join(',') + '\n';
      
      // Write to file
      fs.writeFileSync(outputPath, csvContent);
      
      return outputPath;
    } catch (error) {
      console.error('Error creating blank CSV:', error.message);
      throw error;
    }
  }
  
  /**
   * Upload a CSV file to Sync
   * @param {string} filePath - Path to the CSV file
   * @param {boolean} zipped - Whether the file is zipped
   * @returns {Promise<Object>} - Response from Sync API
   */
  async uploadCsvToSync(filePath, zipped = false) {
    try {
      // Request upload credentials
      const credentials = await this.requestUploadCredentials(zipped);
      
      // Upload to S3
      const uploadSuccessful = await this.uploadCsvToS3(filePath, credentials);
      
      if (!uploadSuccessful) {
        throw new Error('Upload to S3 failed');
      }
      
      return {
        success: true,
        uploadId: credentials.id,
        message: 'CSV uploaded successfully'
      };
    } catch (error) {
      console.error('Error uploading CSV to Sync:', error.message);
      throw error;
    }
  }
  
  /**
   * Clear all inventory by uploading a blank CSV
   * @returns {Promise<Object>} - Response from Sync API
   */
  async clearAllInventory() {
    try {
      // Create a blank CSV file
      const blankCsvPath = path.join(__dirname, '../data/blank_inventory.csv');
      await this.createBlankCsv(blankCsvPath);
      
      // Upload the blank CSV
      const result = await this.uploadCsvToSync(blankCsvPath);
      
      // Clean up the temporary file
      // fs.unlinkSync(blankCsvPath);
      
      return {
        success: true,
        message: 'All inventory cleared successfully',
        ...result
      };
    } catch (error) {
      console.error('Error clearing inventory:', error.message);
      throw error;
    }
  }
}

export default SyncService; 