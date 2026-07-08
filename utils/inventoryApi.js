import axios from 'axios';
import dotenv from "dotenv";

// Load environment variables
dotenv.config();

/**
 * Utility class for SeatScouts inventory API operations
 */
class InventoryApi {
  constructor() {
    this.baseURL = 'https://app.seatscouts.com/sync/api';
    this.debug = process.env.INVENTORY_API_DEBUG === '1';
    this.timeoutMs = parseInt(process.env.INVENTORY_API_TIMEOUT_MS, 10) || 7000;
    this.authFailCooldownMs = parseInt(process.env.INVENTORY_API_AUTH_FAIL_COOLDOWN_MS, 10) || 10 * 60 * 1000;
    this.suspendedUntil = 0;
    this.headers = {
      'X-Company-Id': process.env.SEATSCOUTS_COMPANY_ID,
      'X-Api-Token': process.env.SEATSCOUTS_API_TOKEN,
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    };
    this.isConfigured = Boolean(
      this.headers['X-Company-Id'] && this.headers['X-Api-Token']
    );

    if (!this.isConfigured) {
      console.warn('[InventoryApi] SeatScouts credentials missing; delete requests will be skipped.');
    }
  }

  /**
   * Delete inventory item by inventory ID (uses batch method)
   * @param {string} inventoryId - The inventory ID to delete
   * @returns {Promise<Object>} API response
   */


  /**
   * Delete multiple inventory items in batch
   * @param {Array<string>} inventoryIds - Array of inventory IDs to delete
   * @returns {Promise<Object>} Batch deletion results
   */
  async deleteInventoryBatch(inventoryIds) {
    if (!this.isConfigured) {
      return {
        successful: [],
        failed: inventoryIds.map(id => ({
          id,
          error: 'SeatScouts credentials missing',
          status: 'CONFIG_MISSING'
        })),
        total: inventoryIds.length,
        skipped: true
      };
    }

    if (this.suspendedUntil > Date.now()) {
      return {
        successful: [],
        failed: inventoryIds.map(id => ({
          id,
          error: 'Inventory API temporarily suspended after auth failures',
          status: 'API_SUSPENDED'
        })),
        total: inventoryIds.length,
        skipped: true
      };
    }

    try {
      if (this.debug) {
        console.log(`[API DEBUG] Attempting to delete inventory batch:`, {
          url: `${this.baseURL}/inventories/delete`,
          inventoryIds: inventoryIds,
          count: inventoryIds.length,
          headers: {
            'X-Company-Id': this.headers['X-Company-Id'],
            'X-Api-Token': this.headers['X-Api-Token'],
            'Content-Type': this.headers['Content-Type']
          }
        });
      }

      const response = await axios.post(`${this.baseURL}/inventories/delete`, {
        inventory_ids: inventoryIds
      }, {
        headers: this.headers,
        timeout: this.timeoutMs
      });

      if (this.debug) {
        console.log(`[API DEBUG] Batch deletion successful:`, response.status, response.data);
      }

      return {
        successful: inventoryIds, // Assume all successful if no error
        failed: [],
        total: inventoryIds.length,
        apiResponse: response.data
      };
    } catch (error) {
      console.error(`Failed to delete inventory batch:`, error.message);
      const statusCode = error.response?.status || 'NETWORK_ERROR';
      if (statusCode === 401 || statusCode === 403) {
        this.suspendedUntil = Date.now() + this.authFailCooldownMs;
        console.warn(
          `[InventoryApi] Received ${statusCode}; suspending delete calls for ${Math.round(this.authFailCooldownMs / 1000)}s`
        );
      }
      if (error.response && this.debug) {
        console.error(`[API DEBUG] Response status:`, error.response.status);
        console.error(`[API DEBUG] Response data:`, error.response.data);
        console.error(`[API DEBUG] Response headers:`, error.response.headers);
      }
      
      // Return failed result for all inventory IDs since batch deletion failed
      // No fallback to individual deletions as there's no separate single deletion API
      return {
        successful: [],
        failed: inventoryIds.map(id => ({
          id: id,
          error: error.message,
          status: statusCode
        })),
        total: inventoryIds.length
      };
    }
  }
}

export default InventoryApi;