import { CookieRefresh } from '../models/cookieRefreshModel.js';
import crypto from 'crypto';

/**
 * Helper class to track cookie refresh operations in the database
 */
class CookieRefreshTracker {
  /**
   * Start tracking a new cookie refresh operation
   * @param {string} eventId - The event ID used for this refresh operation
   * @param {object} proxy - The proxy object used for this refresh
   * @returns {Promise<object>} The created refresh tracking record
   */
  static async startRefresh(eventId, proxy) {
    const refreshId = crypto.randomUUID();
    const proxyString = proxy?.proxy || 'no_proxy';
    
    const refreshRecord = await CookieRefresh.create({
      refreshId,
      status: 'in_progress',
      eventId,
      startTime: new Date(),
      proxy: proxyString
    });
    
    console.log(`Started tracking cookie refresh operation: ${refreshId} for event ${eventId}`);
    return refreshRecord;
  }
  
  /**
   * Mark a refresh operation as successful
   * @param {string} refreshId - The ID of the refresh operation
   * @param {number} cookieCount - Number of cookies retrieved
   * @param {number} retryCount - Number of retries performed
   * @returns {Promise<object>} The updated refresh tracking record
   */
  static async markSuccess(refreshId, cookieCount, retryCount) {
    const completionTime = new Date();
    const nextScheduledRefresh = new Date(
      completionTime.getTime() + (30 * 60 * 1000) // Default 30 minutes
    );
    
    const refreshRecord = await CookieRefresh.findOneAndUpdate(
      { refreshId },
      {
        status: 'success',
        completionTime,
        nextScheduledRefresh,
        cookieCount,
        retryCount,
        duration: completionTime - (await CookieRefresh.findOne({ refreshId })).startTime,
      },
      { new: true }
    );
    
    console.log(`Cookie refresh completed successfully: ${refreshId} with ${cookieCount} cookies`);
    return refreshRecord;
  }
  
  /**
   * Mark a refresh operation as failed
   * @param {string} refreshId - The ID of the refresh operation
   * @param {string} errorMessage - Error message describing the failure
   * @param {number} retryCount - Number of retries performed
   * @returns {Promise<object>} The updated refresh tracking record
   */
  static async markFailed(refreshId, errorMessage, retryCount) {
    const completionTime = new Date();
    // Schedule next attempt sooner if it failed
    const nextScheduledRefresh = new Date(
      completionTime.getTime() + (5 * 60 * 1000) // 5 minutes for failures
    );
    
    const refreshRecord = await CookieRefresh.findOneAndUpdate(
      { refreshId },
      {
        status: 'failed',
        completionTime,
        nextScheduledRefresh,
        errorMessage,
        retryCount,
        duration: completionTime - (await CookieRefresh.findOne({ refreshId })).startTime,
      },
      { new: true }
    );
    
    console.log(`Cookie refresh failed: ${refreshId}, next attempt: ${nextScheduledRefresh.toISOString()}`);
    return refreshRecord;
  }
  
  /**
   * Get statistics about cookie refresh operations
   * @param {number} limit - Number of recent operations to analyze
   * @returns {Promise<object>} Statistics about cookie refresh operations
   */
  static async getStats(limit = 100) {
    const recentRefreshes = await CookieRefresh.find()
      .sort({ startTime: -1 })
      .limit(limit);
    
    const successCount = recentRefreshes.filter(r => r.status === 'success').length;
    const failedCount = recentRefreshes.filter(r => r.status === 'failed').length;
    const inProgressCount = recentRefreshes.filter(r => r.status === 'in_progress').length;
    
    const totalCookies = recentRefreshes.reduce((sum, r) => sum + (r.cookieCount || 0), 0);
    const averageCookies = successCount > 0 
      ? totalCookies / successCount 
      : 0;
    
    const averageDuration = recentRefreshes
      .filter(r => r.duration)
      .reduce((sum, r) => sum + r.duration, 0) / (successCount + failedCount);
    
    const nextRefresh = await CookieRefresh.findOne({ status: 'success' })
      .sort({ nextScheduledRefresh: 1 });
    
    return {
      total: recentRefreshes.length,
      successCount,
      failedCount,
      inProgressCount,
      successRate: recentRefreshes.length > 0 
        ? (successCount / recentRefreshes.length * 100).toFixed(1) + '%' 
        : 'N/A',
      averageCookies: averageCookies.toFixed(1),
      averageDuration: averageDuration ? `${(averageDuration / 1000).toFixed(1)}s` : 'N/A',
      nextScheduledRefresh: nextRefresh?.nextScheduledRefresh || 'None scheduled',
      latestRefresh: recentRefreshes[0] || null
    };
  }
  
  /**
   * Check if a refresh is due
   * @returns {Promise<boolean>} Whether a refresh is due
   */
  static async isRefreshDue() {
    const now = new Date();
    const lastSuccessful = await CookieRefresh.findOne({ status: 'success' })
      .sort({ completionTime: -1 });
    
    if (!lastSuccessful) {
      return true; // No successful refresh yet, should refresh
    }
    
    return lastSuccessful.nextScheduledRefresh <= now;
  }
}

export default CookieRefreshTracker; 