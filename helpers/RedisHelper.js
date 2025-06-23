import { getRedisClient } from '../config/db.js';

/**
 * Redis Helper utility for caching and data operations
 * Provides methods to interact with Redis for the external microservice integration
 */
class RedisHelper {
  constructor() {
    this.client = null;
  }

  /**
   * Initialize Redis client
   */
  init() {
    try {
      this.client = getRedisClient();
    } catch (error) {
      console.error('Failed to initialize Redis client:', error);
      throw error;
    }
  }

  /**
   * Get event data from Redis cache
   * @param {string} eventId - Event ID
   * @returns {Object|null} Event data or null if not found
   */
  async getEvent(eventId) {
    try {
      if (!this.client) this.init();
      const eventData = await this.client.hGetAll(`event:${eventId}`);
      return Object.keys(eventData).length > 0 ? eventData : null;
    } catch (error) {
      console.error(`Error getting event ${eventId} from Redis:`, error);
      return null;
    }
  }

  /**
   * Cache event data in Redis
   * @param {string} eventId - Event ID
   * @param {Object} eventData - Event data to cache
   * @param {number} ttl - Time to live in seconds (optional)
   */
  async setEvent(eventId, eventData, ttl = 3600) {
    try {
      if (!this.client) this.init();
      const key = `event:${eventId}`;
      await this.client.hSet(key, eventData);
      if (ttl > 0) {
        await this.client.expire(key, ttl);
      }
      console.log(`Event ${eventId} cached in Redis`);
    } catch (error) {
      console.error(`Error caching event ${eventId} in Redis:`, error);
    }
  }

  /**
   * Get seat group data from Redis cache
   * @param {string} groupId - Seat group ID
   * @returns {Object|null} Seat group data or null if not found
   */
  async getSeatGroup(groupId) {
    try {
      if (!this.client) this.init();
      const groupData = await this.client.hGetAll(`seat_group:${groupId}`);
      return Object.keys(groupData).length > 0 ? groupData : null;
    } catch (error) {
      console.error(`Error getting seat group ${groupId} from Redis:`, error);
      return null;
    }
  }

  /**
   * Cache seat group data in Redis
   * @param {string} groupId - Seat group ID
   * @param {Object} groupData - Seat group data to cache
   * @param {number} ttl - Time to live in seconds (optional)
   */
  async setSeatGroup(groupId, groupData, ttl = 1800) {
    try {
      if (!this.client) this.init();
      const key = `seat_group:${groupId}`;
      await this.client.hSet(key, groupData);
      if (ttl > 0) {
        await this.client.expire(key, ttl);
      }
      console.log(`Seat group ${groupId} cached in Redis`);
    } catch (error) {
      console.error(`Error caching seat group ${groupId} in Redis:`, error);
    }
  }

  /**
   * Get sync information
   * @returns {string|null} Last sync timestamp or null
   */
  async getSyncInfo() {
    try {
      if (!this.client) this.init();
      return await this.client.get('sync:last_update');
    } catch (error) {
      console.error('Error getting sync info from Redis:', error);
      return null;
    }
  }

  /**
   * Update sync information
   * @param {string} timestamp - Sync timestamp
   */
  async setSyncInfo(timestamp) {
    try {
      if (!this.client) this.init();
      await this.client.set('sync:last_update', timestamp);
      console.log('Sync info updated in Redis');
    } catch (error) {
      console.error('Error setting sync info in Redis:', error);
    }
  }

  /**
   * Delete cached data
   * @param {string} key - Redis key to delete
   */
  async deleteKey(key) {
    try {
      if (!this.client) this.init();
      await this.client.del(key);
      console.log(`Key ${key} deleted from Redis`);
    } catch (error) {
      console.error(`Error deleting key ${key} from Redis:`, error);
    }
  }

  /**
   * Check if Redis is connected
   * @returns {boolean} Connection status
   */
  isConnected() {
    try {
      return this.client && this.client.isOpen;
    } catch (error) {
      return false;
    }
  }

  /**
   * Get all keys matching a pattern
   * @param {string} pattern - Redis key pattern
   * @returns {Array} Array of matching keys
   */
  async getKeys(pattern) {
    try {
      if (!this.client) this.init();
      return await this.client.keys(pattern);
    } catch (error) {
      console.error(`Error getting keys with pattern ${pattern}:`, error);
      return [];
    }
  }
}

// Export singleton instance
const redisHelper = new RedisHelper();
export default redisHelper;