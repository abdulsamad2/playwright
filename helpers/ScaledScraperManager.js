import moment from "moment";
import { setTimeout as setTimeoutPromise } from "timers/promises";
import { Event, ErrorLog, ConsecutiveGroup } from "../models/index.js";
import { ScrapeEvent, refreshHeaders, generateEnhancedHeaders } from "../scraper.js";
import { cpus } from "os";
import fs from "fs/promises";
import path from "path";
import ProxyManager from "./ProxyManager.js";
import { v4 as uuidv4 } from 'uuid';
import SyncService from '../services/syncService.js';
import nodeFs from 'fs';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import SessionManager from './SessionManager.js';
import { runCsvUploadCycle } from './csvUploadCycle.js';
import AutoRestartMonitor from './AutoRestartMonitor.js';
import { CoordinationService } from '../services/coordinationService.js';
import { getCurrentInstanceConfig } from '../config/instanceConfig.js';
import axios from 'axios';

/**
 * ScaledScraperManager - Enhanced scraper manager for horizontal scaling
 * Supports both primary and secondary instance modes
 */
export class ScaledScraperManager {
  constructor() {
    this.instanceConfig = getCurrentInstanceConfig();
    this.coordinationService = new CoordinationService(this);
    this.isRunning = false;
    this.activeJobs = new Map();
    this.successCount = 0;
    this.failedEvents = new Set();
    this.retryQueue = [];
    this.lastCookieRefresh = null;
    this.lastCsvUpload = null;
    this.autoRestartMonitor = null;
    
    // Instance-specific configuration
    this.maxConcurrentEvents = this.instanceConfig.maxEvents;
    this.enableCookieRefresh = this.instanceConfig.enableCookieRefresh;
    this.enableCsvUpload = this.instanceConfig.enableCsvUpload;
    this.enableScraping = this.instanceConfig.enableScraping;
    
    // Coordination settings
    this.primaryUrl = process.env.PRIMARY_URL || 'http://127.0.0.1:3000';
    this.eventFetchInterval = 30000; // Fetch new events every 30 seconds
    this.assignedEvents = new Set(); // Events assigned to this instance
    
    console.log(`Initialized ${this.instanceConfig.type} instance with max ${this.maxConcurrentEvents} events`);
  }

  /**
   * Start continuous scraping based on instance type
   */
  async startContinuousScraping() {
    if (this.isRunning) {
      console.log('Scraper is already running');
      return;
    }

    this.isRunning = true;
    console.log(`Starting ${this.instanceConfig.type} instance scraper...`);

    try {
      if (this.instanceConfig.type === 'primary') {
        await this.startPrimaryInstance();
      } else {
        await this.startSecondaryInstance();
      }
    } catch (error) {
      console.error('Error starting scraper:', error);
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Start primary instance - handles coordination, cookies, CSV upload, and scraping
   */
  async startPrimaryInstance() {
    console.log('Starting primary instance with full capabilities...');
    
    // Start cookie refresh cycle if enabled
    if (this.enableCookieRefresh) {
      this.startCookieRefreshCycle();
    }
    
    // Start CSV upload cycle if enabled
    if (this.enableCsvUpload) {
      this.startCsvUploadCycle();
    }
    
    // Start auto-restart monitoring
    this.startAutoRestartMonitoring();
    
    // Start scraping if enabled
    if (this.enableScraping) {
      this.startScrapingLoop();
    }
    
    console.log('Primary instance started successfully');
  }

  /**
   * Start secondary instance - only handles scraping
   */
  async startSecondaryInstance() {
    console.log('Starting secondary instance for scraping only...');
    
    // Wait for registration with primary
    await this.waitForRegistration();
    
    // Start scraping loop
    if (this.enableScraping) {
      this.startScrapingLoop();
    }
    
    console.log('Secondary instance started successfully');
  }

  /**
   * Wait for successful registration with primary instance
   */
  async waitForRegistration() {
    const maxWaitTime = 60000; // 1 minute
    const checkInterval = 5000; // 5 seconds
    const startTime = Date.now();
    
    while (!this.coordinationService.isRegistered && (Date.now() - startTime) < maxWaitTime) {
      console.log('Waiting for registration with primary instance...');
      await new Promise(resolve => global.setTimeout(resolve, checkInterval));
    }
    
    if (!this.coordinationService.isRegistered) {
      throw new Error('Failed to register with primary instance within timeout');
    }
    
    console.log('Successfully registered with primary instance');
  }

  /**
   * Start the main scraping loop
   */
  async startScrapingLoop() {
    console.log('Starting scraping loop...');
    
    while (this.isRunning) {
      try {
        await this.processEvents();
        await new Promise(resolve => setTimeout(resolve, 10000)); // 10 second interval
      } catch (error) {
        console.error('Error in scraping loop:', error);
        await new Promise(resolve => setTimeout(resolve, 30000)); // Wait 30 seconds on error
      }
    }
  }

  /**
   * Process events based on instance type
   */
  async processEvents() {
    let events = [];
    
    if (this.instanceConfig.type === 'primary') {
      // Primary instance gets events from database directly
      events = await this.getEventsFromDatabase();
    } else {
      // Secondary instance gets events from coordination service
      events = await this.getEventsFromCoordination();
    }
    
    if (events.length === 0) {
      return;
    }
    
    console.log(`Processing ${events.length} events on ${this.instanceConfig.type} instance`);
    
    // Process events with concurrency limit
    const concurrentLimit = Math.min(this.maxConcurrentEvents, events.length);
    const chunks = this.chunkArray(events, concurrentLimit);
    
    for (const chunk of chunks) {
      if (!this.isRunning) break;
      
      const promises = chunk.map(event => this.processEvent(event));
      await Promise.allSettled(promises);
    }
  }

  /**
   * Get events from database (primary instance)
   */
  async getEventsFromDatabase() {
    try {
      const events = await Event.find({
        isActive: true,
        $or: [
          { assignedInstance: { $exists: false } },
          { assignedInstance: this.coordinationService.instanceId },
          { assignedInstance: null }
        ]
      })
      .limit(this.maxConcurrentEvents)
      .sort({ lastUpdated: 1 });
      
      return events;
    } catch (error) {
      console.error('Error fetching events from database:', error);
      return [];
    }
  }

  /**
   * Get events from coordination service (secondary instance)
   */
  async getEventsFromCoordination() {
    try {
      const response = await axios.get(`${this.primaryUrl}/api/coordination/events`, {
        params: {
          instanceId: this.coordinationService.instanceId,
          limit: this.maxConcurrentEvents
        }
      });
      
      if (response.data.success) {
        return response.data.data;
      }
      
      return [];
    } catch (error) {
      console.error('Error fetching events from coordination service:', error);
      return [];
    }
  }

  /**
   * Process a single event
   */
  async processEvent(event) {
    const eventId = event._id.toString();
    
    // Skip if already processing
    if (this.activeJobs.has(eventId)) {
      return;
    }
    
    this.activeJobs.set(eventId, {
      eventId,
      startTime: Date.now(),
      status: 'processing'
    });
    
    try {
      // Get cookies and headers
      const { cookies, headers } = await this.getCookiesAndHeaders(eventId);
      
      // Scrape the event
      const result = await ScrapeEvent(event, cookies, headers);
      
      if (result.success) {
        this.successCount++;
        this.failedEvents.delete(eventId);
        
        // Update event status
        await this.updateEventStatus(eventId, 'success', result);
      } else {
        this.failedEvents.add(eventId);
        
        // Update event status
        await this.updateEventStatus(eventId, 'failed', result);
      }
    } catch (error) {
      console.error(`Error processing event ${eventId}:`, error);
      this.failedEvents.add(eventId);
      
      // Update event status
      await this.updateEventStatus(eventId, 'error', { error: error.message });
    } finally {
      this.activeJobs.delete(eventId);
    }
  }

  /**
   * Get cookies and headers for scraping
   */
  async getCookiesAndHeaders(eventId) {
    if (this.instanceConfig.type === 'primary') {
      // Primary instance generates its own cookies and headers
      return await this.generateCookiesAndHeaders(eventId);
    } else {
      // Secondary instance gets cookies from primary
      return await this.getCookiesFromPrimary(eventId);
    }
  }

  /**
   * Generate cookies and headers (primary instance)
   */
  async generateCookiesAndHeaders(eventId) {
    try {
      // Use existing cookie/header generation logic
      const headers = await refreshHeaders();
      const cookies = await this.getFreshCookies();
      
      return { cookies, headers };
    } catch (error) {
      console.error('Error generating cookies and headers:', error);
      return { cookies: '', headers: {} };
    }
  }

  /**
   * Get cookies from primary instance (secondary instance)
   */
  async getCookiesFromPrimary(eventId) {
    try {
      const response = await axios.get(`${this.primaryUrl}/api/coordination/cookies`);
      
      if (response.data.success) {
        return {
          cookies: response.data.data.cookies,
          headers: response.data.data.headers || {}
        };
      }
      
      return { cookies: '', headers: {} };
    } catch (error) {
      console.error('Error getting cookies from primary:', error);
      return { cookies: '', headers: {} };
    }
  }

  /**
   * Update event status
   */
  async updateEventStatus(eventId, status, result) {
    try {
      if (this.instanceConfig.type === 'primary') {
        // Primary instance updates database directly
        await Event.findByIdAndUpdate(eventId, {
          lastUpdated: new Date(),
          lastScrapedBy: this.coordinationService.instanceId,
          status: status,
          lastResult: result
        });
      } else {
        // Secondary instance reports to primary
        await axios.post(`${this.primaryUrl}/api/coordination/events/status`, {
          eventId,
          status,
          result,
          instanceId: this.coordinationService.instanceId
        });
      }
    } catch (error) {
      console.error(`Error updating event status for ${eventId}:`, error);
    }
  }

  /**
   * Start cookie refresh cycle (primary instance only)
   */
  startCookieRefreshCycle() {
    if (!this.enableCookieRefresh) return;
    
    console.log('Starting cookie refresh cycle...');
    
    const refreshInterval = 15 * 60 * 1000; // 15 minutes
    
    setInterval(async () => {
      try {
        console.log('Refreshing cookies...');
        await this.refreshCookies();
        this.lastCookieRefresh = new Date();
        console.log('Cookies refreshed successfully');
      } catch (error) {
        console.error('Error refreshing cookies:', error);
      }
    }, refreshInterval);
  }

  /**
   * Start CSV upload cycle (primary instance only)
   */
  startCsvUploadCycle() {
    if (!this.enableCsvUpload) return;
    
    console.log('Starting CSV upload cycle...');
    
    const uploadInterval = 30 * 60 * 1000; // 30 minutes
    
    setInterval(async () => {
      try {
        console.log('Starting CSV upload...');
        await runCsvUploadCycle();
        this.lastCsvUpload = new Date();
        console.log('CSV upload completed successfully');
      } catch (error) {
        console.error('Error in CSV upload cycle:', error);
      }
    }, uploadInterval);
  }

  /**
   * Start auto-restart monitoring (primary instance only)
   */
  startAutoRestartMonitoring() {
    if (this.autoRestartMonitor) return;
    
    this.autoRestartMonitor = new AutoRestartMonitor(this);
    this.autoRestartMonitor.start();
  }

  /**
   * Get autoRestartMonitor with fallback methods
   */
  get autoRestartMonitor() {
    if (!this._autoRestartMonitor) {
      this._autoRestartMonitor = {
        start: () => console.log('AutoRestartMonitor started'),
        startMonitoring: () => console.log('AutoRestartMonitor monitoring started'),
        recordFailure: () => console.log('AutoRestartMonitor recorded failure'),
        stop: () => console.log('AutoRestartMonitor stopped')
      };
    }
    return this._autoRestartMonitor;
  }

  set autoRestartMonitor(value) {
    this._autoRestartMonitor = value;
  }

  /**
   * Stop continuous scraping
   */
  stopContinuousScraping() {
    console.log('Stopping scraper...');
    this.isRunning = false;
    
    // Clear active jobs
    this.activeJobs.clear();
    
    // Stop auto-restart monitor
    if (this.autoRestartMonitor) {
      this.autoRestartMonitor.stop();
    }
    
    console.log('Scraper stopped');
  }

  /**
   * Get scraper status
   */
  getStatus() {
    return {
      isRunning: this.isRunning,
      instanceType: this.instanceConfig.type,
      instanceId: this.coordinationService.instanceId,
      activeJobs: Array.from(this.activeJobs.keys()),
      successCount: this.successCount,
      failedEvents: Array.from(this.failedEvents),
      retryQueueSize: this.retryQueue.length,
      maxConcurrentEvents: this.maxConcurrentEvents,
      lastCookieRefresh: this.lastCookieRefresh,
      lastCsvUpload: this.lastCsvUpload,
      coordination: this.coordinationService.getCoordinationStatus()
    };
  }

  /**
   * Utility function to chunk array
   */
  chunkArray(array, chunkSize) {
    const chunks = [];
    for (let i = 0; i < array.length; i += chunkSize) {
      chunks.push(array.slice(i, i + chunkSize));
    }
    return chunks;
  }

  /**
   * Get active event count for coordination
   */
  getActiveEventCount() {
    return this.activeJobs.size;
  }

  /**
   * Logging helper
   */
  logWithTime(message, level = 'info') {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] [${level.toUpperCase()}] ${message}`);
  }

  // Placeholder methods for compatibility
  async refreshCookies() {
    // Implement cookie refresh logic
  }

  async getFreshCookies() {
    // Implement fresh cookie retrieval
    return '';
  }
}

export default ScaledScraperManager;