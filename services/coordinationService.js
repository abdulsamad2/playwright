import axios from 'axios';
import { getCurrentInstanceConfig, COORDINATION_ENDPOINTS, HEALTH_CHECK_CONFIG } from '../config/instanceConfig.js';
import { Event } from '../models/index.js';

/**
 * CoordinationService handles communication between primary and secondary instances
 * Primary instance acts as coordinator, secondary instances register and get work assignments
 */
export class CoordinationService {
  constructor(logger) {
    this.logger = logger;
    this.instanceConfig = getCurrentInstanceConfig();
    this.registeredInstances = new Map(); // instanceId -> instanceInfo
    this.instanceHeartbeats = new Map(); // instanceId -> lastHeartbeat
    this.eventAssignments = new Map(); // eventId -> instanceId
    this.primaryUrl = process.env.PRIMARY_URL || 'http://127.0.0.1:3000';
    this.instanceId = this.generateInstanceId();
    this.isRegistered = false;
    
    // Start coordination based on instance type
    if (this.instanceConfig.type === 'primary') {
      this.startPrimaryCoordination();
    } else {
      this.startSecondaryCoordination();
    }
  }

  generateInstanceId() {
    return `${this.instanceConfig.type}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  }

  /**
   * Primary instance coordination - manages secondary instances
   */
  startPrimaryCoordination() {
    this.logger?.logWithTime('Starting primary coordination service', 'info');
    
    // Clean up stale instances periodically
    setInterval(() => {
      this.cleanupStaleInstances();
    }, HEALTH_CHECK_CONFIG.INSTANCE_TIMEOUT);
    
    // Redistribute events periodically
    setInterval(() => {
      this.redistributeEvents();
    }, 60000); // Every minute
  }

  /**
   * Secondary instance coordination - registers with primary and gets assignments
   */
  startSecondaryCoordination() {
    this.logger?.logWithTime('Starting secondary coordination service', 'info');
    
    // Register with primary instance
    this.registerWithPrimary();
    
    // Send heartbeats to primary
    setInterval(() => {
      this.sendHeartbeat();
    }, HEALTH_CHECK_CONFIG.HEARTBEAT_INTERVAL);
    
    // Retry registration if failed
    setInterval(() => {
      if (!this.isRegistered) {
        this.registerWithPrimary();
      }
    }, HEALTH_CHECK_CONFIG.COORDINATION_RETRY_INTERVAL);
  }

  /**
   * Register secondary instance with primary
   */
  async registerWithPrimary() {
    try {
      const response = await axios.post(`${this.primaryUrl}${COORDINATION_ENDPOINTS.REGISTER_INSTANCE}`, {
        instanceId: this.instanceId,
        type: this.instanceConfig.type,
        port: this.instanceConfig.port,
        maxEvents: this.instanceConfig.maxEvents,
        capabilities: {
          enableScraping: this.instanceConfig.enableScraping,
          enableCookieRefresh: this.instanceConfig.enableCookieRefresh,
          enableCsvUpload: this.instanceConfig.enableCsvUpload
        }
      });
      
      if (response.data.success) {
        this.isRegistered = true;
        this.logger?.logWithTime(`Successfully registered with primary instance: ${this.instanceId}`, 'success');
      }
    } catch (error) {
      this.logger?.logWithTime(`Failed to register with primary: ${error.message}`, 'error');
      this.isRegistered = false;
    }
  }

  /**
   * Send heartbeat to primary instance
   */
  async sendHeartbeat() {
    if (!this.isRegistered) return;
    
    try {
      await axios.post(`${this.primaryUrl}${COORDINATION_ENDPOINTS.HEARTBEAT}`, {
        instanceId: this.instanceId,
        timestamp: Date.now(),
        status: 'healthy',
        activeEvents: this.getActiveEventCount()
      });
    } catch (error) {
      this.logger?.logWithTime(`Heartbeat failed: ${error.message}`, 'warning');
      this.isRegistered = false;
    }
  }

  /**
   * Register a secondary instance (called by primary)
   */
  registerInstance(instanceData) {
    const { instanceId, type, port, maxEvents, capabilities } = instanceData;
    
    this.registeredInstances.set(instanceId, {
      id: instanceId,
      type,
      port,
      maxEvents,
      capabilities,
      registeredAt: Date.now(),
      activeEvents: 0
    });
    
    this.instanceHeartbeats.set(instanceId, Date.now());
    
    this.logger?.logWithTime(`Registered instance: ${instanceId} (${type})`, 'info');
    
    return { success: true, message: 'Instance registered successfully' };
  }

  /**
   * Update instance heartbeat (called by primary)
   */
  updateHeartbeat(instanceId, heartbeatData) {
    if (this.registeredInstances.has(instanceId)) {
      this.instanceHeartbeats.set(instanceId, Date.now());
      
      // Update active events count
      const instance = this.registeredInstances.get(instanceId);
      instance.activeEvents = heartbeatData.activeEvents || 0;
      
      return { success: true, message: 'Heartbeat updated' };
    }
    
    return { success: false, message: 'Instance not found' };
  }

  /**
   * Clean up stale instances that haven't sent heartbeats
   */
  cleanupStaleInstances() {
    const now = Date.now();
    const staleInstances = [];
    
    for (const [instanceId, lastHeartbeat] of this.instanceHeartbeats.entries()) {
      if (now - lastHeartbeat > HEALTH_CHECK_CONFIG.INSTANCE_TIMEOUT) {
        staleInstances.push(instanceId);
      }
    }
    
    for (const instanceId of staleInstances) {
      this.logger?.logWithTime(`Removing stale instance: ${instanceId}`, 'warning');
      this.registeredInstances.delete(instanceId);
      this.instanceHeartbeats.delete(instanceId);
      
      // Reassign events from this instance
      this.reassignEventsFromInstance(instanceId);
    }
  }

  /**
   * Get events assigned to a specific instance
   */
  async getEventsForInstance(instanceId, limit = 100) {
    try {
      // Get events that need scraping and aren't currently assigned or assigned to this instance
      const events = await Event.find({
        $or: [
          { assignedInstance: { $exists: false } },
          { assignedInstance: instanceId },
          { assignedInstance: null }
        ],
        isActive: true
      })
      .limit(limit)
      .sort({ lastUpdated: 1 }); // Prioritize events that haven't been updated recently
      
      // Assign events to this instance
      const eventIds = events.map(event => event._id);
      await Event.updateMany(
        { _id: { $in: eventIds } },
        { assignedInstance: instanceId, assignedAt: new Date() }
      );
      
      // Update local assignment tracking
      events.forEach(event => {
        this.eventAssignments.set(event._id.toString(), instanceId);
      });
      
      return events;
    } catch (error) {
      this.logger?.logWithTime(`Error getting events for instance ${instanceId}: ${error.message}`, 'error');
      return [];
    }
  }

  /**
   * Redistribute events among available instances
   */
  async redistributeEvents() {
    try {
      const healthyInstances = this.getHealthyInstances();
      if (healthyInstances.length === 0) return;
      
      // Get unassigned events
      const unassignedEvents = await Event.find({
        $or: [
          { assignedInstance: { $exists: false } },
          { assignedInstance: null }
        ],
        isActive: true
      });
      
      if (unassignedEvents.length === 0) return;
      
      // Distribute events evenly among healthy instances
      const eventsPerInstance = Math.ceil(unassignedEvents.length / healthyInstances.length);
      
      for (let i = 0; i < healthyInstances.length; i++) {
        const instance = healthyInstances[i];
        const startIndex = i * eventsPerInstance;
        const endIndex = Math.min(startIndex + eventsPerInstance, unassignedEvents.length);
        const eventsToAssign = unassignedEvents.slice(startIndex, endIndex);
        
        if (eventsToAssign.length > 0) {
          const eventIds = eventsToAssign.map(event => event._id);
          await Event.updateMany(
            { _id: { $in: eventIds } },
            { assignedInstance: instance.id, assignedAt: new Date() }
          );
          
          this.logger?.logWithTime(`Assigned ${eventsToAssign.length} events to instance ${instance.id}`, 'info');
        }
      }
    } catch (error) {
      this.logger?.logWithTime(`Error redistributing events: ${error.message}`, 'error');
    }
  }

  /**
   * Reassign events from a failed instance
   */
  async reassignEventsFromInstance(failedInstanceId) {
    try {
      // Clear assignments from failed instance
      await Event.updateMany(
        { assignedInstance: failedInstanceId },
        { $unset: { assignedInstance: 1, assignedAt: 1 } }
      );
      
      // Remove from local tracking
      for (const [eventId, instanceId] of this.eventAssignments.entries()) {
        if (instanceId === failedInstanceId) {
          this.eventAssignments.delete(eventId);
        }
      }
      
      this.logger?.logWithTime(`Reassigned events from failed instance: ${failedInstanceId}`, 'info');
      
      // Trigger redistribution
      setTimeout(() => this.redistributeEvents(), 5000);
    } catch (error) {
      this.logger?.logWithTime(`Error reassigning events from instance ${failedInstanceId}: ${error.message}`, 'error');
    }
  }

  /**
   * Get list of healthy instances
   */
  getHealthyInstances() {
    const now = Date.now();
    const healthyInstances = [];
    
    for (const [instanceId, instance] of this.registeredInstances.entries()) {
      const lastHeartbeat = this.instanceHeartbeats.get(instanceId);
      if (lastHeartbeat && (now - lastHeartbeat) < HEALTH_CHECK_CONFIG.INSTANCE_TIMEOUT) {
        healthyInstances.push(instance);
      }
    }
    
    return healthyInstances;
  }

  /**
   * Get coordination status
   */
  getCoordinationStatus() {
    const healthyInstances = this.getHealthyInstances();
    
    return {
      instanceId: this.instanceId,
      instanceType: this.instanceConfig.type,
      isRegistered: this.isRegistered,
      totalInstances: this.registeredInstances.size,
      healthyInstances: healthyInstances.length,
      instances: Array.from(this.registeredInstances.values()),
      eventAssignments: this.eventAssignments.size
    };
  }

  /**
   * Get active event count for this instance
   */
  getActiveEventCount() {
    // This should be implemented based on your scraper manager
    return 0; // Placeholder
  }
}

export default CoordinationService;