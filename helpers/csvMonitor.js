import fs from 'fs';
import path from 'path';

/**
 * CSV Processing Monitor for 1000+ Events
 * Provides real-time monitoring of the optimized CSV processing system
 */
class CsvMonitor {
  constructor() {
    this.dataDir = path.join(process.cwd(), 'data');
    this.combinedCsvPath = path.join(this.dataDir, 'all_events_combined.csv');
    this.eventsDataPath = path.join(this.dataDir, 'events_data.json');
  }

  /**
   * Get comprehensive CSV processing statistics
   */
  async getStats() {
    const stats = {
      timestamp: new Date().toISOString(),
      memoryData: await this.getMemoryStats(),
      combinedCsv: await this.getCombinedCsvStats(),
      performance: this.getPerformanceMetrics(),
      recommendations: []
    };

    // Generate recommendations
    stats.recommendations = this.generateRecommendations(stats);

    return stats;
  }

  /**
   * Get in-memory data statistics
   */
  async getMemoryStats() {
    try {
      if (!fs.existsSync(this.eventsDataPath)) {
        return {
          exists: false,
          events: 0,
          totalRecords: 0,
          fileSize: 0
        };
      }

      const fileStats = fs.statSync(this.eventsDataPath);
      const data = JSON.parse(fs.readFileSync(this.eventsDataPath, 'utf8'));
      
      let totalRecords = 0;
      const eventStats = [];

      for (const [eventId, records] of Object.entries(data)) {
        totalRecords += records.length;
        eventStats.push({
          eventId,
          recordCount: records.length,
          avgSeatsPerRecord: records.length > 0 ? 
            Math.round(records.reduce((sum, r) => sum + (r.quantity || 0), 0) / records.length) : 0
        });
      }

      return {
        exists: true,
        events: Object.keys(data).length,
        totalRecords,
        fileSize: Math.round(fileStats.size / 1024), // KB
        lastModified: fileStats.mtime,
        topEvents: eventStats
          .sort((a, b) => b.recordCount - a.recordCount)
          .slice(0, 10)
      };
    } catch (error) {
      return {
        exists: false,
        error: error.message
      };
    }
  }

  /**
   * Get combined CSV file statistics
   */
  async getCombinedCsvStats() {
    try {
      if (!fs.existsSync(this.combinedCsvPath)) {
        return {
          exists: false,
          records: 0,
          fileSize: 0
        };
      }

      const fileStats = fs.statSync(this.combinedCsvPath);
      const content = fs.readFileSync(this.combinedCsvPath, 'utf8');
      const lines = content.split('\n');
      const recordCount = Math.max(0, lines.length - 2); // Subtract header and empty last line

      // Check for required fields
      const header = lines[0] || '';
      const hasEventId = header.includes('event_id');
      const hasMappingId = header.includes('mapping_id');

      // Sample a few records to check data quality
      const sampleRecords = lines.slice(1, 6).filter(line => line.trim());
      const dataQuality = this.analyzeDataQuality(sampleRecords, header);

      return {
        exists: true,
        records: recordCount,
        fileSize: Math.round(fileStats.size / 1024 / 1024 * 100) / 100, // MB
        lastModified: fileStats.mtime,
        hasRequiredFields: hasEventId && hasMappingId,
        missingFields: [
          !hasEventId ? 'event_id' : null,
          !hasMappingId ? 'mapping_id' : null
        ].filter(Boolean),
        dataQuality
      };
    } catch (error) {
      return {
        exists: false,
        error: error.message
      };
    }
  }

  /**
   * Analyze data quality of CSV records
   */
  analyzeDataQuality(sampleRecords, header) {
    if (sampleRecords.length === 0) {
      return { quality: 'unknown', issues: ['No sample records available'] };
    }

    const headers = header.split(',');
    const issues = [];
    let validRecords = 0;

    for (const record of sampleRecords) {
      const fields = record.split(',');
      
      if (fields.length !== headers.length) {
        issues.push('Field count mismatch');
        continue;
      }

      // Check for empty critical fields
      const eventIdIndex = headers.indexOf('event_id');
      const mappingIdIndex = headers.indexOf('mapping_id');
      const sectionIndex = headers.indexOf('section');
      const seatsIndex = headers.indexOf('seats');

      if (eventIdIndex >= 0 && !fields[eventIdIndex]) {
        issues.push('Missing event_id values');
      }
      if (mappingIdIndex >= 0 && !fields[mappingIdIndex]) {
        issues.push('Missing mapping_id values');
      }
      if (sectionIndex >= 0 && !fields[sectionIndex]) {
        issues.push('Missing section values');
      }
      if (seatsIndex >= 0 && !fields[seatsIndex]) {
        issues.push('Missing seats values');
      }

      validRecords++;
    }

    const qualityScore = validRecords / sampleRecords.length;
    let quality = 'good';
    if (qualityScore < 0.5) quality = 'poor';
    else if (qualityScore < 0.8) quality = 'fair';

    return {
      quality,
      qualityScore: Math.round(qualityScore * 100),
      issues: [...new Set(issues)], // Remove duplicates
      sampleSize: sampleRecords.length
    };
  }

  /**
   * Get performance metrics
   */
  getPerformanceMetrics() {
    const memUsage = process.memoryUsage();
    
    return {
      memoryUsage: {
        heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024), // MB
        heapTotal: Math.round(memUsage.heapTotal / 1024 / 1024), // MB
        external: Math.round(memUsage.external / 1024 / 1024), // MB
        rss: Math.round(memUsage.rss / 1024 / 1024) // MB
      },
      uptime: Math.round(process.uptime() / 60), // minutes
      nodeVersion: process.version
    };
  }

  /**
   * Generate recommendations based on current stats
   */
  generateRecommendations(stats) {
    const recommendations = [];

    // Memory recommendations
    if (stats.performance.memoryUsage.heapUsed > 1024) {
      recommendations.push({
        type: 'memory',
        priority: 'high',
        message: `High memory usage (${stats.performance.memoryUsage.heapUsed}MB). Consider running cleanup.`
      });
    }

    // CSV file recommendations
    if (!stats.combinedCsv.exists) {
      recommendations.push({
        type: 'csv',
        priority: 'high',
        message: 'Combined CSV file does not exist. Force generation recommended.'
      });
    } else if (!stats.combinedCsv.hasRequiredFields) {
      recommendations.push({
        type: 'csv',
        priority: 'critical',
        message: `Combined CSV missing required fields: ${stats.combinedCsv.missingFields.join(', ')}`
      });
    }

    // Data quality recommendations
    if (stats.combinedCsv.dataQuality?.quality === 'poor') {
      recommendations.push({
        type: 'quality',
        priority: 'high',
        message: `Poor data quality detected: ${stats.combinedCsv.dataQuality.issues.join(', ')}`
      });
    }

    // Performance recommendations
    const recordsPerEvent = stats.memoryData.events > 0 ? 
      Math.round(stats.memoryData.totalRecords / stats.memoryData.events) : 0;
    
    if (recordsPerEvent > 100) {
      recommendations.push({
        type: 'performance',
        priority: 'medium',
        message: `High average records per event (${recordsPerEvent}). Monitor for memory usage.`
      });
    }

    return recommendations;
  }

  /**
   * Print formatted statistics to console
   */
  async printStats() {
    const stats = await this.getStats();
    
    console.log('\n=== CSV PROCESSING MONITOR ===');
    console.log(`Timestamp: ${stats.timestamp}`);
    
    console.log('\n📊 MEMORY DATA:');
    if (stats.memoryData.exists) {
      console.log(`  Events in memory: ${stats.memoryData.events}`);
      console.log(`  Total records: ${stats.memoryData.totalRecords}`);
      console.log(`  JSON file size: ${stats.memoryData.fileSize} KB`);
      console.log(`  Avg records/event: ${Math.round(stats.memoryData.totalRecords / stats.memoryData.events)}`);
    } else {
      console.log(`  ❌ No memory data available`);
    }

    console.log('\n📄 COMBINED CSV:');
    if (stats.combinedCsv.exists) {
      console.log(`  Records: ${stats.combinedCsv.records}`);
      console.log(`  File size: ${stats.combinedCsv.fileSize} MB`);
      console.log(`  Required fields: ${stats.combinedCsv.hasRequiredFields ? '✅' : '❌'}`);
      console.log(`  Data quality: ${stats.combinedCsv.dataQuality?.quality || 'unknown'}`);
    } else {
      console.log(`  ❌ Combined CSV file not found`);
    }

    console.log('\n💾 PERFORMANCE:');
    console.log(`  Memory usage: ${stats.performance.memoryUsage.heapUsed} MB`);
    console.log(`  Uptime: ${stats.performance.uptime} minutes`);

    if (stats.recommendations.length > 0) {
      console.log('\n⚠️  RECOMMENDATIONS:');
      stats.recommendations.forEach(rec => {
        const icon = rec.priority === 'critical' ? '🚨' : 
                    rec.priority === 'high' ? '⚠️' : 'ℹ️';
        console.log(`  ${icon} ${rec.message}`);
      });
    } else {
      console.log('\n✅ All systems optimal');
    }

    console.log('\n===============================\n');
  }

  /**
   * Force cleanup of old events
   */
  async forceCleanup(maxAgeHours = 24) {
    try {
      const { cleanupOldEvents } = await import('../controllers/inventoryController.js');
      const cleaned = cleanupOldEvents(maxAgeHours);
      console.log(`🧹 Cleaned up ${cleaned} events older than ${maxAgeHours} hours`);
      return cleaned;
    } catch (error) {
      console.error(`❌ Cleanup failed: ${error.message}`);
      return 0;
    }
  }

  /**
   * Force generation of combined CSV
   */
  async forceCombinedGeneration() {
    try {
      const { forceCombinedCSVGeneration } = await import('../controllers/inventoryController.js');
      const result = forceCombinedCSVGeneration();
      
      if (result.success) {
        console.log(`✅ Generated combined CSV: ${result.recordCount} records from ${result.eventCount} events`);
      } else {
        console.log(`❌ Failed to generate combined CSV: ${result.message}`);
      }
      
      return result;
    } catch (error) {
      console.error(`❌ CSV generation failed: ${error.message}`);
      return { success: false, message: error.message };
    }
  }
}

export default CsvMonitor; 