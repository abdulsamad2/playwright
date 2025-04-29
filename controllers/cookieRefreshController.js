import { CookieRefresh } from '../models/cookieRefreshModel.js';
import CookieRefreshTracker from '../helpers/CookieRefreshTracker.js';

/**
 * Get statistics about cookie refresh operations
 * @param {object} req - Express request object
 * @param {object} res - Express response object
 */
export const getCookieRefreshStats = async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 100;
    const stats = await CookieRefreshTracker.getStats(limit);
    
    res.json({
      success: true,
      data: stats
    });
  } catch (error) {
    console.error('Error getting cookie refresh stats:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
};

/**
 * Get recent cookie refresh operations
 * @param {object} req - Express request object
 * @param {object} res - Express response object
 */
export const getRecentRefreshes = async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 20;
    const page = parseInt(req.query.page) || 1;
    const skip = (page - 1) * limit;
    
    const status = req.query.status || null;
    
    // Build query
    const query = {};
    if (status) {
      query.status = status;
    }
    
    // Get total count
    const total = await CookieRefresh.countDocuments(query);
    
    // Get refreshes
    const refreshes = await CookieRefresh.find(query)
      .sort({ startTime: -1 })
      .skip(skip)
      .limit(limit);
    
    res.json({
      success: true,
      data: {
        refreshes,
        pagination: {
          total,
          page,
          limit,
          pages: Math.ceil(total / limit)
        }
      }
    });
  } catch (error) {
    console.error('Error getting recent refreshes:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
};

/**
 * Trigger a manual cookie refresh
 * @param {object} req - Express request object
 * @param {object} res - Express response object
 */
export const triggerRefresh = async (req, res) => {
  try {
    // Import refreshCookiesPeriodically function dynamically
    const { refreshCookiesPeriodically } = await import('../scraper.js');
    
    // Start the refresh process
    res.json({
      success: true,
      message: 'Cookie refresh triggered'
    });
    
    // Run the refresh process in the background
    refreshCookiesPeriodically().catch(error => {
      console.error('Error in manual cookie refresh:', error);
    });
  } catch (error) {
    console.error('Error triggering cookie refresh:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
};

/**
 * Get a specific cookie refresh record
 * @param {object} req - Express request object
 * @param {object} res - Express response object
 */
export const getRefreshDetails = async (req, res) => {
  try {
    const { refreshId } = req.params;
    
    const refresh = await CookieRefresh.findOne({ refreshId });
    
    if (!refresh) {
      return res.status(404).json({
        success: false,
        error: 'Cookie refresh record not found'
      });
    }
    
    res.json({
      success: true,
      data: refresh
    });
  } catch (error) {
    console.error('Error getting refresh details:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
}; 