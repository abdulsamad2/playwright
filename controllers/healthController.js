import mongoose from "mongoose";
import scraperManager from "../scraperManager.js";

// Initialize scraper manager

export const checkHealth = (req, res) => {
  console.log(
    `Scraper running status in checkHealth: ${scraperManager.isRunning}`
  ); // Debug log
  res.json({
    status: "healthy",
    scraperRunning: scraperManager.isRunning,
    mongoConnection: mongoose.connection.readyState === 1,
  });
};