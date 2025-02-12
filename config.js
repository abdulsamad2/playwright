import { firefox, chromium, webkit } from "playwright";

export const BROWSERS = [
  { name: "firefox", engine: firefox },
  // { name: "chromium", engine: chromium },
  // // { name: "webkit", engine: webkit },
  // { name: "edge", engine: chromium },
  // { name: "safari", engine: webkit },
];

export const DEVICES = [
  // // // Android Phones
  // {
  //   name: "Pixel 8 Pro",
  //   viewport: { width: 412, height: 915 },
  //   isMobile: true,
  // },
  // { name: "Pixel 7", viewport: { width: 412, height: 892 }, isMobile: true },
  // {
  //   name: "Samsung Galaxy S24 Ultra",
  //   viewport: { width: 412, height: 915 },
  //   isMobile: true,
  // },
  // {
  //   name: "Samsung Galaxy S23",
  //   viewport: { width: 393, height: 851 },
  //   isMobile: true,
  // },
  // {
  //   name: "Samsung Galaxy A54",
  //   viewport: { width: 390, height: 844 },
  //   isMobile: true,
  // },
  // { name: "OnePlus 11", viewport: { width: 412, height: 915 }, isMobile: true },
  // {
  //   name: "Xiaomi 13 Pro",
  //   viewport: { width: 393, height: 851 },
  //   isMobile: true,
  // },

  // // iOS Phones
  // {
  //   name: "iPhone 15 Pro Max",
  //   viewport: { width: 430, height: 932 },
  //   isMobile: true,
  // },
  // { name: "iPhone 14", viewport: { width: 390, height: 844 }, isMobile: true },
  // {
  //   name: "iPhone 13 Mini",
  //   viewport: { width: 375, height: 812 },
  //   isMobile: true,
  // },
  // {
  //   name: "iPhone SE (3rd Gen)",
  //   viewport: { width: 375, height: 667 },
  //   isMobile: true,
  // },

  // // Android Tablets
  // {
  //   name: "Samsung Galaxy Tab S9",
  //   viewport: { width: 800, height: 1280 },
  //   isMobile: true,
  // },
  // {
  //   name: "Lenovo Tab P11 Pro",
  //   viewport: { width: 1600, height: 2560 },
  //   isMobile: true,
  // },
  // {
  //   name: "Google Pixel Tablet",
  //   viewport: { width: 1600, height: 2560 },
  //   isMobile: true,
  // },

  // iPads
  // {
  //   name: 'iPad Pro 12.9" (6th Gen)',
  //   viewport: { width: 1024, height: 1366 },
  //   isMobile: true,
  // },
  // {
  //   name: "iPad Air (5th Gen)",
  //   viewport: { width: 820, height: 1180 },
  //   isMobile: true,
  // },
  // {
  //   name: "iPad Mini (6th Gen)",
  //   viewport: { width: 768, height: 1024 },
  //   isMobile: true,
  // },

  // Desktop Monitors and Laptops
  // High-End Desktop Monitors
  { name: "Desktop Full HD", viewport: { width: 1920, height: 1080 } },
  { name: "Desktop 4K", viewport: { width: 3840, height: 2160 } },
  { name: "Desktop 5K", viewport: { width: 5120, height: 2880 } },
  { name: "Desktop 8K", viewport: { width: 7680, height: 4320 } },
  { name: "Ultrawide Monitor", viewport: { width: 3440, height: 1440 } },
  { name: "Super Ultrawide", viewport: { width: 5120, height: 1440 } },

  // Modern Laptops
  { name: 'MacBook Pro 16"', viewport: { width: 1728, height: 1117 } },
  { name: 'MacBook Pro 14"', viewport: { width: 1512, height: 982 } },
  { name: "MacBook Air M2", viewport: { width: 1512, height: 982 } },
  { name: "MacBook Air M1", viewport: { width: 1440, height: 900 } },
  { name: "Dell XPS 15", viewport: { width: 1920, height: 1200 } },
  { name: "Dell XPS 13", viewport: { width: 1920, height: 1200 } },
  {
    name: "Lenovo ThinkPad X1 Carbon",
    viewport: { width: 1920, height: 1080 },
  },
  { name: "Razer Blade 15", viewport: { width: 1920, height: 1080 } },
  { name: "Surface Laptop 4", viewport: { width: 2256, height: 1504 } },
  { name: "Surface Book 3", viewport: { width: 3000, height: 2000 } },
  { name: "HP Spectre x360", viewport: { width: 1920, height: 1080 } },
  { name: "ASUS ROG Zephyrus", viewport: { width: 2560, height: 1440 } },

  // Gaming Monitors
  { name: "Gaming 1080p 240Hz", viewport: { width: 1920, height: 1080 } },
  { name: "Gaming 1440p 165Hz", viewport: { width: 2560, height: 1440 } },
  { name: "Gaming 4K 144Hz", viewport: { width: 3840, height: 2160 } },
  { name: "Ultrawide Gaming", viewport: { width: 3440, height: 1440 } },

  // Standard Resolutions
  { name: "HD Ready", viewport: { width: 1280, height: 720 } },
  { name: "HD+", viewport: { width: 1366, height: 768 } },
  { name: "Full HD", viewport: { width: 1920, height: 1080 } },
  { name: "2K QHD", viewport: { width: 2560, height: 1440 } },
  { name: "4K UHD", viewport: { width: 3840, height: 2160 } },
  { name: "5K", viewport: { width: 5120, height: 2880 } },
  { name: "8K UHD", viewport: { width: 7680, height: 4320 } },

  // Professional Monitors
  { name: "Professional 4K", viewport: { width: 4096, height: 2160 } },
  { name: "Professional 5K", viewport: { width: 5120, height: 2880 } },
  { name: "Professional DCI 4K", viewport: { width: 4096, height: 2160 } },
  { name: "Professional 6K", viewport: { width: 6016, height: 3384 } },

  // Tablets and iPads
  { name: "iPad Pro 12.9", viewport: { width: 2048, height: 2732 } },
  { name: "iPad Pro 11", viewport: { width: 1668, height: 2388 } },
  { name: "iPad Air", viewport: { width: 1640, height: 2360 } },
  { name: "iPad Mini", viewport: { width: 1488, height: 2266 } },
  { name: "Surface Pro 8", viewport: { width: 2880, height: 1920 } },
  { name: "Galaxy Tab S8 Ultra", viewport: { width: 2960, height: 1848 } },

  // Mobile Devices (Landscape)
  { name: "iPhone 15 Pro Max", viewport: { width: 2796, height: 1290 } },
  { name: "iPhone 15 Pro", viewport: { width: 2556, height: 1179 } },
  { name: "iPhone 14 Plus", viewport: { width: 2778, height: 1284 } },
  { name: "Samsung S24 Ultra", viewport: { width: 3120, height: 1440 } },
  { name: "Samsung S24+", viewport: { width: 3120, height: 1440 } },
  { name: "Google Pixel 8 Pro", viewport: { width: 2992, height: 1344 } },

  // Legacy and Special Resolutions
  { name: "SXGA", viewport: { width: 1280, height: 1024 } },
  { name: "WXGA", viewport: { width: 1280, height: 800 } },
  { name: "WUXGA", viewport: { width: 1920, height: 1200 } },
  { name: "WQXGA", viewport: { width: 2560, height: 1600 } },
  { name: "4K DCI", viewport: { width: 4096, height: 2160 } },

  // Uncommon but Supported Resolutions
  { name: "Square Monitor", viewport: { width: 1920, height: 1920 } },
  { name: "Portrait Display", viewport: { width: 1080, height: 1920 } },
  { name: "Ultra-Portrait", viewport: { width: 1440, height: 3040 } },
  { name: "Legacy 4:3", viewport: { width: 1600, height: 1200 } },
  { name: "Legacy 5:4", viewport: { width: 1280, height: 1024 } },

  // Extreme Resolutions
  { name: "Triple 4K", viewport: { width: 11520, height: 2160 } },
  { name: "Dual 8K", viewport: { width: 15360, height: 4320 } },
  { name: "16K", viewport: { width: 15360, height: 8640 } },

  // Mini Displays
  { name: "Small Netbook", viewport: { width: 1024, height: 600 } },
  { name: "Mini Display", viewport: { width: 800, height: 600 } },
  { name: "Compact Screen", viewport: { width: 1024, height: 768 } },
];
