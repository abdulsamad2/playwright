import { firefox, chromium, webkit } from "playwright";

export const BROWSERS = [
  { name: "firefox", engine: firefox },
  { name: "chromium", engine: chromium },
  // { name: "webkit", engine: webkit },
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
  { name: "Desktop Full HD", viewport: { width: 1920, height: 1080 } },
  { name: "Desktop 4K", viewport: { width: 3840, height: 2160 } },
  { name: 'MacBook Pro 16"', viewport: { width: 1728, height: 1117 } },
  { name: "MacBook Air M2", viewport: { width: 1512, height: 982 } },
  { name: "Dell XPS 15", viewport: { width: 1920, height: 1200 } },
  {
    name: "Lenovo ThinkPad X1 Carbon",
    viewport: { width: 1920, height: 1080 },
  },

  // Common Resolutions
  { name: "HD Ready", viewport: { width: 1280, height: 720 } },
  { name: "HD+", viewport: { width: 1366, height: 768 } },
  { name: "Full HD", viewport: { width: 1920, height: 1080 } },
  { name: "2K", viewport: { width: 2560, height: 1440 } },
  { name: "Ultrawide Monitor", viewport: { width: 3440, height: 1440 } },

  // Smaller Screens and Unique Sizes
  { name: "Small Laptop", viewport: { width: 1280, height: 800 } },
  { name: "Netbook", viewport: { width: 1024, height: 600 } },
  { name: "Large Desktop", viewport: { width: 2560, height: 1600 } },
];
