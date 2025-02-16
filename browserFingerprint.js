import { createHash } from "crypto";
import { firefox } from "playwright";

export class BrowserFingerprint {
  static platforms = [
    { name: "Android", version: "13.0", arch: "aarch64" },
    { name: "Android", version: "14.0", arch: "aarch64" },
    { name: "iPhone", version: "17_1", arch: "arm64" },
    { name: "iPad", version: "17_1", arch: "arm64" },
  ];

  static browsers = [
    { name: "Chrome", version: "120.0.0.0", engine: "chrome" },
    { name: "Firefox", version: "121.0", engine: firefox },
    { name: "Safari", version: "17.0", engine: "webkit" },
  ];

  static languages = ["en-US", "en-GB", "en-CA"];

  static devices = [
    // Android Phones
    {
      name: "Pixel 8 Pro",
      viewport: { width: 412, height: 915 },
      deviceScaleFactor: 3.5,
      isMobile: true,
    },
    {
      name: "Samsung Galaxy S24 Ultra",
      viewport: { width: 412, height: 915 },
      deviceScaleFactor: 3.5,
      isMobile: true,
    },
    {
      name: "Samsung Galaxy S23",
      viewport: { width: 393, height: 851 },
      deviceScaleFactor: 3,
      isMobile: true,
    },
    {
      name: "Samsung Galaxy A54",
      viewport: { width: 390, height: 844 },
      deviceScaleFactor: 2.75,
      isMobile: true,
    },
    // iOS Phones
    {
      name: "iPhone 15 Pro Max",
      viewport: { width: 430, height: 932 },
      deviceScaleFactor: 3,
      isMobile: true,
    },
    {
      name: "iPhone 14",
      viewport: { width: 390, height: 844 },
      deviceScaleFactor: 3,
      isMobile: true,
    },
    {
      name: "iPhone 13 Mini",
      viewport: { width: 375, height: 812 },
      deviceScaleFactor: 3,
      isMobile: true,
    },
    // Tablets
    {
      name: "Samsung Galaxy Tab S9",
      viewport: { width: 800, height: 1280 },
      deviceScaleFactor: 2,
      isMobile: true,
    },
    {
      name: 'iPad Pro 12.9" (6th Gen)',
      viewport: { width: 1024, height: 1366 },
      deviceScaleFactor: 2,
      isMobile: true,
    },
    {
      name: "iPad Air (5th Gen)",
      viewport: { width: 820, height: 1180 },
      deviceScaleFactor: 2,
      isMobile: true,
    },
  ];

  static generate() {
    const platform =
      this.platforms[Math.floor(Math.random() * this.platforms.length)];
    const browser =
      this.browsers[Math.floor(Math.random() * this.browsers.length)];
    const language =
      this.languages[Math.floor(Math.random() * this.languages.length)];
    const device =
      this.devices[Math.floor(Math.random() * this.devices.length)];

    // Mobile-specific memory and CPU configurations
    const memoryOptions = [2, 4, 6, 8];
    const cpuOptions = [4, 6, 8];

    return {
      platform,
      browser,
      language,
      device,
      screen: device.viewport,
      colorDepth: 32,
      deviceMemory:
        memoryOptions[Math.floor(Math.random() * memoryOptions.length)],
      hardwareConcurrency:
        cpuOptions[Math.floor(Math.random() * cpuOptions.length)],
      timezone: "America/New_York",
      touchPoints: 5,
      devicePixelRatio: device.deviceScaleFactor,
      sessionId: createHash("sha256")
        .update(Math.random().toString())
        .digest("hex"),
    };
  }

  static generateUserAgent(fingerprint) {
    const { platform, browser, device } = fingerprint;

    // Mobile-specific user agent strings
    switch (browser.name.toLowerCase()) {
      case "chrome":
        if (platform.name === "Android") {
          return `Mozilla/5.0 (Linux; Android ${platform.version}; ${device.name}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${browser.version} Mobile Safari/537.36`;
        } else {
          return `Mozilla/5.0 (${platform.name}; CPU ${platform.name} OS ${platform.version} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/${browser.version} Mobile/15E148 Safari/604.1`;
        }

      case "firefox":
        if (platform.name === "Android") {
          return `Mozilla/5.0 (Android ${platform.version}; Mobile; rv:${browser.version}) Gecko/${browser.version} Firefox/${browser.version}`;
        } else {
          return `Mozilla/5.0 (${platform.name}; CPU ${platform.name} OS ${platform.version} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/${browser.version} Mobile/15E148 Safari/605.1.15`;
        }

      case "safari":
        return `Mozilla/5.0 (${platform.name}; CPU ${platform.name} OS ${platform.version} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/${browser.version} Mobile/15E148 Safari/604.1`;

      default:
        return "";
    }
  }

  static getRandomBrowserConfig() {
    const fingerprint = this.generate();
    return {
      fingerprint,
      userAgent: this.generateUserAgent(fingerprint),
      engine: fingerprint.browser.engine,
      viewport: fingerprint.device.viewport,
      deviceScaleFactor: fingerprint.device.deviceScaleFactor,
      locale: fingerprint.language,
      timezone: fingerprint.timezone,
      isMobile: true,
      hasTouch: true,
    };
  }
}

export function getBrowserLaunchOptions(config, proxy) {
  const proxyUrl = new URL(`http://${proxy.proxy}`);

  return {
    headless: true,
    proxy: {
      server: `http://${proxyUrl.hostname}:${proxyUrl.port || 80}`,
      username: proxy.username,
      password: proxy.password,
    },
    args: [
      "--disable-dev-shm-usage",
      "--disable-setuid-sandbox",
      "--no-sandbox",
      "--disable-gpu",
      "--disable-software-rasterizer",
      `--window-size=${config.viewport.width},${config.viewport.height}`,
      "--enable-touch-events",
    ],
  };
}

export function getContextOptions(config) {
  return {
    viewport: config.viewport,
    userAgent: config.userAgent,
    deviceScaleFactor: config.deviceScaleFactor,
    locale: config.locale,
    timezoneId: config.timezone,
    colorScheme: "light",
    bypassCSP: true,
    acceptDownloads: false,
    ignoreHTTPSErrors: true,
    javaScriptEnabled: true,
    hasTouch: true,
    isMobile: true,
    extraHTTPHeaders: {
      "Accept-Language": `${config.locale},en;q=0.9`,
      DNT: "1",
    },
  };
}
