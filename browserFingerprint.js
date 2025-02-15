import { createHash } from "crypto";
import { firefox } from "playwright";

export class BrowserFingerprint {
  static platforms = [
    { name: "Windows", version: "10", arch: "x64" },
    { name: "Macintosh", version: "10_15_7", arch: "Intel" },
    { name: "Macintosh", version: "14_1", arch: "arm64" }, // For M1/M2 Macs
  ];

  static browsers = [
    { name: "Chrome", version: "120.0.0.0", engine: "chrome" },
    { name: "Firefox", version: "121.0", engine: firefox },
    { name: "Safari", version: "17.0", engine: "webkit" },
  ];

  static languages = ["en-US", "en-GB", "en-CA"];

  static devices = [
    // Modern Laptops
    {
      name: 'MacBook Pro 16"',
      viewport: { width: 1728, height: 1117 },
      deviceScaleFactor: 2,
      isMobile: false,
    },
    {
      name: 'MacBook Pro 14"',
      viewport: { width: 1512, height: 982 },
      deviceScaleFactor: 2,
      isMobile: false,
    },
    {
      name: "MacBook Air M2",
      viewport: { width: 1512, height: 982 },
      deviceScaleFactor: 2,
      isMobile: false,
    },
    {
      name: "MacBook Air M1",
      viewport: { width: 1440, height: 900 },
      deviceScaleFactor: 2,
      isMobile: false,
    },
    // Standard Resolutions
    {
      name: "Standard HD",
      viewport: { width: 1920, height: 1080 },
      deviceScaleFactor: 1,
      isMobile: false,
    },
    {
      name: "Standard QHD",
      viewport: { width: 2560, height: 1440 },
      deviceScaleFactor: 1,
      isMobile: false,
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

    // Add some randomization to memory and CPU cores
    const memoryOptions = [4, 8, 16, 32];
    const cpuOptions = [4, 6, 8, 10, 12];

    return {
      platform,
      browser,
      language,
      device,
      screen: device.viewport,
      colorDepth: 24,
      deviceMemory:
        memoryOptions[Math.floor(Math.random() * memoryOptions.length)],
      hardwareConcurrency:
        cpuOptions[Math.floor(Math.random() * cpuOptions.length)],
      timezone: "America/New_York",
      touchPoints: 0,
      devicePixelRatio: device.deviceScaleFactor,
      sessionId: createHash("sha256")
        .update(Math.random().toString())
        .digest("hex"),
    };
  }

  static generateUserAgent(fingerprint) {
    const { platform, browser } = fingerprint;

    switch (browser.name.toLowerCase()) {
      case "chrome":
        return `Mozilla/5.0 (${
          platform.name === "Windows"
            ? "Windows NT 10.0; Win64; x64"
            : "Macintosh; " +
              (platform.arch === "arm64" ? "ARM " : "") +
              "Intel Mac OS X " +
              platform.version
        }) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${
          browser.version
        } Safari/537.36`;

      case "firefox":
        return `Mozilla/5.0 (${
          platform.name === "Windows"
            ? "Windows NT 10.0; Win64; x64"
            : "Macintosh; " +
              (platform.arch === "arm64" ? "ARM " : "") +
              "Intel Mac OS X " +
              platform.version
        }; rv:109.0) Gecko/20100101 Firefox/${browser.version}`;

      case "safari":
        return `Mozilla/5.0 (${
          platform.name === "Windows"
            ? "Windows NT 10.0; Win64; x64"
            : "Macintosh; " +
              (platform.arch === "arm64" ? "ARM " : "") +
              "Intel Mac OS X " +
              platform.version
        }) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/${
          browser.version
        } Safari/605.1.15`;

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
    hasTouch: false,
    isMobile: false,
    extraHTTPHeaders: {
      "Accept-Language": `${config.locale},en;q=0.9`,
      DNT: "1",
    },
  };
}
