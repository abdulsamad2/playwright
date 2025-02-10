import { firefox, chromium, webkit } from "playwright";
import { BrowserFingerprint } from "./browserFingerprint.js";
import { proxyArray } from "./proxyList.js";


const BROWSERS = [
  { name: "firefox", engine: firefox },
  // { name: "chromium", engine: chromium },
  // { name: "webkit", engine: webkit },
  // { name: "edge", engine: chromium },
  // { name: "safari", engine: webkit },
];

const DEVICES = [
  // Android Phones
  {
    name: "Pixel 8 Pro",
    viewport: { width: 412, height: 915 },
    isMobile: true,
  },
  { name: "Pixel 7", viewport: { width: 412, height: 892 }, isMobile: true },
  {
    name: "Samsung Galaxy S24 Ultra",
    viewport: { width: 412, height: 915 },
    isMobile: true,
  },
  {
    name: "Samsung Galaxy S23",
    viewport: { width: 393, height: 851 },
    isMobile: true,
  },
  {
    name: "Samsung Galaxy A54",
    viewport: { width: 390, height: 844 },
    isMobile: true,
  },
  { name: "OnePlus 11", viewport: { width: 412, height: 915 }, isMobile: true },
  {
    name: "Xiaomi 13 Pro",
    viewport: { width: 393, height: 851 },
    isMobile: true,
  },

  // iOS Phones
  {
    name: "iPhone 15 Pro Max",
    viewport: { width: 430, height: 932 },
    isMobile: true,
  },
  { name: "iPhone 14", viewport: { width: 390, height: 844 }, isMobile: true },
  {
    name: "iPhone 13 Mini",
    viewport: { width: 375, height: 812 },
    isMobile: true,
  },
  {
    name: "iPhone SE (3rd Gen)",
    viewport: { width: 375, height: 667 },
    isMobile: true,
  },

  // Android Tablets
  {
    name: "Samsung Galaxy Tab S9",
    viewport: { width: 800, height: 1280 },
    isMobile: true,
  },
  {
    name: "Lenovo Tab P11 Pro",
    viewport: { width: 1600, height: 2560 },
    isMobile: true,
  },
  {
    name: "Google Pixel Tablet",
    viewport: { width: 1600, height: 2560 },
    isMobile: true,
  },

  // iPads
  {
    name: 'iPad Pro 12.9" (6th Gen)',
    viewport: { width: 1024, height: 1366 },
    isMobile: true,
  },
  {
    name: "iPad Air (5th Gen)",
    viewport: { width: 820, height: 1180 },
    isMobile: true,
  },
  {
    name: "iPad Mini (6th Gen)",
    viewport: { width: 768, height: 1024 },
    isMobile: true,
  },

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


class Scraper {
  constructor() {
    this.sessions = new Map();
    this.fingerprints = new Map();
    this.proxyArray = proxyArray;
    this.currentProxyIndex = 0;
    this.currentBrowserIndex = 0;
    this.currentDeviceIndex = 0;
    this.failedProxies = new Set();
    this.failedBrowsers = new Set();
    this.lastRotationTime = Date.now();
    this.rotationCooldown = 5 * 60 * 1000; // 5 minutes cooldown
  }

  resetFailedResources() {
    const now = Date.now();
    if (now - this.lastRotationTime >= this.rotationCooldown) {
      this.failedProxies.clear();
      this.failedBrowsers.clear();
      this.lastRotationTime = now;
      console.log("Reset failed resources list");
    }
  }

  getNextViableProxy() {
    this.resetFailedResources();
    let attempts = 0;
    const maxAttempts = this.proxyArray.length;

    while (attempts < maxAttempts) {
      const proxy = this.proxyArray[this.currentProxyIndex];
      this.currentProxyIndex = (this.currentProxyIndex + 1) % this.proxyArray.length;

      if (!this.failedProxies.has(`${proxy.host}:${proxy.port}`)) {
        return proxy;
      }
      attempts++;
    }

    // If all proxies failed, reset and try again
    this.failedProxies.clear();
    return this.proxyArray[0];
  }

  getNextViableBrowser() {
    this.resetFailedResources();
    let attempts = 0;
    const maxAttempts = BROWSERS.length;

    while (attempts < maxAttempts) {
      const browser = BROWSERS[this.currentBrowserIndex];
      this.currentBrowserIndex = (this.currentBrowserIndex + 1) % BROWSERS.length;

      if (!this.failedBrowsers.has(browser.name)) {
        return browser;
      }
      attempts++;
    }

    // If all browsers failed, reset and try again
    this.failedBrowsers.clear();
    return BROWSERS[0];
  }

  markResourceAsFailed(proxy, browser) {
    if (proxy) {
      this.failedProxies.add(`${proxy.host}:${proxy.port}`);
      console.log(`Marked proxy ${proxy.host}:${proxy.port} as failed`);
    }
    if (browser) {
      this.failedBrowsers.add(browser.name);
      console.log(`Marked browser ${browser.name} as failed`);
    }
  }

  async initBrowser(forceRotation = false) {
    if (forceRotation) {
      await this.close();
    }

    this.fingerprint = BrowserFingerprint.generate();
    const selectedBrowser = this.getNextViableBrowser();
    const selectedDevice = this.getNextDevice();
    const selectedProxy = this.getNextViableProxy();

    console.log(
      `Initializing with browser: ${selectedBrowser.name}, device: ${selectedDevice.name}, proxy: ${selectedProxy.host}:${selectedProxy.port}`
    );

    try {
      const launchOptions = {
        proxy: {
          server: `${selectedProxy.host}:${selectedProxy.port}`,
          username: selectedProxy.username,
          password: selectedProxy.password,
        },
        headless: true,
      };

      this.browser = await selectedBrowser.engine.launch(launchOptions);
      this.currentBrowser = selectedBrowser;
      this.currentProxy = selectedProxy;

      const contextOptions = {
        viewport: selectedDevice.viewport,
        userAgent: BrowserFingerprint.generateUserAgent(this.fingerprint),
        deviceScaleFactor: this.fingerprint.devicePixelRatio,
        locale: this.fingerprint.language,
        timezoneId: "America/New_York",
        permissions: ["geolocation"],
        bypassCSP: true,
        ignoreHTTPSErrors: true,
        extraHTTPHeaders: {
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
          "Accept-Language": "en-US,en;q=0.9",
          "Accept-Encoding": "gzip, deflate, br",
          "Cache-Control": "max-age=0",
          Connection: "keep-alive",
          "Upgrade-Insecure-Requests": "1",
          "Sec-Ch-Ua": `"${this.fingerprint.browser.name}";v="${this.fingerprint.browser.version}"`,
          "Sec-Ch-Ua-Mobile": selectedDevice.isMobile ? "?1" : "?0",
          "Sec-Ch-Ua-Platform": `"${this.fingerprint.platform.name}"`,
          "Sec-Fetch-Dest": "document",
          "Sec-Fetch-Mode": "navigate",
          "Sec-Fetch-Site": "none",
          "Sec-Fetch-User": "?1",
        },
      };

      this.context = await this.browser.newContext(contextOptions);
      await this.context.setDefaultTimeout(30000);
      await this.context.setDefaultNavigationTimeout(60000);

      return true;
    } catch (error) {
      console.error(`Failed to initialize browser: ${error.message}`);
      this.markResourceAsFailed(selectedProxy, selectedBrowser);
      throw error;
    }
  }

  async handleTicketmasterChallenge(page) {
    try {
      const challengePresent = await page.evaluate(() => {
        return document.body.textContent.includes("Your Browsing Activity Has Been Paused");
      });

      if (challengePresent) {
        console.log("Detected Ticketmaster challenge, rotating resources...");
        this.markResourceAsFailed(this.currentProxy, this.currentBrowser);
        throw new Error("Challenge detected - forcing rotation");
      }
    } catch (error) {
      throw error;
    }
  }

  async scrape(url, options = {}) {
    const maxRetries = options.maxRetries || 3;
    let retries = 0;

    while (retries <= maxRetries) {
      let page = null;

      try {
        if (!this.browser || retries > 0) {
          await this.initBrowser(true);
        }

        console.log(`Attempt ${retries + 1} of ${maxRetries + 1}`);
        page = await this.context.newPage();

        console.log(`Navigating to ${url}...`);
        await page.goto(url, {
          waitUntil: "networkidle",
          timeout: 60000,
        });

        await this.handleTicketmasterChallenge(page);
        await this.simulateHumanBehavior(page);

        const title = await page.title();
        console.log("Page Title:", title);

        const data = await page.evaluate(() => {
          const tickets = Array.from(document.querySelectorAll(".sc-1cro0l1-0")).map((ticket) => {
            const section = ticket.querySelector('[data-bdd="quick-pick-item-desc"]')?.textContent?.trim();
            const ticketType = ticket.querySelector("div.sc-m796ke-6.ijdmtk > span")?.textContent?.trim();
            const price = ticket.querySelector('[data-bdd="quick-pick-price-button"]')?.textContent?.trim();
            return { section, ticketType, price };
          });

          return {
            eventInfo: {
              title: document.querySelector("h1")?.textContent?.trim() || "",
              dateTime: document.querySelector("#edp-event-header div.sc-1eku3jf-12.BANxv span > span")?.textContent?.trim() || "",
            },
            tickets: tickets,
          };
        });

        return JSON.stringify(data, null, 2);
      } catch (error) {
        retries++;
        console.error(`Attempt ${retries} failed:`, error.message);
        this.markResourceAsFailed(this.currentProxy, this.currentBrowser);

        if (retries <= maxRetries) {
          const delay = Math.pow(2, retries) * 1000 + Math.random() * 1000;
          console.log(`Waiting ${Math.round(delay / 1000)} seconds before retrying...`);
          await new Promise((r) => setTimeout(r, delay));
        } else {
          throw error;
        }
      } finally {
        if (page) {
          await page.close().catch(() => {});
        }
      }
    }
  }

  async simulateHumanBehavior(page) {
    const viewportSize = page.viewportSize();
    if (!viewportSize) return;

    try {
      await page.waitForTimeout(2000 + Math.random() * 3000);

      for (let i = 0; i < 3 + Math.random() * 5; i++) {
        const x = Math.floor(Math.random() * viewportSize.width);
        const y = Math.floor(Math.random() * viewportSize.height);

        await page.mouse.move(x, y, {
          steps: 10 + Math.floor(Math.random() * 20),
        });

        await page.waitForTimeout(500 + Math.random() * 1000);
      }

      await page.evaluate(() => {
        return new Promise((resolve) => {
          let steps = 0;
          const maxSteps = 5 + Math.floor(Math.random() * 5);

          const scroll = () => {
            if (steps >= maxSteps) {
              resolve();
              return;
            }

            const amount = Math.random() * 100 - 50;
            window.scrollBy({
              top: amount,
              behavior: "smooth",
            });

            steps++;
            setTimeout(scroll, 1000 + Math.random() * 2000);
          };

          scroll();
        });
      });

      await page.waitForTimeout(1000 + Math.random() * 2000);
    } catch (error) {
      console.error("Error in human behavior simulation:", error.message);
    }
  }

  async scrape(url, options = {}) {
    if (!this.browser) {
      await this.initBrowser();
    }

    const maxRetries = options.maxRetries || 3;
    let retries = 0;

    while (retries <= maxRetries) {
      let page = null;

      try {
        console.log(`Attempt ${retries + 1} of ${maxRetries + 1}`);

        page = await this.context.newPage();

        console.log(`Navigating to ${url}...`);
        await page.goto(url, {
          waitUntil: "networkidle",
          timeout: 60000,
        });

        await this.handleTicketmasterChallenge(page);
        await this.simulateHumanBehavior(page);

        const title = await page.title();
        console.log("Page Title:", title);

        const data = await page.evaluate(() => {
          const tickets = Array.from(
            document.querySelectorAll(".sc-1cro0l1-0")
          ).map((ticket) => {
            const section = ticket
              .querySelector('[data-bdd="quick-pick-item-desc"]')
              ?.textContent?.trim();
            const ticketType = ticket
              .querySelector("div.sc-m796ke-6.ijdmtk > span")
              ?.textContent?.trim();
            const price = ticket
              .querySelector('[data-bdd="quick-pick-price-button"]')
              ?.textContent?.trim();
            const title = document.querySelector("h1")?.textContent?.trim();
            const dateTime = document
              .querySelector(
                "#edp-event-header div.sc-1eku3jf-12.BANxv span > span"
              )
              ?.textContent?.trim();
            console.log(section, ticketType, price, title, dateTime);
            return {
              section,
              ticketType,
              price,
            };
          });

          return {
            eventInfo: {
              title: document.querySelector("h1")?.textContent?.trim() || "",
              dateTime:
                document
                  .querySelector(
                    "#edp-event-header div.sc-1eku3jf-12.BANxv span > span"
                  )
                  ?.textContent?.trim() || "",
            },
            tickets: tickets,
          };
        });

        // Return the data as a JSON string
        return JSON.stringify(data, null, 2);
      } catch (error) {
        retries++;
        console.error(`Attempt ${retries} failed:`, error.message);

        if (retries <= maxRetries) {
          const delay = Math.pow(2, retries) * 1000 + Math.random() * 1000;
          console.log(
            `Waiting ${Math.round(delay / 1000)} seconds before retrying...`
          );
          await new Promise((r) => setTimeout(r, delay));

          await this.context?.close().catch(() => {});
          await this.browser?.close().catch(() => {});
          await this.initBrowser();
        } else {
          throw error;
        }
      } finally {
        if (page) {
          await page.close().catch(() => {});
        }
      }
    }
  }

  async close() {
    await this.context?.close().catch(() => {});
    await this.browser?.close().catch(() => {});
  }
}

export default Scraper;