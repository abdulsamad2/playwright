import { firefox, chromium, webkit } from "playwright";
import { BrowserFingerprint } from "./browserFingerprint.js";

const BROWSERS = [
  { name: 'firefox', engine: firefox },
  // { name: 'chromium', engine: chromium },
  // { name: 'webkit', engine: webkit },
  // { name: 'edge', engine: webkit },
  // { name: 'safari', engine: webkit },

];

const DEVICES = [
  // // Mobile Devices - Android
  // { name: "Pixel 5", viewport: { width: 393, height: 851 }, isMobile: true },
  // { name: "Pixel 4", viewport: { width: 353, height: 745 }, isMobile: true },
  // { name: "Pixel 4a", viewport: { width: 393, height: 851 }, isMobile: true },
  // { name: "Pixel 3", viewport: { width: 393, height: 786 }, isMobile: true },
  // {
  //   name: "Samsung Galaxy S8+",
  //   viewport: { width: 360, height: 740 },
  //   isMobile: true,
  // },
  {
    name: "Samsung Galaxy S20 Ultra",
    viewport: { width: 412, height: 915 },
    isMobile: true,
  },
  // {
  //   name: "Galaxy Tab S4",
  //   viewport: { width: 712, height: 1138 },
  //   isMobile: true,
  // },

  // Mobile Devices - iOS
  // {
  //   name: "iPhone 13 Pro",
  //   viewport: { width: 390, height: 844 },
  //   isMobile: true,
  // },
  // { name: "iPhone 13", viewport: { width: 390, height: 844 }, isMobile: true },
  // { name: "iPhone 12", viewport: { width: 390, height: 844 }, isMobile: true },
  // { name: "iPhone 11", viewport: { width: 414, height: 896 }, isMobile: true },
  // { name: "iPhone XR", viewport: { width: 414, height: 896 }, isMobile: true },
  // { name: "iPhone SE", viewport: { width: 375, height: 667 }, isMobile: true },
  // {
  //   name: "iPad Pro 11",
  //   viewport: { width: 834, height: 1194 },
  //   isMobile: true,
  // },
  // { name: "iPad Mini", viewport: { width: 768, height: 1024 }, isMobile: true },

  // // Desktop Devices
  // { name: "Desktop Chrome", viewport: { width: 1920, height: 1080 } },
  { name: "Desktop Edge", viewport: { width: 1920, height: 1080 } },
  // { name: "Desktop Firefox", viewport: { width: 1280, height: 720 } },
  // { name: "Desktop Safari", viewport: { width: 1440, height: 900 } },

  // // Common Desktop Resolutions
  { name: "HD Desktop", viewport: { width: 1366, height: 768 } },
  // { name: "Full HD", viewport: { width: 1920, height: 1080 } },
  // { name: "2K Display", viewport: { width: 2560, height: 1440 } },
  // { name: "4K Display", viewport: { width: 3840, height: 2160 } },

  // // Laptop Displays
  // { name: "MacBook Air", viewport: { width: 1280, height: 800 } },
  // { name: "MacBook Pro 13", viewport: { width: 1440, height: 900 } },
  // { name: "MacBook Pro 16", viewport: { width: 1536, height: 960 } },
];

export class Scraper {
  constructor() {
    this.sessions = new Map();
    this.fingerprints = new Map();
    this.proxyUrls = [
      "http://142.173.189.42:8357",
      "http://142.173.189.91:8406",
    ];
    this.proxyUsername = "QY4TK";
    this.proxyPassword = "kxZUosyN";
    this.currentBrowserIndex = 0;
    this.currentDeviceIndex = 0;
  }

  getRandomProxy() {
    return this.proxyUrls[Math.floor(Math.random() * this.proxyUrls.length)];
  }
  getNextBrowser() {
    const browser = BROWSERS[this.currentBrowserIndex];
    this.currentBrowserIndex = (this.currentBrowserIndex + 1) % BROWSERS.length;
    return browser;
  }

  getNextDevice() {
    const device = DEVICES[this.currentDeviceIndex];
    this.currentDeviceIndex = (this.currentDeviceIndex + 1) % DEVICES.length;
    return device;
  }

  async initBrowser() {
    this.fingerprint = BrowserFingerprint.generate();
    const selectedBrowser = this.getNextBrowser();
    const selectedDevice = this.getNextDevice();

    console.log(
      `Using browser: ${selectedBrowser.name}, device: ${selectedDevice.name}`
    );

    const launchOptions = {
      proxy: {
        server: this.getRandomProxy(),
        username: this.proxyUsername,
        password: this.proxyPassword,
      },
      headless: true,
    };

    this.browser = await selectedBrowser.engine.launch(launchOptions);

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
        Accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
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
  }

  async handleTicketmasterChallenge(page) {
    try {
      const challengePresent = await page.evaluate(() => {
        return document.body.textContent.includes(
          "Your Browsing Activity Has Been Paused"
        );
      });

      if (challengePresent) {
        console.log("Detected Ticketmaster challenge, handling...");

        await this.simulateHumanBehavior(page);

        const buttons = await page.$$("button");
        for (const button of buttons) {
          const text = await button.textContent();
          if (
            text.toLowerCase().includes("continue") ||
            text.toLowerCase().includes("verify")
          ) {
            await this.simulateHumanBehavior(page);
            await button.click();
            break;
          }
        }

        await page
          .waitForLoadState("networkidle", { timeout: 30000 })
          .catch(() => {});

        const stillChallenged = await page.evaluate(() => {
          return document.body.textContent.includes(
            "Your Browsing Activity Has Been Paused"
          );
        });

        if (stillChallenged) {
          throw new Error("Challenge not resolved");
        }
      }
    } catch (error) {
      console.error("Error handling challenge:", error.message);
      throw error;
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
              title,
              dateTime,
              ticket: {
                section,
                ticketType,
                price,
              },
            };
          });

          return {
            eventInfo: {
              title: tickets[0]?.title,
              dateTime: tickets[0]?.dateTime,
            },
            tickets: tickets.map((t) => t.ticket),
          };
        });

        return data;
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
