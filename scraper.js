import { firefox, chromium, webkit } from "playwright";
import { BrowserFingerprint } from "./browserFingerprint.js";
import { proxyArray } from "./proxyList.js";
import fs from "fs";
import { BROWSERS, DEVICES } from "./config.js";

class Scraper {
  constructor() {
    this.sessions = new Map();
    this.fingerprints = new Map();
    this.proxyArray = proxyArray;
    this.currentProxyIndex = 0;
    this.currentBrowserIndex = 0;
    this.currentDeviceIndex = 0;
  }

  getRandomProxy() {
    return this.proxyArray[Math.floor(Math.random() * this.proxyArray.length)];
  }

  getNextProxy() {
    const proxy = this.proxyArray[this.currentProxyIndex];
    this.currentProxyIndex =
      (this.currentProxyIndex + 1) % this.proxyArray.length;
    return proxy;
  }

  getProxiesByType(type) {
    return this.proxyArray.filter((proxy) => proxy.host.includes(type));
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
    const selectedProxy = this.getNextProxy();

    console.log(
      `Using browser: ${selectedBrowser.name}, device: ${selectedDevice.name}, proxy: ${selectedProxy.host}:${selectedProxy.port}`
    );

    const launchOptions = {
      proxy: {
        server: `${selectedProxy.host}:${selectedProxy.port}`,
        username: selectedProxy.username,
        password: selectedProxy.password,
      },
      headless: false,
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
        // click on button
     const data = await page.evaluate(async () => {
       const SELECTORS = {
         ACCEPT_BUTTON: '[data-bdd="accept-modal-accept-button"]',
         SVG_MAP:
           "#map-container > div.zoomer > div.zoomer__controls > button.zoomer__control--zoomin",
         SEAT: 'circle[data-component="svg__seat"]',
         EVENT_TITLE: "h1",
         EVENT_DATE: "#edp-event-header div.sc-1eku3jf-12.BANxv span > span",
         ROW: 'g[data-component="svg__row"]',
         SECTION: 'g[data-component="svg__block"]',
         PRICE_POPUP: ".standard-admission",
         TOOLTIP: '[role="tooltip"]',
       };

       const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

       const simulateHover = async (element) => {
         // Create mouse events
         const events = [
           new MouseEvent("pointerover", { bubbles: true }),
           new MouseEvent("mouseenter", { bubbles: true }),
           new MouseEvent("mouseover", { bubbles: true }),
           new MouseEvent("pointermove", { bubbles: true }),
           new MouseEvent("mousemove", { bubbles: true }),
         ];

         // Dispatch all events
         for (const event of events) {
           element.dispatchEvent(event);
         }

         // Wait for tooltip to appear
         await sleep(150);
       };

       const getPriceFromTooltip = () => {
         // Try multiple selectors to find price
         const selectors = [
           ".standard-admission",
           '[role="tooltip"] .standard-admission',
           '[role="tooltip"] [class*="price"]',
           '[class*="tooltip"] [class*="price"]',
           '[class*="popup"] [class*="price"]',
         ];

         for (const selector of selectors) {
           const element = document.querySelector(selector);
           if (element) {
             const text = element.textContent;
             const priceMatch = text.match(/\$(\d+(\.\d{2})?)/);
             if (priceMatch) return parseFloat(priceMatch[1]);
           }
         }

         return null;
       };

       const scrapeSeats = async () => {
         try {
           // Handle accept button
           try {
             const acceptButton = await waitForElement(
               SELECTORS.ACCEPT_BUTTON,
               1000
             );
             if (acceptButton) {
               acceptButton.click();
               await sleep(200);
             }
           } catch (e) {}

           // Handle map zoom
           const svg = await waitForElement(SELECTORS.SVG_MAP);
           if (!svg) throw new Error("SVG map not found");

           for (let i = 0; i < 5; i++) {
             svg.click();
             await sleep(200);
           }

           await waitForElement(SELECTORS.SEAT, 1000);

           // Get sections and rows
           const seatElements = document.querySelectorAll(SELECTORS.SEAT);
           const sections = new Map();
           const rows = new Map();

           seatElements.forEach((seat) => {
             const sectionElement = seat.closest(SELECTORS.SECTION);
             const rowElement = seat.closest(SELECTORS.ROW);

             if (sectionElement && !sections.has(sectionElement)) {
               sections.set(
                 sectionElement,
                 sectionElement.getAttribute("data-section-name")
               );
             }
             if (rowElement && !rows.has(rowElement)) {
               rows.set(rowElement, rowElement.getAttribute("data-row-name"));
             }
           });

           // Batch process seats with improved price scraping
           const seats = [];
           const availableSeats = Array.from(seatElements).filter((seat) =>
             seat.classList.contains("is-available")
           );

           // Process seats in smaller batches to avoid overwhelming the UI
           const BATCH_SIZE = 5;
           for (let i = 0; i < availableSeats.length; i += BATCH_SIZE) {
             const batch = availableSeats.slice(i, i + BATCH_SIZE);

             // Process each seat in the batch
             for (const seat of batch) {
               const sectionElement = seat.closest(SELECTORS.SECTION);
               const rowElement = seat.closest(SELECTORS.ROW);

               // Clear any existing tooltips
               document
                 .querySelectorAll('[role="tooltip"]')
                 .forEach((el) => el.remove());

               // Try to get price multiple times if needed
               let price = null;
               let attempts = 0;
               while (!price && attempts < 3) {
                 await simulateHover(seat);
                 price = getPriceFromTooltip();
                 attempts++;
                 if (!price) await sleep(100); // Wait before retry
               }

               if (price) {
                 seats.push({
                   id: seat.getAttribute("id"),
                   seatNumber: seat.getAttribute("data-seat-name"),
                   row: rows.get(rowElement),
                   section: sections.get(sectionElement),
                   isAvailable: true,
                   price,
                 });
               }
             }

             // Small delay between batches
             await sleep(100);
           }

           // Rest of your code for grouping seats and creating consecutive groups...
           const validSeats = seats.filter(
             (seat) => seat.id && seat.seatNumber
           );

           const groupedSeats = {};
           validSeats.forEach((seat) => {
             if (!groupedSeats[seat.section]) {
               groupedSeats[seat.section] = {};
             }
             if (!groupedSeats[seat.section][seat.row]) {
               groupedSeats[seat.section][seat.row] = [];
             }
             groupedSeats[seat.section][seat.row].push(seat);
           });

           const consecutiveGroups = [];

           Object.entries(groupedSeats).forEach(([section, rows]) => {
             Object.entries(rows).forEach(([row, seats]) => {
               const sortedSeats = seats.sort(
                 (a, b) => parseInt(a.seatNumber) - parseInt(b.seatNumber)
               );
               let currentGroup = [sortedSeats[0]];

               for (let i = 1; i < sortedSeats.length; i++) {
                 const currentSeat = sortedSeats[i];
                 const previousSeat = sortedSeats[i - 1];

                 if (
                   parseInt(currentSeat.seatNumber) ===
                   parseInt(previousSeat.seatNumber) + 1
                 ) {
                   currentGroup.push(currentSeat);
                 } else {
                   if (currentGroup.length >= 2) {
                     consecutiveGroups.push({
                       section,
                       row,
                       seatCount: currentGroup.length,
                       seatRange: `${currentGroup[0].seatNumber} to ${
                         currentGroup[currentGroup.length - 1].seatNumber
                       }`,
                       seats: currentGroup.map((s) => ({
                         number: s.seatNumber,
                         price: s.price,
                       })),
                     });
                   }
                   currentGroup = [currentSeat];
                 }
               }

               if (currentGroup.length >= 2) {
                 consecutiveGroups.push({
                   section,
                   row,
                   seatCount: currentGroup.length,
                   seatRange: `${currentGroup[0].seatNumber} to ${
                     currentGroup[currentGroup.length - 1].seatNumber
                   }`,
                   seats: currentGroup.map((s) => ({
                     number: s.seatNumber,
                     price: s.price,
                   })),
                 });
               }
             });
           });

           const [eventTitle, eventDate] = await Promise.all([
             document
               .querySelector(SELECTORS.EVENT_TITLE)
               ?.textContent?.trim() || "",
             document
               .querySelector(SELECTORS.EVENT_DATE)
               ?.textContent?.trim() || "",
           ]);

           return {
             eventInfo: {
               title: eventTitle,
               dateTime: eventDate,
             },
             seatingInfo: {
               availableSeats: validSeats.length,
               consecutiveGroups,
               seats: validSeats,
             },
           };
         } catch (error) {
           console.error("Error during scraping:", error);
           throw error;
         }
       };

       // waitForElement function remains the same
       const waitForElement = (selector, timeout = 5000) => {
         return new Promise((resolve, reject) => {
           const element = document.querySelector(selector);
           if (element) {
             resolve(element);
             return;
           }

           const observer = new MutationObserver(() => {
             const element = document.querySelector(selector);
             if (element) {
               observer.disconnect();
               resolve(element);
             }
           });

           observer.observe(document.body, {
             childList: true,
             subtree: true,
           });

           setTimeout(() => {
             observer.disconnect();
             resolve(null);
           }, timeout);
         });
       };

       return scrapeSeats();
     });
        console.log("Data:", data);
        const jsonString = JSON.stringify(data, null, 2);
        fs.writeFileSync("result.json", jsonString);
        // save to mongodb using mongoose 
        return jsonString;
        // Execute the scraping
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
