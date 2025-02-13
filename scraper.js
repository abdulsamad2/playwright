import { firefox, chromium, webkit } from "playwright";
import { BrowserFingerprint } from "./browserFingerprint.js";
import { proxyArray } from "./proxyList.js";
import { BROWSERS, DEVICES } from "./config.js";

class Scraper {
  constructor() {
    this.sessions = new Map();
    this.fingerprints = new Map();
    this.proxyArray = proxyArray;
    this.currentProxyIndex = 0;
    this.currentBrowserIndex = 0;
    this.currentDeviceIndex = 0;
    this.lastKnownSeatCount = null;
    this.lastFullScrapeTime = null;
    this.FULL_SCRAPE_INTERVAL = 120 * 60 * 1000; // 15 minutes
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
    const CHALLENGE_TIMEOUT = 10000; // 10 seconds timeout for challenge
    const startTime = Date.now();

    try {
      const challengePresent = await page.evaluate(() => {
        return document.body.textContent.includes(
          "Your Browsing Activity Has Been Paused"
        );
      });

      if (challengePresent) {
        console.log(
          "Detected Ticketmaster challenge, attempting quick resolution..."
        );

        // Quick human behavior simulation
        await page.waitForTimeout(1000 + Math.random() * 1000);
        const viewportSize = page.viewportSize();
        if (viewportSize) {
          await page.mouse.move(
            Math.floor(Math.random() * viewportSize.width),
            Math.floor(Math.random() * viewportSize.height),
            { steps: 5 }
          );
        }

        // Try to find and click the button
        const buttons = await page.$$("button");
        let buttonClicked = false;

        for (const button of buttons) {
          if (Date.now() - startTime > CHALLENGE_TIMEOUT) {
            console.log("Challenge handling timeout - will switch browser");
            throw new Error("Challenge timeout");
          }

          const text = await button.textContent();
          if (
            text.toLowerCase().includes("continue") ||
            text.toLowerCase().includes("verify")
          ) {
            await button.click();
            buttonClicked = true;
            break;
          }
        }

        if (!buttonClicked) {
          throw new Error("Could not find challenge button");
        }

        // Quick check if challenge is resolved
        await page.waitForTimeout(2000);

        const stillChallenged = await page.evaluate(() => {
          return document.body.textContent.includes(
            "Your Browsing Activity Has Been Paused"
          );
        });

        if (stillChallenged) {
          console.log("Challenge not resolved quickly - switching browser");
          throw new Error("Challenge not resolved");
        }
      }
    } catch (error) {
      console.error("Challenge handling failed:", error.message);
      // Close current browser and create new one with different fingerprint
      await this.close();
      await this.initBrowser();
      throw new Error("Switching browser due to challenge");
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

  async getAvailableSeatCount(page) {
    return await page.evaluate(() => {
      const seats = document.querySelectorAll(
        'circle[data-component="svg__seat"].is-available'
      );
      return seats.length;
    });
  }
  async performQuickCheck(page) {
    console.log("Performing quick availability check...");
    const startTime = Date.now();

    try {
      await this.handleTicketmasterChallenge(page);

      // Define selectors
      const SELECTORS = {
        SVG_MAP:
          "#map-container > div.zoomer > div.zoomer__controls > button.zoomer__control--zoomin",
        SEATS: 'circle[data-component="svg__seat"]',
        ACCEPT_BUTTON: '[data-bdd="accept-modal-accept-button"]',
      };

      // Handle accept button if present
      try {
        const acceptButton = await page.waitForSelector(
          SELECTORS.ACCEPT_BUTTON,
          { timeout: 2000 }
        );
        if (acceptButton) {
          await acceptButton.click();
          await page.waitForTimeout(200);
        }
      } catch (e) {
        // Accept button not found, continue
      }

      // Wait for zoom button with shorter timeout first
      console.log("Waiting for zoom button...");
      const zoomButton = await page
        .waitForSelector(SELECTORS.SVG_MAP, {
          timeout: 10000,
          state: "visible",
        })
        .catch(() => null);

      if (!zoomButton) {
        console.log("Zoom button not immediately visible, waiting longer...");
        await page.waitForSelector(SELECTORS.SVG_MAP, {
          timeout: 30000,
          state: "visible",
        });
      }

      // Perform zooming with verification
      console.log("Starting zoom process...");
      let zoomAttempts = 0;
      const maxZoomAttempts = 3;

      while (zoomAttempts < maxZoomAttempts) {
        try {
          // Click zoom button multiple times with verification
          for (let i = 0; i < 3; i++) {
            await page.click(SELECTORS.SVG_MAP);
            await page.waitForTimeout(200);
          }
          break; // Break if zooming successful
        } catch (error) {
          zoomAttempts++;
          console.log(`Zoom attempt ${zoomAttempts} failed, retrying...`);
          await page.waitForTimeout(1000);

          if (zoomAttempts === maxZoomAttempts) {
            console.error("Failed to zoom after multiple attempts");
            throw new Error("Zoom operation failed");
          }
        }
      }

      // Wait for seats to be visible after zooming
      console.log("Waiting for seats to be visible after zoom...");
      await page.waitForSelector(SELECTORS.SEATS, {
        timeout: 10000,
        state: "visible",
      });

      // Get seat count
      const seatCount = await this.getAvailableSeatCount(page);

      const checkDuration = ((Date.now() - startTime) / 1000).toFixed(2);
      console.log(
        `Quick check completed in ${checkDuration}s. Available seats: ${seatCount}`
      );

      return {
        availableSeats: seatCount,
        quickCheckDuration: checkDuration,
      };
    } catch (error) {
      console.error("Error during quick check:", error.message);
      throw error;
    }
  }

  async performFullScrape(page) {
    console.log("Performing full seat scrape with pricing and grouping...");
    const startTime = Date.now();

    try {
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
          const events = [
            new MouseEvent("pointerover", { bubbles: true }),
            new MouseEvent("mouseenter", { bubbles: true }),
            new MouseEvent("mouseover", { bubbles: true }),
            new MouseEvent("pointermove", { bubbles: true }),
            new MouseEvent("mousemove", { bubbles: true }),
          ];

          for (const event of events) {
            element.dispatchEvent(event);
          }

          await sleep(150);
        };

        const getPriceFromTooltip = () => {
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

        const scrapeSeats = async () => {
          try {
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

            const svg = await waitForElement(SELECTORS.SVG_MAP);
            if (!svg) throw new Error("SVG map not found");

            for (let i = 0; i < 5; i++) {
              svg.click();
              await sleep(200);
            }

            await waitForElement(SELECTORS.SEAT, 1000);

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

            const seats = [];
            const availableSeats = Array.from(seatElements).filter((seat) =>
              seat.classList.contains("is-available")
            );

            const BATCH_SIZE = 5;
            for (let i = 0; i < availableSeats.length; i += BATCH_SIZE) {
              const batch = availableSeats.slice(i, i + BATCH_SIZE);

              for (const seat of batch) {
                const sectionElement = seat.closest(SELECTORS.SECTION);
                const rowElement = seat.closest(SELECTORS.ROW);

                document
                  .querySelectorAll('[role="tooltip"]')
                  .forEach((el) => el.remove());

                let price = null;
                let attempts = 0;
                while (!price && attempts < 3) {
                  await simulateHover(seat);
                  price = getPriceFromTooltip();
                  attempts++;
                  if (!price) await sleep(100);
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

              await sleep(100);
            }

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

        return scrapeSeats();
      });

      const scrapeDuration = ((Date.now() - startTime) / 1000).toFixed(2);
      console.log(`Full scrape completed in ${scrapeDuration}s`);
      this.lastFullScrapeTime = Date.now();

      return {
        ...data,
        scrapeDuration,
      };
    } catch (error) {
      console.error("Error during full scrape:", error.message);
      throw error;
    }
  }

  shouldPerformFullScrape(currentSeatCount) {
    if (this.lastKnownSeatCount === null) return true;
    if (currentSeatCount !== this.lastKnownSeatCount) return true;
    if (!this.lastFullScrapeTime) return true;

    const timeSinceLastFullScrape = Date.now() - this.lastFullScrapeTime;
    return timeSinceLastFullScrape >= this.FULL_SCRAPE_INTERVAL;
  }
  async scrape(url, options = {}) {
    if (!this.browser) {
      await this.initBrowser();
    }

    const maxRetries = options.maxRetries || 10;
    let retries = 0;
    const scrapeStart = Date.now();

    while (retries <= maxRetries) {
      let page = null;

      try {
        console.log(
          `\n=== Scrape attempt ${retries + 1} of ${maxRetries + 1} ===`
        );
        console.log(`URL: ${url}`);
        console.log(`Time: ${new Date().toISOString()}`);

        page = await this.context.newPage();

        console.log("Navigating to page...");
        await page.goto(url, {
          waitUntil: "networkidle",
          timeout: 60000,
        });

        // Immediately check for challenge after page load
        console.log("Checking for Ticketmaster challenge...");
        const challengePresent = await page.evaluate(() => {
          return document.body.textContent.includes(
            "Your Browsing Activity Has Been Paused"
          );
        });

        if (challengePresent) {
          console.log("Challenge detected immediately after page load");
          await this.handleTicketmasterChallenge(page);

          // Double-check if challenge is still present after handling
          const stillChallenged = await page.evaluate(() => {
            return document.body.textContent.includes(
              "Your Browsing Activity Has Been Paused"
            );
          });

          if (stillChallenged) {
            throw new Error("Challenge persists after handling attempt");
          }
        }

        // First do a quick check
        const quickCheck = await this.performQuickCheck(page);
        console.log(
          `Quick check results: ${quickCheck.availableSeats} seats available`
        );

        let result;
        if (this.shouldPerformFullScrape(quickCheck.availableSeats)) {
          console.log(
            "Changes detected or full scrape due - performing detailed scrape..."
          );
          result = await this.performFullScrape(page);
          this.lastKnownSeatCount = quickCheck.availableSeats;
        } else {
          console.log("No changes detected - using quick check results");
          result = {
            eventInfo: {
              title: await page.title(),
              dateTime: await page.$eval(
                "#edp-event-header div.sc-1eku3jf-12.BANxv span > span",
                (el) => el.textContent
              ),
            },
            seatingInfo: {
              availableSeats: quickCheck.availableSeats,
              consecutiveGroups: [], // Empty as we're not doing full scrape
              seats: [], // Empty as we're not doing full scrape
            },
            quickCheckOnly: true,
            scrapeDuration: quickCheck.quickCheckDuration,
          };
        }

        const totalDuration = ((Date.now() - scrapeStart) / 1000).toFixed(2);
        console.log(`\nScrape completed successfully in ${totalDuration}s`);
        console.log(`Available seats: ${result.seatingInfo.availableSeats}`);
        console.log(`Full scrape performed: ${!result.quickCheckOnly}`);
        console.log("===============================\n");

        return result;
      } catch (error) {
        retries++;
        console.error(
          `\nError during scrape attempt ${retries}:`,
          error.message
        );

        if (retries <= maxRetries) {
          const delay = Math.pow(2, retries) * 1000 + Math.random() * 1000;
          console.log(`Waiting ${Math.round(delay / 1000)}s before retry...`);
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

  async performQuickCheck(page) {
    console.log("Performing quick availability check...");
    const startTime = Date.now();

    try {
      // Define selectors
      const SELECTORS = {
        SVG_MAP:
          "#map-container > div.zoomer > div.zoomer__controls > button.zoomer__control--zoomin",
        SEATS: 'circle[data-component="svg__seat"]',
        ACCEPT_BUTTON: '[data-bdd="accept-modal-accept-button"]',
      };

      // Handle accept button if present
      try {
        const acceptButton = await page.waitForSelector(
          SELECTORS.ACCEPT_BUTTON,
          { timeout: 2000 }
        );
        if (acceptButton) {
          await acceptButton.click();
          await page.waitForTimeout(200);
        }
      } catch (e) {
        // Accept button not found, continue
      }

      // Wait for zoom button with shorter timeout first
      console.log("Waiting for zoom button...");
      const zoomButton = await page
        .waitForSelector(SELECTORS.SVG_MAP, {
          timeout: 10000,
          state: "visible",
        })
        .catch(() => null);

      if (!zoomButton) {
        console.log("Zoom button not immediately visible, waiting longer...");
        await page.waitForSelector(SELECTORS.SVG_MAP, {
          timeout: 30000,
          state: "visible",
        });
      }

      // Perform zooming with verification
      console.log("Starting zoom process...");
      let zoomAttempts = 0;
      const maxZoomAttempts = 3;

      while (zoomAttempts < maxZoomAttempts) {
        try {
          // Click zoom button multiple times with verification
          for (let i = 0; i <3; i++) {
            await page.click(SELECTORS.SVG_MAP);
            await page.waitForTimeout(200);
          }
          break; // Break if zooming successful
        } catch (error) {
          zoomAttempts++;
          console.log(`Zoom attempt ${zoomAttempts} failed, retrying...`);
          await page.waitForTimeout(1000);

          if (zoomAttempts === maxZoomAttempts) {
            console.error("Failed to zoom after multiple attempts");
            throw new Error("Zoom operation failed");
          }
        }
      }

      // Wait for seats to be visible after zooming
      console.log("Waiting for seats to be visible after zoom...");
      await page.waitForSelector(SELECTORS.SEATS, {
        timeout: 60000,
        state: "visible",
      });

      // Get seat count
      const seatCount = await this.getAvailableSeatCount(page);

      const checkDuration = ((Date.now() - startTime) / 1000).toFixed(2);
      console.log(
        `Quick check completed in ${checkDuration}s. Available seats: ${seatCount}`
      );

      return {
        availableSeats: seatCount,
        quickCheckDuration: checkDuration,
      };
    } catch (error) {
      console.error("Error during quick check:", error.message);
      throw error;
    }
  }

  async close() {
    await this.context?.close().catch(() => {});
    await this.browser?.close().catch(() => {});
  }
}

export default Scraper;