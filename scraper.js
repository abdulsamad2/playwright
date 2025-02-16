import { createRequire } from "module";
const require = createRequire(import.meta.url);
const axios = require("axios");
const { HttpsProxyAgent } = require("https-proxy-agent");
const fs = require("fs");
import { firefox } from "playwright";
import proxyArray from "./helpers/proxy.js";
import { AttachRowSection } from "./helpers/seatBatch.js";
import GenerateNanoPlaces from "./helpers/seats.js";
import crypto from "crypto";
import { BrowserFingerprint } from "./browserFingerprint.js";

let browser = null;
let context = null;
let capturedState = {
  cookies: null,
  fingerprint: null,
  lastRefresh: null,
  proxy: null,
};

function generateCorrelationId() {
  return crypto.randomUUID();
}

async function handleTicketmasterChallenge(page) {
  const CHALLENGE_TIMEOUT = 10000;
  const startTime = Date.now();

  try {
    const challengePresent = await page.evaluate(() => {
      return document.body.textContent.includes(
        "Your Browsing Activity Has Been Paused"
      );
    });

    if (challengePresent) {
      console.log("Detected Ticketmaster challenge, attempting resolution...");
      await page.waitForTimeout(1000 + Math.random() * 1000);

      const viewportSize = page.viewportSize();
      if (viewportSize) {
        await page.mouse.move(
          Math.floor(Math.random() * viewportSize.width),
          Math.floor(Math.random() * viewportSize.height),
          { steps: 5 }
        );
      }

      const buttons = await page.$$("button");
      let buttonClicked = false;

      for (const button of buttons) {
        if (Date.now() - startTime > CHALLENGE_TIMEOUT) {
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

      await page.waitForTimeout(2000);
      const stillChallenged = await page.evaluate(() => {
        return document.body.textContent.includes(
          "Your Browsing Activity Has Been Paused"
        );
      });

      if (stillChallenged) {
        throw new Error("Challenge not resolved");
      }
    }
    return true;
  } catch (error) {
    console.error("Challenge handling failed:", error.message);
    capturedState = {
      cookies: null,
      fingerprint: null,
      lastRefresh: null,
      proxy: null,
    };
    throw error;
  }
}

async function initBrowser(proxy) {
  if (!proxy || !proxy.proxy) {
    const { proxy: newProxy } = GetProxy();
    proxy = newProxy;
  }

  const fingerprint = BrowserFingerprint.generate();
  const userAgent = BrowserFingerprint.generateUserAgent(fingerprint);

  try {
    if (browser) {
      await browser.close().catch(() => {});
    }

    const proxyUrl = new URL(`http://${proxy.proxy}`);

    browser = await firefox.launch({
      headless: true,
      proxy: {
        server: `http://${proxyUrl.hostname}:${proxyUrl.port || 80}`,
        username: proxy.username,
        password: proxy.password,
      },
    });

    context = await browser.newContext({
      viewport: fingerprint.screen,
      userAgent,
      locale: fingerprint.language,
      deviceScaleFactor: fingerprint.devicePixelRatio,
      colorScheme: "light",
      timezoneId: fingerprint.timezone,
    });

    return { context, fingerprint };
  } catch (error) {
    console.error("Error initializing browser:", error);
    throw error;
  }
}

async function captureCookies(page, fingerprint) {
  const MAX_RETRIES = 3;
  const RETRY_DELAY = 2000;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      // Check for challenge before attempting to capture cookies
      const challengePresent = await page.evaluate(() => {
        return document.body.textContent.includes(
          "Your Browsing Activity Has Been Paused"
        );
      });

      if (challengePresent) {
        console.log(
          `Attempt ${attempt}: Challenge detected during cookie capture`
        );

        // Try to handle the challenge
        const challengeResolved = await handleTicketmasterChallenge(page);
        if (!challengeResolved) {
          if (attempt === MAX_RETRIES) {
            console.log("Max retries reached during challenge resolution");
            return { cookies: null, fingerprint };
          }
          await page.waitForTimeout(RETRY_DELAY);
          continue;
        }
      }

      let cookies = await context.cookies();

      // Verify cookies are valid
      if (!cookies || cookies.length === 0) {
        console.log(`Attempt ${attempt}: No cookies captured`);
        if (attempt === MAX_RETRIES) {
          return { cookies: null, fingerprint };
        }
        await page.waitForTimeout(RETRY_DELAY);
        continue;
      }

      // Extend the expiry time of each cookie to 1 hour from now
      const oneHourFromNow = Date.now() + 60 * 60 * 1000;
      cookies = cookies.map((cookie) => ({
        ...cookie,
        expires: oneHourFromNow / 1000,
        expiry: oneHourFromNow / 1000,
      }));

      // Update the cookies in the context with new expiry times
      await Promise.all(
        cookies.map((cookie) =>
          context.addCookies([
            {
              ...cookie,
              expires: oneHourFromNow / 1000,
              expiry: oneHourFromNow / 1000,
            },
          ])
        )
      );

      console.log(`Successfully captured cookies on attempt ${attempt}`);
      return { cookies, fingerprint };
    } catch (error) {
      console.error(`Error capturing cookies (attempt ${attempt}):`, error);

      if (attempt === MAX_RETRIES) {
        console.log("Max retries reached during cookie capture");
        return { cookies: null, fingerprint };
      }

      await page.waitForTimeout(RETRY_DELAY);
    }
  }

  return { cookies: null, fingerprint };
}

async function refreshHeaders(eventId, proxy) {
  if (!proxy || !proxy.proxy) {
    const { proxy: newProxy } = GetProxy();
    proxy = newProxy;
  }

  try {
    const { context: newContext, fingerprint } = await initBrowser(proxy);
    context = newContext;

    const page = await context.newPage();
    const url = `https://www.ticketmaster.com/event/${eventId}`;

    await page.goto(url, {
      waitUntil: "networkidle",
      timeout: 30000,
    });

    const data = await captureCookies(page, fingerprint);
    await page.close().catch(() => {});

    // Only update capturedState if we successfully got cookies
    if (data.cookies) {
      capturedState = {
        cookies: data.cookies,
        fingerprint: fingerprint,
        lastRefresh: Date.now(),
        proxy: proxy,
      };
    } else {
      capturedState = {
        cookies: null,
        fingerprint: null,
        lastRefresh: null,
        proxy: null,
      };
    }

    return capturedState;
  } catch (error) {
    capturedState = {
      cookies: null,
      fingerprint: null,
      lastRefresh: null,
      proxy: null,
    };
    console.error("Error in refreshHeaders:", error);
    throw error;
  } finally {
    if (browser) {
      await browser.close().catch(() => {});
      browser = null;
      context = null;
    }
  }
}

async function getCapturedData(eventId, proxy, forceRefresh = false) {
  const currentTime = Date.now();
  const refreshInterval = 15 * 60 * 1000; // 15 minutes
  const MAX_REFRESH_ATTEMPTS = 3;

  if (
    !capturedState.cookies ||
    !capturedState.fingerprint ||
    !capturedState.lastRefresh ||
    !capturedState.proxy ||
    currentTime - capturedState.lastRefresh > refreshInterval ||
    forceRefresh
  ) {
    console.log("Getting fresh cookies...");

    for (let attempt = 1; attempt <= MAX_REFRESH_ATTEMPTS; attempt++) {
      try {
        const result = await refreshHeaders(eventId, proxy);
        if (result.cookies) {
          return result;
        }

        console.log(`Retry ${attempt}: Failed to get valid cookies`);
        if (attempt === MAX_REFRESH_ATTEMPTS) {
          throw new Error("Failed to capture valid cookies after max attempts");
        }

        await new Promise((resolve) => setTimeout(resolve, 2000));
      } catch (error) {
        console.error(
          `Error getting captured data (attempt ${attempt}):`,
          error
        );
        if (attempt === MAX_REFRESH_ATTEMPTS) {
          throw error;
        }
      }
    }
  }

  return capturedState;
}
const GetData = async (headers, proxyAgent, url, eventId) => {
  return new Promise(async (resolve, reject) => {
    try {
      let abortController = new AbortController();
      const timeout = setTimeout(() => {
        abortController.abort();
        console.log("Request aborted due to timeout");
        console.log(eventId, "eventId");
        return resolve(false);
      }, 10000);

      try {
        const response = await axios.get(url, {
          httpsAgent: proxyAgent,
          headers: {
            ...headers,
            "Accept-Encoding": "gzip, deflate, br",
            Connection: "keep-alive",
          },
          timeout: 10000,
          signal: abortController.signal,
          validateStatus: (status) => status === 200,
        });

        clearTimeout(timeout);
        return resolve(response.data);
      } catch (error) {
        clearTimeout(timeout);
        console.log(`Request failed: ${error.message}`);
        return resolve(false);
      }
    } catch (e) {
      console.log(e, "error");
      return resolve(false);
    }
  });
};

const GetProxy = () => {
  let _proxy = [...proxyArray?.proxies];
  const randomProxy = Math.floor(Math.random() * _proxy.length);
  _proxy = _proxy[randomProxy];

  if (!_proxy || !_proxy.proxy || !_proxy.username || !_proxy.password) {
    throw new Error("Invalid proxy configuration");
  }

  try {
    const proxyUrl = new URL(`http://${_proxy.proxy}`);
    let proxyURl = `http://${_proxy.username}:${_proxy.password}@${
      proxyUrl.hostname
    }:${proxyUrl.port || 80}`;
    const proxyAgent = new HttpsProxyAgent(proxyURl);
    return { proxyAgent, proxy: _proxy };
  } catch (error) {
    console.error("Invalid proxy URL format:", error);
    throw new Error("Invalid proxy URL format");
  }
};

const ScrapeEvent = async (event) => {
  const { proxyAgent, proxy } = GetProxy();
  const correlationId = generateCorrelationId();

  try {
    const capturedData = await getCapturedData(event?.eventId, proxy);
    const cookieString = capturedData.cookies
      .map((cookie) => `${cookie.name}=${cookie.value}`)
      .join("; ");

    const userAgent = BrowserFingerprint.generateUserAgent(
      capturedData.fingerprint
    );

    const MapHeader = {
      "User-Agent": userAgent,
      Accept: `*/*`,
      Origin: `https://www.ticketmaster.com`,
      Referer: `https://www.ticketmaster.com/`,
      "Content-Encoding": "gzip",
      Cookie: cookieString,
    };

    const FacetHeader = {
      Accept: "*/*",
      "Accept-Language": capturedData.fingerprint.language,
      "Accept-Encoding": "gzip, deflate, br, zstd",
      "User-Agent": userAgent,
      Referer: "https://www.ticketmaster.com/",
      Origin: "https://www.ticketmaster.com",
      Cookie: cookieString,
      "tmps-correlation-id": correlationId,
      "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe",
    };

    console.log("Starting event scraping with captured cookies...");

    const mapUrl = `https://mapsapi.tmol.io/maps/geometry/3/event/${event?.eventId}/placeDetailNoKeys?useHostGrids=true&app=CCP&sectionLevel=true&systemId=HOST`;
    const facetUrl = `https://services.ticketmaster.com/api/ismds/event/${event?.eventId}/facets?by=section+shape+attributes+available+accessibility+offer+inventoryTypes+offerTypes+description&show=places+inventoryTypes+offerTypes&embed=offer&embed=description&q=available&compress=places&resaleChannelId=internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us&apikey=b462oi7fic6pehcdkzony5bxhe&apisecret=pquzpfrfz7zd2ylvtz3w5dtyse`;

    const DataMap = await GetData(
      MapHeader,
      proxyAgent,
      mapUrl,
      event?.eventId
    );
    const DataFacets = await GetData(
      FacetHeader,
      proxyAgent,
      facetUrl,
      event?.eventId
    );

    if (DataFacets && DataMap) {
      console.log("API requests completed successfully");
      let dataGet = GenerateNanoPlaces(DataFacets?.facets);
      let finalData = AttachRowSection(
        dataGet,
        DataMap,
        DataFacets?._embedded?.offer,
        event,
        DataFacets?._embedded?.description
      );
      return finalData;
    } else {
      console.log("Failed to get data from APIs");
      return false;
    }
  } catch (e) {
    console.error("Scraping error:", e);
    return false;
  } finally {
    if (browser) {
      await browser.close().catch(() => {});
      browser = null;
      context = null;
    }
  }
};

export { ScrapeEvent, refreshHeaders };
