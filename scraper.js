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
  headers: null,
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
      headers: null,
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

    // Split proxy into host and port
    const [host, port] = proxy.proxy.split(":");

    browser = await firefox.launch({
      headless: true,
      proxy: {
        server: `http://${host}:${port}`,
        username: proxy.username,
        password: proxy.password,
        bypass: "", // Empty bypass list
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

async function captureApiHeaders(page, eventId, fingerprint) {
  return new Promise((resolve, reject) => {
    const apiPattern = new RegExp(`/api/ismds/event/${eventId}/facets.*`);
    let captured = false;
    let timeoutId;

    const cleanup = () => {
      if (timeoutId) clearTimeout(timeoutId);
      page.removeListener("request", requestHandler);
    };

    const requestHandler = async (request) => {
      const url = request.url();
      if (apiPattern.test(url) && !captured) {
        captured = true;
        cleanup();
        try {
          const headers = request.headers();
          let cookies = await context.cookies();

          // Extend the expiry time of each cookie to 1 hour from now
          const oneHourFromNow = Date.now() + 60 * 60 * 1000; // 1 hour in milliseconds
          cookies = cookies.map((cookie) => ({
            ...cookie,
            expires: oneHourFromNow / 1000, // Convert to seconds for cookie expiry
            expiry: oneHourFromNow / 1000, // Some implementations use 'expiry' instead of 'expires'
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

          resolve({ headers, cookies, fingerprint });
        } catch (error) {
          reject(error);
        }
      }
    };

    page.on("request", requestHandler);

    timeoutId = setTimeout(() => {
      cleanup();
      reject(new Error("Timeout waiting for API request"));
    }, 30000);
  });
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

    await handleTicketmasterChallenge(page);

    const data = await captureApiHeaders(page, eventId, fingerprint);
    await page.close().catch(() => {});

    capturedState = {
      headers: data.headers,
      cookies: data.cookies,
      fingerprint: fingerprint,
      lastRefresh: Date.now(),
      proxy: proxy,
    };

    return capturedState;
  } catch (error) {
    capturedState = {
      headers: null,
      cookies: null,
      fingerprint: null,
      lastRefresh: null,
      proxy: null,
    };
    console.error("Error in refreshHeaders:", error);
    throw error;
  }
}

async function getCapturedData(eventId, proxy, forceRefresh = false) {
  const currentTime = Date.now();
  const refreshInterval = 15 * 60 * 1000; // 15 minutes

  if (
    !capturedState.headers ||
    !capturedState.cookies ||
    !capturedState.fingerprint ||
    !capturedState.lastRefresh ||
    !capturedState.proxy ||
    currentTime - capturedState.lastRefresh > refreshInterval ||
    forceRefresh
  ) {
    console.log("Getting fresh headers and cookies...");
    try {
      return await refreshHeaders(eventId, proxy);
    } catch (error) {
      console.error("Error getting captured data:", error);
      throw error;
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

    console.log("Starting event scraping with captured headers...");

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
