import { createRequire } from "module";
import { createHash } from "crypto";
const require = createRequire(import.meta.url);
const axios = require("axios");
const { HttpsProxyAgent } = require("https-proxy-agent");
const fs = require("fs");
import { firefox } from "playwright";
import proxyArray from "../settings/proxy.js";
import { AttachRowSection } from "./seatBatch.js";
import GenerateNanoPlaces from "./seats.js";
import { postEventLines } from "../API.js";
import { SendMail } from "../helpers/mailer.js";
import failedProxies from "../settings/failedProxy.js";
import crypto from "crypto";

// Browser Fingerprint Class
export class BrowserFingerprint {
  static platforms = [
    { name: "Windows", version: "10", arch: "x64" },
    { name: "Macintosh", version: "10_15_7", arch: "Intel" },
  ];

  static browsers = [
    { name: "Chrome", version: "120.0.0.0" },
    { name: "Firefox", version: "121.0" },
    { name: "Safari", version: "17.0" },
  ];

  static languages = ["en-US", "en-GB", "en-CA"];

  static screens = [
    { width: 1920, height: 1080 },
    { width: 2560, height: 1440 },
    { width: 1440, height: 900 },
  ];

  static generate() {
    const platform =
      this.platforms[Math.floor(Math.random() * this.platforms.length)];
    const browser =
      this.browsers[Math.floor(Math.random() * this.browsers.length)];
    const language =
      this.languages[Math.floor(Math.random() * this.languages.length)];
    const screen =
      this.screens[Math.floor(Math.random() * this.screens.length)];

    return {
      platform,
      browser,
      language,
      screen,
      colorDepth: 24,
      deviceMemory: 8,
      hardwareConcurrency: 8,
      timezone: "America/New_York",
      touchPoints: 0,
      devicePixelRatio: 1,
      sessionId: createHash("sha256")
        .update(Math.random().toString())
        .digest("hex"),
    };
  }

  static generateUserAgent(fingerprint) {
    const { platform, browser } = fingerprint;
    if (browser.name === "Chrome") {
      return `Mozilla/5.0 (${
        platform.name === "Windows"
          ? "Windows NT 10.0; Win64; x64"
          : "Macintosh; Intel Mac OS X " + platform.version
      }) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${
        browser.version
      } Safari/537.36`;
    }
    return "";
  }
}

let fileCounter = 0;

// Browser and header state
let browser = null;
let context = null;
let capturedHeaders = null;
let capturedCookies = null;

function generateCorrelationId() {
  return crypto.randomUUID();
}

async function initBrowser(proxy) {
  // Generate fingerprint
  const fingerprint = BrowserFingerprint.generate();
  const userAgent = BrowserFingerprint.generateUserAgent(fingerprint);

  try {
    // Format proxy URL correctly
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
      viewport: {
        width: fingerprint.screen.width,
        height: fingerprint.screen.height,
      },
      userAgent: userAgent,
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

    page.on("request", async (request) => {
      const url = request.url();
      if (apiPattern.test(url) && !captured) {
        captured = true;
        const headers = request.headers();
        const cookies = await context.cookies();
        resolve({ headers, cookies, fingerprint });
      }
    });

    setTimeout(() => {
      if (!captured) {
        reject(new Error("Timeout waiting for API request"));
      }
    }, 600000);
  });
}

async function getCapturedData(eventId, proxy) {
  console.log("Getting fresh headers and cookies...");
  try {
    // Always create a new browser instance
    if (browser) {
      await browser.close().catch(() => {});
      browser = null;
      context = null;
    }

    const { context: newContext, fingerprint } = await initBrowser(proxy);
    context = newContext;

    // Verify browser is properly initialized
    if (!context) {
      throw new Error("Failed to initialize browser context");
    }

    const page = await context.newPage();
    const url = `https://www.ticketmaster.com/event/${eventId}`;

    await page.goto(url, { waitUntil: "networkidle" });
    const data = await captureApiHeaders(page, eventId, fingerprint);
    await page.close();

    capturedHeaders = data.headers;
    capturedCookies = data.cookies;

    return data;
  } catch (error) {
    console.error("Error capturing headers:", error);
    throw error;
  }
}

const GetData = async (headers, proxyAgent, url, eventId) => {
  return new Promise(async (resolve, reject) => {
    try {
      let abortController = new AbortController();
      const timeout = setTimeout(() => {
        abortController.abort();
        failedProxies.failedProxies.push(proxyAgent?.proxy);
        if (failedProxies.failedProxies.length >= 50) {
          failedProxies.failedProxies = [];
          SendMail(eventId, failedProxies.failedProxies);
        }
        console.log("Aborted");
        console.log(eventId, "eventId");
        return resolve(false);
      }, 5000);

      const { data, status } = await axios
        .get(
          url,
          {
            method: "GET",
            httpsAgent: proxyAgent,
            headers: headers,
            timeout: 5000,
          },
          { signal: abortController.signal }
        )
        .then((x) => {
          clearTimeout(timeout);
          return x;
        })
        .catch((e) => {
          return e;
        });

      if (status) {
        if (status == 200) {
          return resolve(data);
        } else {
          return resolve(false);
        }
      } else {
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

  // Validate proxy format
  if (!_proxy || !_proxy.proxy || !_proxy.username || !_proxy.password) {
    throw new Error("Invalid proxy configuration");
  }

  try {
    // Validate proxy URL format
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
    // First get the headers and cookies with fingerprint
    const capturedData = await getCapturedData(event?.eventId, proxy);
    const cookieString = capturedData.cookies
      .map((cookie) => `${cookie.name}=${cookie.value}`)
      .join("; ");

    const userAgent = BrowserFingerprint.generateUserAgent(
      capturedData.fingerprint
    );

    // Construct headers using captured data and fingerprint
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
        fs.writeFile(
          fileCounter + "dataGet.json",
          JSON.stringify(dataGet),
          "utf8",
          async function (err) {
            if (err) {
              console.log(
                "An error occurred while writing JSON Object to File."
              );
            }
            console.log(
              fileCounter + "output.json",
              "JSON file has been saved."
            );
            fileCounter += 1;
          }
        );
     // Apply the selection filter
    // dataGet = dataGet.filter((x) => x?.selection && x?.selection != "resale");
    //  console.log("dataGet after accessibility filter:", dataGet);
      // dataGet = dataGet.filter(
      //   (x) =>
      //     !(
      //       x.accessibility.includes("companion") ||
      //       x.accessibility.includes("sight") ||
      //       x.accessibility.includes("hearing") ||
      //       x.accessibility.includes("wheelchair") ||
      //       x.accessibility.includes("mobility")
      //     )
      // );
console.log("DataGet structure:", {
  length: dataGet?.length,
  firstItem: dataGet?.[0],
  samplePlaces: dataGet?.[0]?.places,
});

console.log("DataMap structure:", {
  type: typeof DataMap,
  pages: DataMap?.pages,
  segments: DataMap?.pages?.[0]?.segments,
});

console.log("Offers:", {
  exists: !!DataFacets?._embedded?.offer,
  sample: DataFacets?._embedded?.offer?.[0],
});

console.log("Event:", event);
console.log("Description:", DataFacets?._embedded?.description);


  fs.writeFile(
    fileCounter + "DataMap.json",
    JSON.stringify(DataMap),
    "utf8",
    async function (err) {
      if (err) {
        console.log("An error occurred while writing JSON Object to File.");
      }
      console.log(fileCounter + "output.json", "JSON file has been saved.");
      fileCounter += 1;
    }
  );
      let finalData = AttachRowSection(
        dataGet,
        DataMap,
        DataFacets?._embedded?.offer,
        event,
        DataFacets?._embedded?.description
      );
console.log("finalData:", finalData);



      // Save the data
      fs.writeFile(
        fileCounter + "output.json",
        JSON.stringify(finalData),
        "utf8",
        async function (err) {
          if (err) {
            console.log("An error occurred while writing JSON Object to File.");
          }
          console.log(fileCounter + "output.json", "JSON file has been saved.");
          fileCounter += 1;
        }
      );

      try {
        console.log("Sending data to backend");
        const { data, status } = await postEventLines(finalData);
        console.log(data,status)
        // if (status != 200) {
        //   SendMail(eventId, "Failed to send data to backend");
        //   console.log(status, "failed");
        // }
        console.log("Cycle completed");

        // Cleanup
        return true;
      } catch (e) {
        console.error("Error posting event lines:", e);
        return false;
      }
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

// Main execution
async function main() {
  try {
    const result = await ScrapeEvent({
      eventId: "0A00614FC8B22987",
    });
    console.log("Scraping result:", result);
  } catch (error) {
    console.error("Scraping error:", error);
  }
}

main();
