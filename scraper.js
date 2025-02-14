import { firefox } from "playwright";
import axios from "axios";
import { HttpsProxyAgent } from "https-proxy-agent";
import { proxyArray } from "./proxyList.js";
import crypto from "crypto";

class SeatsAPIScraper {
  constructor() {
    this.proxyArray = proxyArray;
    this.currentProxyIndex = 0;
    this.browser = null;
    this.context = null;
    this.cachedHeaders = null;
    this.cachedCookies = null;
  }

  generateCorrelationId() {
    return crypto.randomUUID();
  }

  getNextProxy() {
    const proxy = this.proxyArray[this.currentProxyIndex];
    this.currentProxyIndex =
      (this.currentProxyIndex + 1) % this.proxyArray.length;
    return proxy;
  }

  formatProxyUrl(proxy) {
    return `http://${proxy.username}:${proxy.password}@${proxy.host}:${proxy.port}`;
  }

  async initBrowser() {
    const proxy = this.getNextProxy();

    this.browser = await firefox.launch({
      headless: true,
      proxy: {
        server: `${proxy.host}:${proxy.port}`,
        username: proxy.username,
        password: proxy.password,
      },
    });

    this.context = await this.browser.newContext({
      viewport: { width: 1920, height: 1080 },
      userAgent:
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
      locale: "en-US",
    });

    return proxy;
  }

  async captureApiHeaders(page, eventId) {
    return new Promise((resolve, reject) => {
      const apiPattern = new RegExp(`/api/ismds/event/${eventId}/facets.*`);
      let captured = false;

      page.on("request", async (request) => {
        const url = request.url();
        if (apiPattern.test(url) && !captured) {
          captured = true;
          const headers = request.headers();
          const cookies = await this.context.cookies();
          resolve({ headers, cookies });
        }
      });

      setTimeout(() => {
        if (!captured) {
          reject(new Error("Timeout waiting for API request"));
        }
      }, 30000);
    });
  }

  async refreshHeaders(eventId) {
    console.log("Refreshing API headers...");
    if (!this.browser) {
      await this.initBrowser();
    }

    const page = await this.context.newPage();
    const url = `https://www.ticketmaster.com/event/${eventId}`;

    await page.goto(url, { waitUntil: "networkidle" });
    const capturedData = await this.captureApiHeaders(page, eventId);
    await page.close();

    this.cachedHeaders = capturedData.headers;
    this.cachedCookies = capturedData.cookies;

    return capturedData;
  }

  async fetchSeatsData(eventId, useCache = true) {
    const apiUrl = `https://services.ticketmaster.com/api/ismds/event/${eventId}/facets`;

    const params = {
      apikey: "b462oi7fic6pehcdkzony5bxhe",
      apisecret: "pquzpfrfz7zd2ylvtz3w5dtyse",
      by: "inventorytypes offer",
      q: "available",
      show: "listpricerange",
      embed: "offer",
      resaleChannelId:
        "internal.ecommerce.consumer.desktop.web.browser.ticketmaster.us",
    };

    let capturedData;
    if (!useCache || !this.cachedHeaders || !this.cachedCookies) {
      capturedData = await this.refreshHeaders(eventId);
    } else {
      capturedData = {
        headers: this.cachedHeaders,
        cookies: this.cachedCookies,
      };
    }

    const cookieString = capturedData.cookies
      .map((cookie) => `${cookie.name}=${cookie.value}`)
      .join("; ");

    const correlationId = this.generateCorrelationId();

    const headers = {
      Accept: "*/*",
      "Accept-Language": "en-US",
      "Accept-Encoding": "gzip, deflate, br, zstd",
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
      Referer: "https://www.ticketmaster.com/",
      Origin: "https://www.ticketmaster.com",
      Host: "services.ticketmaster.com",
      Connection: "keep-alive",
      Cookie: cookieString,
      "tmps-correlation-id": correlationId,
      "sec-fetch-dest": "empty",
      "sec-fetch-mode": "cors",
      "sec-fetch-site": "same-site",
      "X-Api-Key": "b462oi7fic6pehcdkzony5bxhe",
    };

    try {
      const proxy = this.getNextProxy();
      const proxyUrl = this.formatProxyUrl(proxy);

      console.log("Making API request with headers...");
      const response = await axios.get(apiUrl, {
        params,
        headers,
        httpsAgent: new HttpsProxyAgent(proxyUrl),
        timeout: 600000,
      });

      return response.data;
    } catch (error) {
      console.error("API request failed:", error.message);
      if (error.response) {
        console.error("Response status:", error.response.status);
        console.error("Response data:", error.response.data);
      }

      // If we get an error and we were using cached headers, try refreshing them
      if (useCache) {
        console.log("Retrying with fresh headers...");
        return this.fetchSeatsData(eventId, false);
      }

      throw error;
    }
  }

  async scrape(eventId) {
    try {
      console.log("Making seats API request...");
      const seatsData = await this.fetchSeatsData(eventId);
      console.log("Seats data:", seatsData);
      return seatsData;
    } catch (error) {
      console.error("Scraping failed:", error);
      throw error;
    }
  }

  async close() {
    await this.context?.close().catch(() => {});
    await this.browser?.close().catch(() => {});
  }
}

export default SeatsAPIScraper;


