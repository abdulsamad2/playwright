import { createHash } from "crypto";

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
