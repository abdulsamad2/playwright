import { createHash } from "crypto";

export class BrowserFingerprint {
  static platforms = [
    // Desktop platforms
    { name: "Windows", version: "10", arch: "x64", type: "desktop" },
    { name: "Windows", version: "11", arch: "x64", type: "desktop" },
    { name: "Ubuntu", version: "22.04", arch: "x64", type: "desktop" },
    { name: "Macintosh", version: "13.4", arch: "Apple M1", type: "desktop" },
    { name: "Macintosh", version: "12.6", arch: "Intel", type: "desktop" },
    // Mobile platforms
    { name: "iPhone", version: "17.2", arch: "arm64", type: "mobile" },
    { name: "iPhone", version: "16.5", arch: "arm64", type: "mobile" },
    { name: "Android", version: "14.0", arch: "arm64-v8a", type: "mobile" },
    { name: "Android", version: "13.0", arch: "arm64-v8a", type: "mobile" },
    { name: "iPad", version: "17.2", arch: "arm64", type: "tablet" },
  ];

  static browsers = [
    // Desktop browsers
    { name: "Chrome", version: "120.0.0.0", mobile: false },
    { name: "Firefox", version: "121.0", mobile: false },
    { name: "Safari", version: "17.0", mobile: false },
    { name: "Edge", version: "119.0.0.0", mobile: false },
    // Mobile browsers
    { name: "Chrome", version: "120.0.0.0", mobile: true },
    { name: "Safari", version: "17.0", mobile: true },
    { name: "Samsung Internet", version: "23.0.0.47", mobile: true },
  ];

  static languages = [
    "en-US",
    "en-GB",
    "en-CA",
    "fr-FR",
    "es-ES",
    "de-DE",
    "it-IT",
    "pt-BR",
    "ja-JP",
    "ko-KR",
  ];

  static screens = [
    // Desktop screens
    { width: 1920, height: 1080, type: "desktop" },
    { width: 2560, height: 1440, type: "desktop" },
    { width: 3840, height: 2160, type: "desktop" }, // 4K
    // Mobile screens
    { width: 390, height: 844, type: "mobile" }, // iPhone 14
    { width: 412, height: 915, type: "mobile" }, // Pixel 7
    { width: 360, height: 800, type: "mobile" }, // Galaxy S21
    { width: 820, height: 1180, type: "tablet" }, // iPad Air
    { width: 1024, height: 1366, type: "tablet" }, // iPad Pro
  ];

  static timezones = [
    "America/New_York",
    "America/Los_Angeles",
    "America/Chicago",
    "Europe/London",
    "Europe/Paris",
    "Europe/Berlin",
    "Asia/Tokyo",
    "Asia/Singapore",
    "Asia/Dubai",
    "Australia/Sydney",
    "Pacific/Auckland",
  ];

  static mobileModels = {
    iPhone: [
      "iPhone 15,3", // iPhone 15 Pro Max
      "iPhone 14,3", // iPhone 14 Pro Max
      "iPhone 14,7", // iPhone 14
      "iPhone 13,4", // iPhone 12 Pro Max
    ],
    Android: [
      "SM-S918B", // Samsung Galaxy S23 Ultra
      "SM-A536B", // Samsung Galaxy A53 5G
      "Pixel 7 Pro", // Google Pixel 7 Pro
      "OnePlus 9 Pro", // OnePlus 9 Pro
    ],
    iPad: [
      "iPad14,6", // iPad Pro 12.9-inch (6th generation)
      "iPad13,8", // iPad Pro 12.9-inch (5th generation)
      "iPad13,1", // iPad Air (4th generation)
    ],
  };

  static generate(deviceType = null) {
    // Filter platforms and browsers based on device type if specified
    const validPlatforms = deviceType
      ? this.platforms.filter((p) => p.type === deviceType)
      : this.platforms;

    const platform =
      validPlatforms[Math.floor(Math.random() * validPlatforms.length)];
    const isMobile = platform.type === "mobile" || platform.type === "tablet";

    // Filter browsers based on platform type
    const validBrowsers = this.browsers.filter((b) => b.mobile === isMobile);
    const browser =
      validBrowsers[Math.floor(Math.random() * validBrowsers.length)];

    // Filter screens based on platform type
    const validScreens = this.screens.filter((s) => s.type === platform.type);
    const screen =
      validScreens[Math.floor(Math.random() * validScreens.length)];

    const fingerprint = {
      platform,
      browser,
      language:
        this.languages[Math.floor(Math.random() * this.languages.length)],
      screen,
      colorDepth: isMobile ? 32 : 24,
      deviceMemory: isMobile
        ? Math.floor(Math.random() * 4) + 2 // 2GB to 6GB for mobile
        : Math.floor(Math.random() * 24) + 8, // 8GB to 32GB for desktop
      hardwareConcurrency: isMobile
        ? Math.floor(Math.random() * 4) + 2 // 2 to 6 cores for mobile
        : Math.floor(Math.random() * 12) + 4, // 4 to 16 cores for desktop
      timezone:
        this.timezones[Math.floor(Math.random() * this.timezones.length)],
      touchPoints: isMobile ? Math.floor(Math.random() * 5) + 1 : 0,
      devicePixelRatio: isMobile
        ? [2, 2.5, 3, 3.5][Math.floor(Math.random() * 4)]
        : [1, 1.5, 2][Math.floor(Math.random() * 3)],
      sessionId: createHash("sha256")
        .update(Math.random().toString())
        .digest("hex"),
    };

    // Add mobile-specific properties
    if (isMobile) {
      fingerprint.mobileModel =
        this.mobileModels[platform.name]?.[
          Math.floor(Math.random() * this.mobileModels[platform.name].length)
        ];
    }

    return fingerprint;
  }

  static generateUserAgent(fingerprint) {
    const { platform, browser, mobileModel } = fingerprint;
    const isMobile = platform.type === "mobile" || platform.type === "tablet";

    // Mobile User Agents
    if (isMobile) {
      if (platform.name === "iPhone" || platform.name === "iPad") {
        return `Mozilla/5.0 (${platform.name}; CPU ${
          platform.name
        } OS ${platform.version.replace(
          /\./g,
          "_"
        )} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/${
          browser.version
        } Mobile/${mobileModel} Safari/604.1`;
      }

      if (platform.name === "Android") {
        if (browser.name === "Samsung Internet") {
          return `Mozilla/5.0 (Linux; Android ${platform.version}; ${mobileModel}) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/${browser.version} Chrome/120.0.0.0 Mobile Safari/537.36`;
        }
        return `Mozilla/5.0 (Linux; Android ${platform.version}; ${mobileModel}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${browser.version} Mobile Safari/537.36`;
      }
    }

    // Desktop User Agents
    if (browser.name === "Chrome") {
      return `Mozilla/5.0 (${
        platform.name === "Windows"
          ? `Windows NT ${
              platform.version === "11" ? "11.0" : "10.0"
            }; Win64; x64`
          : platform.name === "Macintosh"
          ? `Macintosh; ${platform.arch} Mac OS X ${platform.version.replace(
              /\./g,
              "_"
            )}`
          : "X11; Linux x86_64"
      }) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${
        browser.version
      } Safari/537.36`;
    }

    if (browser.name === "Firefox") {
      return `Mozilla/5.0 (${
        platform.name === "Windows"
          ? "Windows NT 10.0; Win64; x64"
          : platform.name === "Macintosh"
          ? `Macintosh; ${platform.arch} Mac OS X ${platform.version.replace(
              /\./g,
              "_"
            )}`
          : "X11; Linux x86_64"
      }; rv:109.0) Gecko/20100101 Firefox/${browser.version}`;
    }

    if (browser.name === "Safari") {
      return `Mozilla/5.0 (Macintosh; ${
        platform.arch
      } Mac OS X ${platform.version.replace(
        /\./g,
        "_"
      )}) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/${
        browser.version
      } Safari/605.1.15`;
    }

    if (browser.name === "Edge") {
      return `Mozilla/5.0 (${
        platform.name === "Windows"
          ? "Windows NT 10.0; Win64; x64"
          : "X11; Linux x86_64"
      }) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${
        browser.version
      } Safari/537.36 Edg/${browser.version}`;
    }

    return "Unknown User Agent";
  }
}
