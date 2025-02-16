/**
 * Configuration object for human behavior simulation
 * @typedef {Object} SimulationConfig
 * @property {number} [initialDelay=2000] - Base delay before starting simulation (ms)
 * @property {number} [initialDelayVariance=3000] - Random variance added to initial delay (ms)
 * @property {number} [minMouseMoves=3] - Minimum number of mouse movements
 * @property {number} [maxMouseMoves=8] - Maximum number of mouse movements
 * @property {number} [mouseStepsMin=10] - Minimum steps for mouse movement
 * @property {number} [mouseStepsMax=30] - Maximum steps for mouse movement
 * @property {number} [mousePauseMin=500] - Minimum pause between mouse movements (ms)
 * @property {number} [mousePauseMax=1500] - Maximum pause between mouse movements (ms)
 */

const DEFAULT_CONFIG = {
  initialDelay: 2000,
  initialDelayVariance: 3000,
  minMouseMoves: 3,
  maxMouseMoves: 8,
  mouseStepsMin: 10,
  mouseStepsMax: 30,
  mousePauseMin: 500,
  mousePauseMax: 1500,
};

/**
 * Simulates random scrolling behavior
 * @param {Object} page - Puppeteer page object
 * @returns {Promise<void>}
 */
const simulateScrolling = async (page) => {
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
};

/**
 * Simulates human-like behavior on a webpage including mouse movements and scrolling
 * @param {Object} page - Puppeteer page object
 * @param {SimulationConfig} [config] - Configuration options
 * @returns {Promise<void>}
 */
async function simulateHumanBehavior(page, config = {}) {
  const settings = { ...DEFAULT_CONFIG, ...config };

  const viewportSize = page.viewportSize();
  if (!viewportSize) {
    throw new Error("Unable to get viewport size");
  }

  try {
    // Initial random delay
    await page.waitForTimeout(
      settings.initialDelay + Math.random() * settings.initialDelayVariance
    );

    // Random mouse movements
    const numMouseMoves =
      settings.minMouseMoves +
      Math.floor(
        Math.random() * (settings.maxMouseMoves - settings.minMouseMoves)
      );

    for (let i = 0; i < numMouseMoves; i++) {
      const x = Math.floor(Math.random() * viewportSize.width);
      const y = Math.floor(Math.random() * viewportSize.height);

      await page.mouse.move(x, y, {
        steps:
          settings.mouseStepsMin +
          Math.floor(
            Math.random() * (settings.mouseStepsMax - settings.mouseStepsMin)
          ),
      });

      await page.waitForTimeout(
        settings.mousePauseMin +
          Math.random() * (settings.mousePauseMax - settings.mousePauseMin)
      );
    }

    // Simulate scrolling behavior
    await simulateScrolling(page);

    // Final random delay
    await page.waitForTimeout(1000 + Math.random() * 2000);
  } catch (error) {
    console.error("Error in human behavior simulation:", error.message);
    throw error; // Re-throw to allow caller to handle the error
  }
}

export { simulateHumanBehavior, DEFAULT_CONFIG };
