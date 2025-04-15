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
 * @property {boolean} [enableAdvancedInteractions=false] - Whether to enable more complex interactions
 * @property {boolean} [enableTimingVariance=true] - Whether to apply random timing variations
 * @property {number} [sessionVariabilityFactor=0] - Factor to introduce session-to-session variability (0-1)
 * @property {boolean} [simulateNetworkDelay=true] - Whether to simulate network delay for page loading
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
  enableAdvancedInteractions: true,
  enableTimingVariance: true,
  sessionVariabilityFactor: 0.2, // 20% variance between sessions
  simulateNetworkDelay: true,
  interactionFailureRate: 0.02, // 2% chance of a simulated interaction "failure" that's handled gracefully
  mouseAcceleration: {
    // Simulate natural mouse acceleration and deceleration
    initialSpeed: 0.5,
    acceleration: 0.2,
    deceleration: 0.3,
    jitter: 0.1
  }
};

// Store behavior patterns to ensure consistency within a session
// but variability between sessions
const sessionBehaviorPatterns = {
  scrollStyle: null, // 'smooth', 'chunky', 'erratic'
  mouseStyle: null,  // 'direct', 'curved', 'hesitant'
  interactionDelay: null, // base delay factor
  lastReset: Date.now()
};

// Reset behavior patterns periodically to simulate different users
const resetBehaviorPatterns = () => {
  // Reset patterns after 24 hours or if not initialized
  if (!sessionBehaviorPatterns.scrollStyle || Date.now() - sessionBehaviorPatterns.lastReset > 24 * 60 * 60 * 1000) {
    const scrollStyles = ['smooth', 'chunky', 'erratic'];
    const mouseStyles = ['direct', 'curved', 'hesitant'];
    
    sessionBehaviorPatterns.scrollStyle = scrollStyles[Math.floor(Math.random() * scrollStyles.length)];
    sessionBehaviorPatterns.mouseStyle = mouseStyles[Math.floor(Math.random() * mouseStyles.length)];
    sessionBehaviorPatterns.interactionDelay = 0.8 + (Math.random() * 0.4); // 0.8 to 1.2 factor
    sessionBehaviorPatterns.lastReset = Date.now();
    
    console.log(`Human behavior patterns reset: ${sessionBehaviorPatterns.mouseStyle} mouse, ${sessionBehaviorPatterns.scrollStyle} scrolling`);
  }
  
  return sessionBehaviorPatterns;
};

/**
 * Simulates a natural mouse movement path between two points
 * @param {number} startX - Starting X coordinate
 * @param {number} startY - Starting Y coordinate
 * @param {number} endX - Ending X coordinate
 * @param {number} endY - Ending Y coordinate
 * @param {number} steps - Number of steps to take
 * @param {string} style - Movement style: 'direct', 'curved', or 'hesitant'
 * @returns {Array<{x: number, y: number}>} Array of coordinate points forming the path
 */
const calculateMousePath = (startX, startY, endX, endY, steps, style = 'direct') => {
  const path = [];
  path.push({ x: startX, y: startY });
  
  // Direct movement (mostly straight line with slight jitter)
  if (style === 'direct') {
    for (let i = 1; i < steps; i++) {
      const ratio = i / steps;
      const x = startX + (endX - startX) * ratio + (Math.random() * 10 - 5);
      const y = startY + (endY - startY) * ratio + (Math.random() * 10 - 5);
      path.push({ x: Math.round(x), y: Math.round(y) });
    }
  } 
  // Curved movement (follows a bezier-like curve)
  else if (style === 'curved') {
    // Control point for the curve
    const ctrlX = startX + (endX - startX) * 0.5 + (Math.random() * 100 - 50);
    const ctrlY = startY + (endY - startY) * 0.5 + (Math.random() * 100 - 50);
    
    for (let i = 1; i < steps; i++) {
      const t = i / steps;
      // Quadratic bezier curve formula
      const x = Math.pow(1 - t, 2) * startX + 2 * (1 - t) * t * ctrlX + Math.pow(t, 2) * endX;
      const y = Math.pow(1 - t, 2) * startY + 2 * (1 - t) * t * ctrlY + Math.pow(t, 2) * endY;
      path.push({ x: Math.round(x), y: Math.round(y) });
    }
  }
  // Hesitant movement (stops occasionally and changes direction slightly)
  else if (style === 'hesitant') {
    let currentX = startX;
    let currentY = startY;
    
    for (let i = 1; i < steps; i++) {
      const ratio = i / steps;
      
      // Occasionally pause (create a duplicate point)
      if (Math.random() < 0.2) {
        path.push({ x: Math.round(currentX), y: Math.round(currentY) });
        continue;
      }
      
      // Occasionally make a small detour
      if (Math.random() < 0.15) {
        const detourX = currentX + (Math.random() * 20 - 10);
        const detourY = currentY + (Math.random() * 20 - 10);
        path.push({ x: Math.round(detourX), y: Math.round(detourY) });
      }
      
      // Continue toward destination
      currentX = startX + (endX - startX) * ratio + (Math.random() * 15 - 7.5);
      currentY = startY + (endY - startY) * ratio + (Math.random() * 15 - 7.5);
      path.push({ x: Math.round(currentX), y: Math.round(currentY) });
    }
  }
  
  // Always ensure the final point is exactly the target
  path.push({ x: endX, y: endY });
  return path;
};

/**
 * Simulates random scrolling behavior
 * @param {Object} page - Puppeteer page object
 * @param {string} style - Scrolling style: 'smooth', 'chunky', or 'erratic'
 * @returns {Promise<void>}
 */
const simulateScrolling = async (page, style = 'smooth') => {
  await page.evaluate((scrollStyle) => {
    return new Promise((resolve) => {
      let steps = 0;
      const maxSteps = 5 + Math.floor(Math.random() * 5);

      const scroll = () => {
        if (steps >= maxSteps) {
          resolve();
          return;
        }

        let amount, behavior;
        
        // Different scrolling behaviors based on style
        if (scrollStyle === 'smooth') {
          amount = 100 + Math.random() * 300;
          behavior = "smooth";
        } else if (scrollStyle === 'chunky') {
          amount = 200 + Math.random() * 600;
          behavior = "auto";
        } else if (scrollStyle === 'erratic') {
          // Occasionally scroll back up
          amount = Math.random() < 0.2 ? 
                  -(50 + Math.random() * 100) : 
                  (100 + Math.random() * 400);
          behavior = Math.random() < 0.5 ? "smooth" : "auto";
        } else {
          amount = 100 + Math.random() * 300;
          behavior = "smooth";
        }

        window.scrollBy({
          top: amount,
          behavior: behavior
        });

        steps++;
        setTimeout(scroll, 1000 + Math.random() * 2000);
      };

      scroll();
    });
  }, style);
};

/**
 * Simulates interaction with random page elements
 * @param {Object} page - Puppeteer page object
 * @returns {Promise<void>}
 */
const simulateRandomInteractions = async (page) => {
  try {
    // Find clickable elements
    const clickableElements = await page.evaluate(() => {
      const elements = [];
      // Buttons
      document.querySelectorAll('button, a, [role="button"], .btn').forEach((el) => {
        if (el.offsetWidth > 0 && el.offsetHeight > 0 && el.style.display !== 'none' && el.style.visibility !== 'hidden') {
          const rect = el.getBoundingClientRect();
          elements.push({
            type: 'click',
            x: rect.left + rect.width / 2,
            y: rect.top + rect.height / 2,
            width: rect.width,
            height: rect.height,
            tag: el.tagName,
            text: el.textContent?.trim().substring(0, 20) || ''
          });
        }
      });
      
      // Form fields
      document.querySelectorAll('input, select, textarea').forEach((el) => {
        if (el.offsetWidth > 0 && el.offsetHeight > 0 && el.style.display !== 'none' && el.style.visibility !== 'hidden') {
          const rect = el.getBoundingClientRect();
          elements.push({
            type: 'input',
            x: rect.left + rect.width / 2,
            y: rect.top + rect.height / 2,
            width: rect.width,
            height: rect.height,
            tag: el.tagName,
            inputType: el.type || 'text'
          });
        }
      });
      
      return elements;
    });
    
    // Skip if no elements found
    if (!clickableElements.length) return;
    
    // Select 1-2 random elements to interact with
    const numInteractions = Math.floor(Math.random() * 2) + 1;
    for (let i = 0; i < Math.min(numInteractions, clickableElements.length); i++) {
      const elementIndex = Math.floor(Math.random() * clickableElements.length);
      const element = clickableElements[elementIndex];
      
      // Remove element from array to avoid choosing it again
      clickableElements.splice(elementIndex, 1);
      
      // Skip small elements (might be close buttons or things we don't want to click)
      if (element.width < 10 || element.height < 10) continue;
      
      // Hover over element first
      await page.mouse.move(element.x, element.y, { steps: 5 });
      await page.waitForTimeout(300 + Math.random() * 500);
      
      // Interact based on element type
      if (element.type === 'click') {
        // Only click if it's not something that would navigate away or submit
        const avoidTexts = ['submit', 'login', 'sign', 'continue', 'next', 'buy', 'purchase', 'checkout'];
        const shouldAvoid = avoidTexts.some(text => 
          element.text.toLowerCase().includes(text)
        );
        
        if (!shouldAvoid) {
          // Sometimes just hover without clicking
          if (Math.random() < 0.5) {
            console.log(`Clicking element: ${element.tag} "${element.text}"`);
            await page.mouse.click(element.x, element.y);
            await page.waitForTimeout(500 + Math.random() * 1000);
          }
        }
      } else if (element.type === 'input' && Math.random() < 0.3) {
        // Only interact with certain input types
        if (['text', 'search', 'email'].includes(element.inputType)) {
          await page.mouse.click(element.x, element.y);
          await page.waitForTimeout(300 + Math.random() * 300);
          
          // Type something then delete it
          const randomText = "test123";
          await page.keyboard.type(randomText, { delay: 100 + Math.random() * 100 });
          await page.waitForTimeout(500 + Math.random() * 500);
          
          // Delete the text
          for (let j = 0; j < randomText.length; j++) {
            await page.keyboard.press('Backspace');
            await page.waitForTimeout(50 + Math.random() * 50);
          }
        }
      }
    }
  } catch (error) {
    console.log("Error during random interactions:", error.message);
    // Just continue, this is a non-critical feature
  }
};

/**
 * Simulates human-like behavior on a webpage including mouse movements and scrolling
 * @param {Object} page - Puppeteer page object
 * @param {SimulationConfig} [config] - Configuration options
 * @returns {Promise<void>}
 */
async function simulateHumanBehavior(page, config = {}) {
  // Ensure behavior patterns are initialized
  const patterns = resetBehaviorPatterns();
  
  const settings = { ...DEFAULT_CONFIG, ...config };
  
  // Apply session-specific behavior patterns
  settings.mouseMovementStyle = patterns.mouseStyle;
  settings.scrollingStyle = patterns.scrollStyle;
  
  // Apply timing variability based on the session pattern
  if (settings.enableTimingVariance && patterns.interactionDelay) {
    settings.mousePauseMin *= patterns.interactionDelay;
    settings.mousePauseMax *= patterns.interactionDelay;
    settings.initialDelay *= patterns.interactionDelay;
  }

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

    // Get current mouse position as starting point
    let currentPosition = { x: 0, y: 0 };
    try {
      currentPosition = await page.evaluate(() => {
        return { 
          x: window.mouseX || 0,
          y: window.mouseY || 0
        };
      });
    } catch (e) {
      // If we can't get the position, start from top-left corner
      currentPosition = { x: 0, y: 0 };
    }

    for (let i = 0; i < numMouseMoves; i++) {
      // Target position for this movement
      const targetX = Math.floor(Math.random() * viewportSize.width);
      const targetY = Math.floor(Math.random() * viewportSize.height);
      
      // Calculate path based on the session's mouse movement style
      const steps = settings.mouseStepsMin +
        Math.floor(Math.random() * (settings.mouseStepsMax - settings.mouseStepsMin));
        
      const path = calculateMousePath(
        currentPosition.x, 
        currentPosition.y,
        targetX,
        targetY,
        steps,
        settings.mouseMovementStyle
      );
      
      // Execute the mouse movement along the path
      for (let j = 1; j < path.length; j++) {
        await page.mouse.move(path[j].x, path[j].y);
        
        // Variable delay between movement points
        const pointDelay = Math.random() * 20;
        if (pointDelay > 5) {
          await page.waitForTimeout(pointDelay);
        }
      }
      
      // Update current position
      currentPosition = { x: targetX, y: targetY };

      // Random pause between major movements
      await page.waitForTimeout(
        settings.mousePauseMin +
          Math.random() * (settings.mousePauseMax - settings.mousePauseMin)
      );
      
      // Occasionally simulate a click (but not on first or last movement)
      if (i > 0 && i < numMouseMoves - 1 && Math.random() < 0.3) {
        await page.mouse.click(currentPosition.x, currentPosition.y);
        await page.waitForTimeout(300 + Math.random() * 700);
      }
    }

    // Simulate scrolling behavior with the session's scroll style
    await simulateScrolling(page, settings.scrollingStyle);
    
    // Add advanced interactions if enabled
    if (settings.enableAdvancedInteractions && Math.random() < 0.5) {
      await simulateRandomInteractions(page);
    }

    // Simulate tabbing through elements
    if (Math.random() < 0.3) {
      const tabCount = Math.floor(Math.random() * 5) + 1;
      for (let i = 0; i < tabCount; i++) {
        await page.keyboard.press('Tab');
        await page.waitForTimeout(300 + Math.random() * 700);
      }
    }

    // Final random delay
    await page.waitForTimeout(1000 + Math.random() * 2000);
    
    // Occasionally do browser-specific actions
    if (Math.random() < 0.1) {
      try {
        // This simulates a user action like opening dev tools or checking browser info
        await page.evaluate(() => {
          // Add a user identifier to storage to simulate persistence
          const sessionId = localStorage.getItem('sessionId') || 
                           `user_${Date.now().toString(36)}_${Math.random().toString(36).substr(2, 5)}`;
          localStorage.setItem('sessionId', sessionId);
          
          // Track page visits to make behavior more realistic
          const pageVisits = parseInt(localStorage.getItem('pageVisits') || '0', 10);
          localStorage.setItem('pageVisits', (pageVisits + 1).toString());
          
          // Return session info for logging
          return { sessionId, pageVisits: pageVisits + 1 };
        });
      } catch (e) {
        // Ignore errors with localStorage
      }
    }
  } catch (error) {
    console.error("Error in human behavior simulation:", error.message);
    // Don't throw, just continue with the task
  }
}

export { simulateHumanBehavior, DEFAULT_CONFIG, calculateMousePath, simulateScrolling };
