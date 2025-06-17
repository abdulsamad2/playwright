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
  // Timing settings
  initialDelay: 1200, // Reduced for less predictability
  initialDelayVariance: 5000, // Increased for more randomness
  minMouseMoves: 5, // Increased for more natural behavior
  maxMouseMoves: 15, // Increased for more exploration
  mouseStepsMin: 20, // Increased for smoother movements
  mouseStepsMax: 50, // Increased for more natural curves
  mousePauseMin: 250, // Reduced for more responsive movements
  mousePauseMax: 2500, // Increased for more natural pauses
  
  // Interaction settings
  enableAdvancedInteractions: true,
  enableTimingVariance: true,
  sessionVariabilityFactor: 0.45, // Increased for more human-like variance
  simulateNetworkDelay: true,
  interactionFailureRate: 0.05, // Increased - humans make mistakes
  
  // Mouse physics (more realistic acceleration/deceleration)
  mouseAcceleration: {
    initialSpeed: 0.25, // Reduced for more gradual start
    acceleration: 0.12, // Reduced for smoother acceleration
    deceleration: 0.22, // Adjusted for smoother stops
    jitter: 0.18, // Increased for more natural imprecision
    curveIntensity: 0.7, // Increased curve intensity
    naturalMovement: true, // Enable natural movement patterns
    preferredEdgeDistance: 15, // Avoid moving too close to screen edges
    doubleClickProbability: 0.03, // Occasional double clicks
  },
  
  // Enhanced typing settings
  typingSettings: {
    baseSpeed: 220, // Base typing speed (ms)
    speedVariance: 180, // Increased variance in typing speed (ms)
    errorRate: 0.04, // Chance of making a typo
    correctionDelay: [300, 800], // Range for correction delay (ms)
    burstProbability: 0.25, // Chance of typing a burst of characters quickly
    pauseProbability: 0.15, // Chance of pausing while typing
    pauseDuration: [150, 1200], // Range of pause duration (ms)
    rhythmVariance: true, // Vary rhythm between keystrokes
    longPauseProbability: 0.03, // Chance of a longer thinking pause
    longPauseDuration: [1000, 3000], // Range for thinking pauses
    keyHoldTime: [80, 200], // How long keys are held down
  },
  
  // Advanced bot detection evasion
  botDetectionEvasion: {
    randomizeUserAgent: true,
    simulatePlugins: true,
    simulateWebGL: true,
    evasionLevel: 'high', // 'low', 'medium', 'high'
    handleRecaptcha: true,
    handleHCaptcha: true,
    // New advanced evasion techniques
    fingerprintConsistency: true, // Maintain consistent fingerprints in a session
    canvasNoiseLevel: 0.5, // Add subtle noise to canvas fingerprinting
    audioFingerprintNoise: true, // Add noise to audio fingerprinting
    screenResolutionVariance: true, // Small variations in reported screen resolution
    timeZoneConsistency: true, // Consistent timezone reporting
    hardwareConcurrency: [2, 8], // Range of reported CPU cores
    deviceMemory: [4, 16], // Range of reported device memory (GB)
    touchEmulation: true, // Emulate touch events on mobile user agents
    batteryDischarging: true, // Simulate battery discharge over session
    cookieConsistency: true, // Maintain consistent cookie behavior
  },
  
  // New behavioral patterns
  behavioralPatterns: {
    readingPause: true, // Pause to simulate reading content
    readingSpeed: [200, 400], // Words per minute reading speed
    interactionConsistency: true, // Consistent interaction patterns
    formFillStrategy: 'natural', // 'direct', 'natural', 'hesitant'
    navigationPatterns: 'explore', // 'direct', 'explore', 'return'
    attentionSpan: [20000, 60000], // How long to focus on one area (ms)
    interestVariability: 0.6, // How much interest varies by content
  },
};

// This block is intentionally left empty as it's being replaced by the updated version above

// Store behavior patterns to ensure consistency within a session
// but variability between sessions
const sessionBehaviorPatterns = {
  scrollStyle: null, // 'smooth', 'chunky', 'erratic'
  mouseStyle: null, // 'direct', 'curved', 'hesitant', 'natural'
  interactionDelay: null, // base delay factor
  formFillStyle: null, // 'direct', 'natural', 'hesitant'
  readingSpeed: null, // words per minute
  attentionSpan: null, // how long to focus on one area (ms)
  lastReset: Date.now(),
  // Fingerprinting consistency
  fingerprint: {
    timezone: null,
    language: null,
    platform: null,
    hardwareConcurrency: null,
    deviceMemory: null,
    screenResolution: null,
  }
};

/**
 * Reset behavior patterns periodically to simulate different users
 * @returns {Object} The current session behavior patterns
 */
const resetBehaviorPatterns = () => {
  // Reset patterns after 24 hours or if not initialized
  if (
    !sessionBehaviorPatterns.mouseStyle ||
    Date.now() - sessionBehaviorPatterns.lastReset > 24 * 60 * 60 * 1000
  ) {
    const scrollStyles = ["smooth", "chunky", "erratic"];
    const mouseStyles = ["direct", "curved", "hesitant", "natural"];
    const formFillStyles = ["direct", "natural", "hesitant"];
    
    // Weight the mouse styles to favor more natural movements
    const mouseStyleWeights = [0.15, 0.25, 0.2, 0.4]; // 40% chance of natural
    
    // Select mouse style based on weights
    let randomValue = Math.random();
    let cumulativeWeight = 0;
    let selectedMouseStyle = mouseStyles[mouseStyles.length - 1]; // Default to last style
    
    for (let i = 0; i < mouseStyles.length; i++) {
      cumulativeWeight += mouseStyleWeights[i];
      if (randomValue <= cumulativeWeight) {
        selectedMouseStyle = mouseStyles[i];
        break;
      }
    }

    // Set the behavior patterns
    sessionBehaviorPatterns.scrollStyle =
      scrollStyles[Math.floor(Math.random() * scrollStyles.length)];
    sessionBehaviorPatterns.mouseStyle = selectedMouseStyle;
    sessionBehaviorPatterns.formFillStyle =
      formFillStyles[Math.floor(Math.random() * formFillStyles.length)];
    
    // More varied interaction delay (0.7 to 1.3 factor)
    sessionBehaviorPatterns.interactionDelay = 0.7 + Math.random() * 0.6;
    
    // Reading speed (words per minute)
    sessionBehaviorPatterns.readingSpeed = 
      Math.floor(Math.random() * 200) + 200; // 200-400 wpm
    
    // Attention span (how long to focus on one area)
    sessionBehaviorPatterns.attentionSpan = 
      Math.floor(Math.random() * 40000) + 20000; // 20-60 seconds
    
    // Set consistent fingerprinting values for this session
    if (DEFAULT_CONFIG.botDetectionEvasion.fingerprintConsistency) {
      const timezones = [
        'America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles',
        'Europe/London', 'Europe/Paris', 'Europe/Berlin', 'Asia/Tokyo', 'Asia/Shanghai',
        'Australia/Sydney'
      ];
      
      const languages = [
        'en-US', 'en-GB', 'es-ES', 'fr-FR', 'de-DE', 'it-IT', 'ja-JP', 'zh-CN'
      ];
      
      const platforms = ['Win32', 'MacIntel', 'Linux x86_64'];
      
      sessionBehaviorPatterns.fingerprint = {
        timezone: timezones[Math.floor(Math.random() * timezones.length)],
        language: languages[Math.floor(Math.random() * languages.length)],
        platform: platforms[Math.floor(Math.random() * platforms.length)],
        hardwareConcurrency: Math.floor(Math.random() * 6) + 2, // 2-8 cores
        deviceMemory: Math.pow(2, Math.floor(Math.random() * 4) + 2), // 4, 8, or 16 GB
        screenResolution: {
          width: 1920 + (Math.floor(Math.random() * 5) * 16), // Small variations
          height: 1080 + (Math.floor(Math.random() * 5) * 9)
        }
      };
    }
    
    sessionBehaviorPatterns.lastReset = Date.now();

    console.log(
      `Human behavior patterns reset: ${sessionBehaviorPatterns.mouseStyle} mouse, ` +
      `${sessionBehaviorPatterns.scrollStyle} scrolling, ` +
      `${sessionBehaviorPatterns.formFillStyle} form filling`
    );
  }

  return sessionBehaviorPatterns;
};

/**
 * Simulates a natural mouse movement path between two points with realistic physics
 * @param {number} startX - Starting X coordinate
 * @param {number} startY - Starting Y coordinate
 * @param {number} endX - Ending X coordinate
 * @param {number} endY - Ending Y coordinate
 * @param {number} steps - Number of steps to take
 * @param {string} style - Movement style: 'direct', 'curved', 'hesitant', or 'natural'
 * @returns {Array<{x: number, y: number}>} Array of coordinate points forming the path
 */
const calculateMousePath = (
  startX,
  startY,
  endX,
  endY,
  steps,
  style = "direct"
) => {
  const path = [];
  path.push({ x: startX, y: startY });
  
  // Apply a small random offset to the end position (humans aren't perfectly precise)
  const targetX = endX + (Math.random() * 2 - 1);
  const targetY = endY + (Math.random() * 2 - 1);
  
  // Calculate distance for speed adjustments
  const distance = Math.sqrt(Math.pow(targetX - startX, 2) + Math.pow(targetY - startY, 2));
  const isLongDistance = distance > 200;
  
  // Direct movement (mostly straight line with slight jitter)
  if (style === "direct") {
    // Apply velocity profile (start slow, speed up, end slow)
    for (let i = 1; i < steps; i++) {
      const ratio = i / steps;
      
      // Apply acceleration/deceleration curve (ease in-out)
      let adjustedRatio;
      if (ratio < 0.5) {
        // Accelerate (ease in)
        adjustedRatio = 2 * ratio * ratio;
      } else {
        // Decelerate (ease out)
        adjustedRatio = -1 + (4 - 2 * ratio) * ratio;
      }
      
      // Calculate position with natural jitter that increases with speed
      const speedFactor = 4 * adjustedRatio * (1 - adjustedRatio); // Peaks at 0.5
      const jitterAmount = 2 + speedFactor * 8; // More jitter at peak speed
      
      const x = startX + (targetX - startX) * adjustedRatio + (Math.random() * jitterAmount - jitterAmount/2);
      const y = startY + (targetY - startY) * adjustedRatio + (Math.random() * jitterAmount - jitterAmount/2);
      
      path.push({ x: Math.round(x), y: Math.round(y) });
    }
  }
  // Curved movement (follows a bezier-like curve with natural hand movement)
  else if (style === "curved") {
    // For longer distances, use multiple control points for more natural curves
    if (isLongDistance && steps > 20) {
      // Create 2-3 control points for complex curve
      const numControlPoints = Math.floor(Math.random() * 2) + 2;
      const controlPoints = [];
      
      for (let i = 0; i < numControlPoints; i++) {
        const ratio = (i + 1) / (numControlPoints + 1);
        const perpDistance = Math.min(100, distance * 0.5) * (Math.random() * 0.8 + 0.6);
        const angle = Math.PI * (Math.random() * 0.3 + 0.85); // Mostly perpendicular
        
        controlPoints.push({
          x: startX + (targetX - startX) * ratio + Math.cos(angle) * perpDistance * (Math.random() > 0.5 ? 1 : -1),
          y: startY + (targetY - startY) * ratio + Math.sin(angle) * perpDistance * (Math.random() > 0.5 ? 1 : -1)
        });
      }
      
      // Generate path using multiple control points
      for (let i = 1; i < steps; i++) {
        const t = i / steps;
        let x = startX * Math.pow(1 - t, numControlPoints + 1);
        let y = startY * Math.pow(1 - t, numControlPoints + 1);
        
        // Apply each control point's influence
        for (let j = 0; j < numControlPoints; j++) {
          const coefficient = (numControlPoints + 1) * Math.pow(t, j + 1) * Math.pow(1 - t, numControlPoints - j);
          x += coefficient * controlPoints[j].x;
        }
        
        // Add final point influence
        x += Math.pow(t, numControlPoints + 1) * targetX;
        y += Math.pow(t, numControlPoints + 1) * targetY;
        
        // Add subtle jitter that varies with speed
        const speedFactor = 4 * t * (1 - t);
        const jitter = 1 + speedFactor * 2;
        x += (Math.random() * jitter - jitter/2);
        y += (Math.random() * jitter - jitter/2);
        
        path.push({ x: Math.round(x), y: Math.round(y) });
      }
    } else {
      // Simpler curve for shorter distances
      // Control point with variable offset based on distance
      const offsetFactor = Math.min(0.5, 50 / distance);
      const ctrlX = startX + (targetX - startX) * 0.5 + ((Math.random() * 2 - 1) * distance * offsetFactor);
      const ctrlY = startY + (targetY - startY) * 0.5 + ((Math.random() * 2 - 1) * distance * offsetFactor);

      for (let i = 1; i < steps; i++) {
        const t = i / steps;
        
        // Apply acceleration/deceleration
        let adjustedT;
        if (t < 0.2) {
          // Slow start (ease in)
          adjustedT = 5 * t * t / 2;
        } else if (t > 0.8) {
          // Slow end (ease out)
          adjustedT = 0.8 + (t - 0.8) * 0.5 * (1 + (t - 0.8) / 0.2);
        } else {
          // Regular speed
          adjustedT = t;
        }
        
        // Quadratic bezier curve formula with adjusted timing
        const x =
          Math.pow(1 - adjustedT, 2) * startX +
          2 * (1 - adjustedT) * adjustedT * ctrlX +
          Math.pow(adjustedT, 2) * targetX;
        const y =
          Math.pow(1 - adjustedT, 2) * startY +
          2 * (1 - adjustedT) * adjustedT * ctrlY +
          Math.pow(adjustedT, 2) * targetY;
          
        // Add subtle jitter that varies with speed
        const speedFactor = 4 * adjustedT * (1 - adjustedT);
        const jitter = 1 + speedFactor * 2;
        
        path.push({ 
          x: Math.round(x + (Math.random() * jitter - jitter/2)), 
          y: Math.round(y + (Math.random() * jitter - jitter/2)) 
        });
      }
    }
  }
  // Hesitant movement (stops occasionally and changes direction slightly)
  else if (style === "hesitant") {
    let currentX = startX;
    let currentY = startY;
    let lastPauseStep = 0;

    for (let i = 1; i < steps; i++) {
      const ratio = i / steps;
      const stepsSincePause = i - lastPauseStep;

      // Occasionally pause (create a duplicate point) - more likely after moving for a while
      const pauseProbability = 0.05 + (stepsSincePause / steps) * 0.3;
      if (Math.random() < pauseProbability && stepsSincePause > 3) {
        // Add slight movement during pause (hand tremor)
        const tremor = Math.random() * 2 - 1;
        path.push({ 
          x: Math.round(currentX + tremor), 
          y: Math.round(currentY + tremor) 
        });
        lastPauseStep = i;
        continue;
      }

      // Occasionally make a small detour or correction
      if (Math.random() < 0.2) {
        // Detour size increases with distance from target
        const remainingDistance = Math.sqrt(
          Math.pow(targetX - currentX, 2) + 
          Math.pow(targetY - currentY, 2)
        );
        const detourSize = Math.min(20, remainingDistance * 0.2);
        
        const detourX = currentX + (Math.random() * detourSize - detourSize/2);
        const detourY = currentY + (Math.random() * detourSize - detourSize/2);
        path.push({ x: Math.round(detourX), y: Math.round(detourY) });
        
        // Update current position to detour position
        currentX = detourX;
        currentY = detourY;
        continue;
      }

      // Continue toward destination with variable speed
      // Slower at beginning and end, faster in middle
      let speedFactor;
      if (ratio < 0.2) {
        speedFactor = ratio * 5; // Accelerate
      } else if (ratio > 0.8) {
        speedFactor = (1 - ratio) * 5; // Decelerate
      } else {
        speedFactor = 1; // Full speed
      }
      
      // Move toward target with jitter proportional to speed
      const step = Math.max(0.01, speedFactor * 0.05); // Step size varies with speed
      const jitter = 3 + speedFactor * 12; // More jitter at higher speeds
      
      currentX = currentX + (targetX - currentX) * step + (Math.random() * jitter - jitter/2);
      currentY = currentY + (targetY - currentY) * step + (Math.random() * jitter - jitter/2);
      
      path.push({ x: Math.round(currentX), y: Math.round(currentY) });
    }
  }
  // New natural style - combines aspects of all styles with realistic physics
  else if (style === "natural") {
    // Natural movement uses a combination of direct and curved paths with realistic physics
    
    // Create a main control point for the overall curve
    const mainCtrlX = startX + (targetX - startX) * 0.5 + (Math.random() * 50 - 25);
    const mainCtrlY = startY + (targetY - startY) * 0.5 + (Math.random() * 50 - 25);
    
    // Add 1-2 minor control points for more natural movement
    const useSecondaryControlPoints = Math.random() < 0.7; // 70% chance
    const controlPoints = [];
    
    if (useSecondaryControlPoints) {
      const numPoints = Math.floor(Math.random() * 2) + 1;
      for (let i = 0; i < numPoints; i++) {
        const t = (i + 1) / (numPoints + 1);
        controlPoints.push({
          t: t,
          x: startX + (targetX - startX) * t + (Math.random() * 30 - 15),
          y: startY + (targetY - startY) * t + (Math.random() * 30 - 15)
        });
      }
    }
    
    // Calculate velocity profile - humans move with variable speed
    const velocityProfile = [];
    let currentVelocity = 0;
    const maxVelocity = 1 + Math.random() * 0.5; // Random max velocity
    const acceleration = 0.05 + Math.random() * 0.05;
    const deceleration = 0.05 + Math.random() * 0.05;
    
    // Generate velocity for each step
    for (let i = 0; i < steps; i++) {
      const t = i / steps;
      
      // Accelerate in first third, maintain in middle, decelerate in final third
      if (t < 0.3) {
        currentVelocity = Math.min(maxVelocity, currentVelocity + acceleration);
      } else if (t > 0.7) {
        currentVelocity = Math.max(0.1, currentVelocity - deceleration);
      }
      
      // Add some random fluctuations to velocity
      const fluctuation = 1 + (Math.random() * 0.2 - 0.1);
      velocityProfile.push(currentVelocity * fluctuation);
    }
    
    // Normalize velocities to ensure we reach the target
    const totalVelocity = velocityProfile.reduce((sum, v) => sum + v, 0);
    const normalizedVelocities = velocityProfile.map(v => v / totalVelocity);
    
    // Generate cumulative distances
    let cumulativeDistance = 0;
    const cumulativeDistances = [0];
    for (let i = 0; i < normalizedVelocities.length; i++) {
      cumulativeDistance += normalizedVelocities[i];
      cumulativeDistances.push(cumulativeDistance);
    }
    
    // Generate path points based on velocity profile
    for (let i = 1; i < steps; i++) {
      const t = cumulativeDistances[i]; // Use normalized cumulative distance as time parameter
      
      // Base position from quadratic bezier
      let x = Math.pow(1 - t, 2) * startX + 2 * (1 - t) * t * mainCtrlX + Math.pow(t, 2) * targetX;
      let y = Math.pow(1 - t, 2) * startY + 2 * (1 - t) * t * mainCtrlY + Math.pow(t, 2) * targetY;
      
      // Apply influence from secondary control points if they exist
      if (useSecondaryControlPoints) {
        for (const point of controlPoints) {
          // Influence decreases with distance from the control point's t value
          const influence = Math.max(0, 1 - Math.abs(t - point.t) * 10);
          if (influence > 0) {
            x += (point.x - x) * influence * 0.3;
            y += (point.y - y) * influence * 0.3;
          }
        }
      }
      
      // Add human-like jitter that varies with speed
      const currentSpeed = normalizedVelocities[i-1] * steps; // Rescale for better jitter
      const jitterAmount = 0.5 + currentSpeed * 2;
      
      x += (Math.random() * jitterAmount - jitterAmount/2);
      y += (Math.random() * jitterAmount - jitterAmount/2);
      
      path.push({ x: Math.round(x), y: Math.round(y) });
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
const simulateScrolling = async (page, style = "smooth") => {
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
        if (scrollStyle === "smooth") {
          amount = 100 + Math.random() * 300;
          behavior = "smooth";
        } else if (scrollStyle === "chunky") {
          amount = 200 + Math.random() * 600;
          behavior = "auto";
        } else if (scrollStyle === "erratic") {
          // Occasionally scroll back up
          amount =
            Math.random() < 0.2
              ? -(50 + Math.random() * 100)
              : 100 + Math.random() * 400;
          behavior = Math.random() < 0.5 ? "smooth" : "auto";
        } else {
          amount = 100 + Math.random() * 300;
          behavior = "smooth";
        }

        window.scrollBy({
          top: amount,
          behavior: behavior,
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
      document
        .querySelectorAll('button, a, [role="button"], .btn')
        .forEach((el) => {
          if (
            el.offsetWidth > 0 &&
            el.offsetHeight > 0 &&
            el.style.display !== "none" &&
            el.style.visibility !== "hidden"
          ) {
            const rect = el.getBoundingClientRect();
            elements.push({
              type: "click",
              x: rect.left + rect.width / 2,
              y: rect.top + rect.height / 2,
              width: rect.width,
              height: rect.height,
              tag: el.tagName,
              text: el.textContent?.trim().substring(0, 20) || "",
            });
          }
        });

      // Form fields
      document.querySelectorAll("input, select, textarea").forEach((el) => {
        if (
          el.offsetWidth > 0 &&
          el.offsetHeight > 0 &&
          el.style.display !== "none" &&
          el.style.visibility !== "hidden"
        ) {
          const rect = el.getBoundingClientRect();
          elements.push({
            type: "input",
            x: rect.left + rect.width / 2,
            y: rect.top + rect.height / 2,
            width: rect.width,
            height: rect.height,
            tag: el.tagName,
            inputType: el.type || "text",
          });
        }
      });

      return elements;
    });

    // Skip if no elements found
    if (!clickableElements.length) return;

    // Select 1-2 random elements to interact with
    const numInteractions = Math.floor(Math.random() * 2) + 1;
    for (
      let i = 0;
      i < Math.min(numInteractions, clickableElements.length);
      i++
    ) {
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
      if (element.type === "click") {
        // Only click if it's not something that would navigate away or submit
        const avoidTexts = [
          "submit",
          "login",
          "sign",
          "continue",
          "next",
          "buy",
          "purchase",
          "checkout",
        ];
        const shouldAvoid = avoidTexts.some((text) =>
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
      } else if (element.type === "input" && Math.random() < 0.3) {
        // Only interact with certain input types
        if (["text", "search", "email"].includes(element.inputType)) {
          await page.mouse.click(element.x, element.y);
          await page.waitForTimeout(300 + Math.random() * 300);

          // Type something then delete it
          const randomText = "test123";
          await page.keyboard.type(randomText, {
            delay: 100 + Math.random() * 100,
          });
          await page.waitForTimeout(500 + Math.random() * 500);

          // Delete the text
          for (let j = 0; j < randomText.length; j++) {
            await page.keyboard.press("Backspace");
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
 * @param {Object} page - Puppeteer/Playwright page object
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
  settings.formFillStyle = patterns.formFillStyle || 'natural';

  // Apply timing variability based on the session pattern
  if (settings.enableTimingVariance && patterns.interactionDelay) {
    settings.mousePauseMin *= patterns.interactionDelay;
    settings.mousePauseMax *= patterns.interactionDelay;
    settings.initialDelay *= patterns.interactionDelay;
  }

  // Apply fingerprinting evasion if enabled
  if (settings.botDetectionEvasion.fingerprintConsistency && patterns.fingerprint) {
    try {
      await applyFingerprintEvasion(page, patterns.fingerprint);
    } catch (error) {
      console.log("Error applying fingerprint evasion:", error.message);
      // Continue anyway - non-critical feature
    }
  }

  const viewportSize = page.viewportSize();
  if (!viewportSize) {
    throw new Error("Unable to get viewport size");
  }

  try {
    // Initial random delay with natural variance
    const initialWait = settings.initialDelay + Math.random() * settings.initialDelayVariance;
    await page.waitForTimeout(initialWait);
    
    // Simulate initial page scanning behavior (humans scan pages before interacting)
    await simulatePageScanning(page, viewportSize);

    // Random mouse movements with improved natural behavior
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
          x: window.mouseX || window.innerWidth / 2 || 0,
          y: window.mouseY || window.innerHeight / 5 || 0, // Start higher on page typically
        };
      });
    } catch (e) {
      // If we can't get the position, start from a more natural position
      currentPosition = { 
        x: Math.floor(viewportSize.width * 0.5), 
        y: Math.floor(viewportSize.height * 0.2)
      };
    }

    // Track areas of interest for more realistic movement patterns
    const interestPoints = await identifyInterestPoints(page, viewportSize);
    
    for (let i = 0; i < numMouseMoves; i++) {
      // Target position for this movement - biased toward areas of interest
      let targetX, targetY;
      
      // Occasionally move to an area of interest (if we found any)
      if (interestPoints.length > 0 && Math.random() < 0.7) {
        const interestPoint = interestPoints[Math.floor(Math.random() * interestPoints.length)];
        // Add some randomness around the interest point
        targetX = interestPoint.x + (Math.random() * 40 - 20);
        targetY = interestPoint.y + (Math.random() * 40 - 20);
        
        // Keep within viewport bounds
        targetX = Math.max(10, Math.min(viewportSize.width - 10, targetX));
        targetY = Math.max(10, Math.min(viewportSize.height - 10, targetY));
      } else {
        // Random position but avoid extreme edges (humans rarely go to exact edges)
        const edgeBuffer = settings.mouseAcceleration.preferredEdgeDistance || 15;
        targetX = edgeBuffer + Math.floor(Math.random() * (viewportSize.width - 2 * edgeBuffer));
        targetY = edgeBuffer + Math.floor(Math.random() * (viewportSize.height - 2 * edgeBuffer));
      }

      // Calculate path based on the session's mouse movement style
      const steps =
        settings.mouseStepsMin +
        Math.floor(
          Math.random() * (settings.mouseStepsMax - settings.mouseStepsMin)
        );

      const path = calculateMousePath(
        currentPosition.x,
        currentPosition.y,
        targetX,
        targetY,
        steps,
        settings.mouseMovementStyle
      );

      // Execute the mouse movement along the path with realistic physics
      for (let j = 1; j < path.length; j++) {
        await page.mouse.move(path[j].x, path[j].y);

        // Variable delay between movement points based on speed
        // Faster in the middle of the movement, slower at start/end
        const movementProgress = j / path.length;
        const speedFactor = 4 * movementProgress * (1 - movementProgress); // Peaks at 0.5
        const pointDelay = Math.random() * 10 * speedFactor;
        
        if (pointDelay > 2) {
          await page.waitForTimeout(pointDelay);
        }
      }

      // Update current position
      currentPosition = { x: targetX, y: targetY };

      // Random pause between major movements with natural variance
      const pauseDuration = settings.mousePauseMin +
        Math.random() * (settings.mousePauseMax - settings.mousePauseMin);
      
      // Occasionally have a longer "thinking" pause
      const pauseMultiplier = Math.random() < 0.1 ? 2.5 : 1;
      
      await page.waitForTimeout(pauseDuration * pauseMultiplier);

      // Occasionally simulate clicks with realistic behavior
      if (i > 0 && i < numMouseMoves - 1) {
        // Higher chance of clicking on interest points
        const isOnInterestPoint = interestPoints.some(point => {
          return Math.abs(currentPosition.x - point.x) < 50 && 
                 Math.abs(currentPosition.y - point.y) < 50;
        });
        
        const clickProbability = isOnInterestPoint ? 0.6 : 0.25;
        
        if (Math.random() < clickProbability) {
          // Occasionally double-click instead of single click
          if (Math.random() < settings.mouseAcceleration.doubleClickProbability) {
            await page.mouse.dblclick(currentPosition.x, currentPosition.y);
          } else {
            await page.mouse.click(currentPosition.x, currentPosition.y);
          }
          
          // Natural pause after clicking
          await page.waitForTimeout(300 + Math.random() * 700);
          
          // Sometimes we need to wait longer after a click (page might be loading)
          if (Math.random() < 0.3) {
            await page.waitForTimeout(500 + Math.random() * 1500);
          }
        }
      }
    }

    // Simulate reading behavior if content is present
    if (settings.behavioralPatterns.readingPause) {
      await simulateReading(page, patterns.readingSpeed);
    }

    // Simulate scrolling behavior with the session's scroll style
    await simulateScrolling(page, settings.scrollingStyle);

    // Add advanced interactions if enabled
    if (settings.enableAdvancedInteractions && Math.random() < 0.7) { // Increased probability
      await simulateRandomInteractions(page);
    }

    // Simulate tabbing through elements (common human behavior)
    if (Math.random() < 0.4) { // Increased probability
      const tabCount = Math.floor(Math.random() * 5) + 1;
      for (let i = 0; i < tabCount; i++) {
        await page.keyboard.press("Tab");
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
          const sessionId =
            localStorage.getItem("sessionId") ||
            `user_${Date.now().toString(36)}_${Math.random()
              .toString(36)
              .substr(2, 5)}`;
          localStorage.setItem("sessionId", sessionId);

          // Track page visits to make behavior more realistic
          const pageVisits = parseInt(
            localStorage.getItem("pageVisits") || "0",
            10
          );
          localStorage.setItem("pageVisits", (pageVisits + 1).toString());

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

export {
  simulateHumanBehavior,
  DEFAULT_CONFIG,
  calculateMousePath,
  simulateScrolling,
};
