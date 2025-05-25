/**
 * A utility function similar to Promise.allSettled but with better error handling
 * Returns an array of objects with status and value/reason properties
 * 
 * @param {Promise[]} promises - Array of promises to settle
 * @returns {Promise<Array>} - Array of settled results
 */
export async function promiseAllSettled(promises) {
  return Promise.all(
    promises.map(promise => 
      promise
        .then(value => ({ status: 'fulfilled', value }))
        .catch(reason => ({ status: 'rejected', reason }))
    )
  );
}

/**
 * Execute promises with limited concurrency
 * @param {Array<Function>} functions - Array of functions that return promises
 * @param {number} concurrency - Maximum number of promises to execute at once
 * @param {Object} options - Additional options
 * @param {number} options.timeout - Maximum time to wait for each promise (ms)
 * @param {Array<number>} options.priorityIndices - Indices of high-priority functions to execute immediately
 * @returns {Promise<Array>} - Results of all promises
 */
export async function promiseWithConcurrency(functions, concurrency = 10, options = {}) {
  const results = [];
  const executing = new Set();
  const timeout = options.timeout || 60000; // Default 60-second timeout per promise
  const priorityIndices = new Set(options.priorityIndices || []);
  
  // Function to create a promise with timeout
  const createPromiseWithTimeout = (func, index) => {
    const promise = func().then(res => {
      results[index] = res;
      return res;
    });
    
    // Add timeout to prevent any single promise from blocking too long
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => {
        reject(new Error(`Promise ${index} timed out after ${timeout}ms`));
      }, timeout);
    });
    
    // Race between the actual promise and the timeout
    return Promise.race([promise, timeoutPromise]);
  };
  
  // Process high-priority functions first, bypassing concurrency limits
  if (priorityIndices.size > 0) {
    const priorityPromises = [];
    for (const index of priorityIndices) {
      if (index >= 0 && index < functions.length) {
        const priorityPromise = createPromiseWithTimeout(functions[index], index);
        priorityPromises.push(priorityPromise);
        
        // Mark this function as processed by setting it to null
        functions[index] = null;
      }
    }
    
    // Wait for all priority promises to complete
    await Promise.allSettled(priorityPromises);
  }
  
  // Process remaining functions with concurrency limit
  for (const [index, func] of functions.entries()) {
    // Skip null functions (already processed as priority)
    if (func === null) continue;
    
    const promise = createPromiseWithTimeout(func, index);
    executing.add(promise);
    
    const clean = () => {
      executing.delete(promise);
    };
    
    promise.then(clean).catch(clean);
    
    // If we reach the concurrency limit, wait for one promise to finish
    if (executing.size >= concurrency) {
      await Promise.race(executing);
    }
  }
  
  // Wait for all promises to complete
  return Promise.all(results);
}
