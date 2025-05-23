# Session and Cookie Management System

## Overview

The new robust session and cookie management system provides persistent browser sessions with automatic 30-minute rotation cycles, designed for challenging sites like Ticketmaster that require maintaining session state.

## Key Features

### 🔄 **30-Minute Session Rotation**
- Sessions automatically rotate every 30 minutes
- Seamless transition between sessions
- No downtime during rotation

### 🍪 **Advanced Cookie Management**
- Persistent cookie storage with rotation
- Automatic cookie validation and expiration handling
- Fallback mechanisms for cookie failures

### 📊 **Session Health Monitoring**
- Real-time session validation every 5 minutes
- Usage tracking and failure count monitoring
- Automatic cleanup of invalid sessions

### 🎯 **Event-Specific Sessions**
- Each event can have its own dedicated session
- Dynamic event selection from database
- Intelligent session reuse and allocation

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   ScraperManager │    │  SessionManager  │    │  CookieManager  │
│                 │◄──►│                  │◄──►│                 │
│ - Event scraping│    │ - Session storage│    │ - Cookie storage│
│ - Batch processg│    │ - 30min rotation │    │ - Cookie refresh│
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Database & File Storage                      │
│  - sessions.json    - cookies.json    - Event collection       │
└─────────────────────────────────────────────────────────────────┘
```

## Configuration

### Session Settings (30-minute cycle)
```javascript
SESSION_CONFIG = {
  ROTATION_INTERVAL: 30 * 60 * 1000,        // 30 minutes
  MAX_SESSION_AGE: 2 * 60 * 60 * 1000,      // 2 hours maximum
  MAX_SESSIONS: 10,                          // Max concurrent sessions
  SESSION_WARMUP_TIME: 5000,                // 5 seconds warmup
  SESSION_VALIDATION_INTERVAL: 5 * 60 * 1000 // Validate every 5 minutes
}
```

### Cookie Settings
```javascript
COOKIE_CONFIG = {
  COOKIE_REFRESH_INTERVAL: 24 * 60 * 60 * 1000, // 24 hours
  MAX_COOKIE_LENGTH: 8000,
  ROTATION_INTERVAL: 4 * 60 * 60 * 1000,         // 4 hours
  PERIODIC_REFRESH_INTERVAL: 30 * 60 * 1000      // 30 minutes
}
```

## API Endpoints

### Session Management
- `GET /api/sessions/stats` - Get session statistics
- `GET /api/sessions/active` - List active sessions
- `POST /api/sessions/rotate` - Force session rotation
- `GET /api/sessions/event/:eventId` - Get session for specific event
- `POST /api/sessions/event/:eventId` - Create session for event
- `POST /api/sessions/validate` - Validate all sessions

### Cookie Management
- `GET /api/cookies/stats` - Get cookie refresh statistics
- `GET /api/cookies/recent` - Get recent refresh operations
- `POST /api/cookies/refresh` - Trigger manual cookie refresh

## Usage Examples

### 1. Getting Session for Event
```javascript
// Automatic session creation/retrieval
const sessionData = await sessionManager.getSessionForEvent(eventId, proxy);

if (sessionData) {
  const headers = await sessionManager.getSessionHeaders(eventId);
  // Use headers for scraping
}
```

### 2. Session Rotation Flow
```javascript
// Check if session needs rotation (automatic)
const sessionAge = Date.now() - sessionData.createdAt;
if (sessionAge > SESSION_CONFIG.ROTATION_INTERVAL) {
  // New session automatically created on next request
}
```

### 3. Using Sessions in Scraping
```javascript
// In ScraperManager.scrapeEvent()
try {
  // Try session-based headers first
  const sessionData = await this.sessionManager.getSessionForEvent(eventId, proxy);
  
  if (sessionData) {
    const headers = await this.sessionManager.getSessionHeaders(eventId);
    // Use session headers for scraping
  } else {
    // Fallback to regular cookie management
    const headers = await this.refreshEventHeaders(eventId);
  }
} catch (error) {
  // Session failed, mark usage and fallback
  this.sessionManager.updateSessionUsage(sessionId, false);
}
```

## Session Lifecycle

### 1. **Creation**
- Random event ID selected from database
- Fresh cookies and headers obtained via `refreshHeaders()`
- Browser fingerprint generated
- Session warmed up with test request

### 2. **Usage**
- Headers provided for scraping requests
- Usage statistics tracked
- Failure count monitored
- Last used timestamp updated

### 3. **Validation**
- Health checks every 5 minutes
- Cookie expiration validation
- Session age verification
- Automatic cleanup of invalid sessions

### 4. **Rotation (30 minutes)**
- Old session marked for cleanup
- New session created automatically
- Seamless transition for ongoing scraping
- No interruption to scraping operations

## Benefits for Difficult Sites

### 🛡️ **Anti-Bot Protection**
- Maintains persistent browser sessions
- Realistic session behavior patterns
- Proper cookie and header management
- Dynamic event selection prevents patterns

### 🔄 **Robust Retry Mechanisms**
- Session-level failure tracking
- Automatic session rotation on failures
- Fallback to cookie management
- Intelligent proxy rotation

### 📈 **Performance Optimization**
- Session reuse reduces overhead
- Batch processing with shared sessions
- Efficient header caching
- Minimal database queries

### 🧠 **Smart Management**
- Database-driven event selection
- Automatic session health monitoring
- Proactive session rotation
- Real-time statistics and monitoring

## File Structure
```
├── helpers/
│   ├── SessionManager.js      # Core session management
│   └── CookieManager.js       # Enhanced cookie management
├── controllers/
│   ├── sessionController.js   # Session API endpoints
│   └── cookieRefreshController.js # Cookie API endpoints
├── scraperManager.js          # Integrated scraping with sessions
└── SESSION_MANAGEMENT_GUIDE.md # This guide
```

## Monitoring and Statistics

### Session Stats
```javascript
{
  totalSessions: 8,
  activeSessions: 6,
  validSessions: 6,
  averageAge: 15,        // minutes
  oldestSession: 28      // minutes
}
```

### Cookie Stats
```javascript
{
  sessions: { /* session stats */ },
  cookies: {
    cachedHeaders: 12,
    lastCookieReset: "2 hours ago",
    capturedState: {
      hasCookies: true,
      cookieCount: 45,
      lastRefresh: "15 minutes ago"
    }
  }
}
```

## Troubleshooting

### Common Issues

1. **Sessions Not Rotating**
   - Check `SESSION_ROTATION_INTERVAL` setting
   - Verify session validation is running
   - Check logs for session creation errors

2. **High Session Failure Rate**
   - Monitor proxy health
   - Check cookie expiration
   - Verify database connectivity for event selection

3. **Memory Usage High**
   - Adjust `MAX_SESSIONS` setting
   - Check session cleanup frequency
   - Monitor `sessions.json` file size

### Debug Commands
```bash
# Check session status
curl http://localhost:3000/api/sessions/stats

# Force session rotation
curl -X POST http://localhost:3000/api/sessions/rotate

# Validate all sessions
curl -X POST http://localhost:3000/api/sessions/validate
```

## Best Practices

1. **Monitor session health regularly**
2. **Use appropriate rotation intervals for your use case**
3. **Implement proper error handling for session failures**
4. **Keep session and cookie files backed up**
5. **Monitor proxy health alongside session health**
6. **Use batch processing to maximize session efficiency**

---

This robust session and cookie management system provides the foundation for successfully scraping challenging sites that require persistent session state and sophisticated anti-bot protection. 