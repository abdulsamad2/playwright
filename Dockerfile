# Use Node.js 18 LTS as base image
FROM node:18-slim

# Install system dependencies for Playwright
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install Node.js dependencies
RUN npm ci

# Install Playwright browsers and system dependencies
RUN npx playwright install --with-deps

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/data /app/output /app/db_export

# Set permissions
RUN chmod +x /app/scripts/start-instances.js

# Create non-root user for security
RUN groupadd -r scraper && useradd -r -g scraper scraper
RUN chown -R scraper:scraper /app
USER scraper

# Expose port (will be overridden by environment variable)
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:${PORT:-3000}/api/health || exit 1

# Default command
CMD ["node", "app.js", "--start-scraper"]