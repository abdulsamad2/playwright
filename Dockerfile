FROM mcr.microsoft.com/playwright:v1.49.1-jammy

WORKDIR /app

# Copy package files and install dependencies
COPY package*.json ./
RUN npm install
RUN npx playwright install

# Copy application code
COPY . .

# Set environment variables
ENV NODE_ENV=production

# Start the application based on SERVICE_TYPE
CMD ["node", "index.js"]