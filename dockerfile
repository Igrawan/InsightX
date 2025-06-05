# Step 1: Build Stage
FROM node:20 AS builder

# Set working directory
WORKDIR /app

# Copy only package files first (faster Docker cache)
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of your code
COPY . .

# Build TypeScript
RUN npm run build

# Step 2: Production Stage
FROM node:20-slim

# Set working directory
WORKDIR /app

# Copy package.json and install only production deps (optional if you have devDeps separated)
COPY package*.json ./
RUN npm install --only=production

# Copy compiled code from builder stage
COPY --from=builder /app/dist ./dist

# Expose the port your app runs on
EXPOSE 3000

# Start the app
CMD ["node", "dist/index.js"]
