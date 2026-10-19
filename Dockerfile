FROM python:3.12-slim

# Prevent interactive debian prompts and configure cloud runtime env
ENV DEBIAN_FRONTEND=noninteractive \
    CLOUD_ENV=true \
    PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium \
    PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 1. Install system dependencies: curl, build tools, ffmpeg, chromium, CA certificates
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    gcc \
    g++ \
    libpq-dev \
    ffmpeg \
    chromium \
    fonts-liberation \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 2. Install Node.js (v20 LTS)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# 3. Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Install Node.js dependencies for whatsapp_bridge
COPY whatsapp_bridge/package*.json ./whatsapp_bridge/
RUN cd whatsapp_bridge && npm install --omit=dev

# 5. Copy application source code
COPY . .

# Expose FastAPI application port
EXPOSE 8000

# Run FastAPI main entrypoint with unbuffered stdout
CMD ["python", "-u", "main.py"]
