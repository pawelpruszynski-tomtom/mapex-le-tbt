# Stage 1: Base image with system dependencies
FROM python:3.9-slim as base

# Install Java (required for PySpark) and other system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    ca-certificates \
    curl \
    postgresql-client && \
    # Add Adoptium repository for Java 11
    mkdir -p /etc/apt/keyrings && \
    wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | tee /etc/apt/keyrings/adoptium.asc && \
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends temurin-11-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jre-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory
WORKDIR /app

# Stage 2: Dependencies
FROM base as dependencies

# Copy requirements first for better caching
COPY src/requirements.txt /app/requirements.txt

# Upgrade pip and install critical dependencies first to avoid conflicts
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --upgrade typing-extensions>=4.6.0 && \
    pip install --no-cache-dir "marshmallow>=3.0.0,<4.0.0" "apispec>=3.3.1,<7.0.0"

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install additional dependencies for API and Redis Queue
RUN pip install --no-cache-dir \
    fastapi==0.104.1 \
    uvicorn[standard]==0.24.0 \
    python-multipart==0.0.6 \
    redis==4.6.0 \
    rq==1.15.1 \
    environs==9.5.0 \
    fcd_py==5.0.854 --extra-index-url https://svc-ar-maps-analytics-editor:AP5GYCYPFsETQzbsgnE8a6cjhNEcSvTaNTUvzNmkDHQTRt9GhcqKa3zAe9j2@artifactory.tomtomgroup.com/artifactory/api/pypi/maps-fcd-pypi-release/simple

# Stage 3: Application
FROM dependencies as application

# Copy the entire project
COPY . /app/

# Install the project package
RUN cd /app/src && pip install -e .

# Create necessary directories
RUN mkdir -p /app/data/tbt/inspection \
    /app/data/tbt/sampling \
    /app/output \
    /app/li_input/geojson \
    /app/logs \
    /app/uploads

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV KEDRO_LOGGING_CONFIG=/app/conf/base/logging.yml

# Expose port for API
EXPOSE 8000

# Default command (can be overridden)
CMD ["python", "-m", "tbt_api.main"]

