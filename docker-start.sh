#!/bin/bash

# TbT Inspection Pipeline - Docker Start Script
# This script starts application services (API + Workers + Dashboard)
# Note: Requires existing Redis and PostgreSQL instances

set -e

echo "🚀 Starting TbT Inspection Pipeline Docker Environment"
echo "=================================================="

# Check if .env file exists
if [ ! -f .env ]; then
    echo "⚠️  .env file not found!"
    echo "📋 Copying .env.example to .env..."
    cp .env.example .env
    echo "✅ Please edit .env file with your Redis and PostgreSQL connection details."
    echo "   Required: REDIS_HOST, REDIS_PORT, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD"
    echo "   Then run this script again."
    exit 1
fi

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Load environment variables
source .env

# Check if required variables are set
echo ""
echo "🔍 Checking configuration..."
if [ -z "$REDIS_HOST" ] || [ -z "$DB_HOST" ]; then
    echo "❌ Required environment variables not set!"
    echo "   Please configure in .env file:"
    echo "   - REDIS_HOST (Redis hostname/IP)"
    echo "   - REDIS_PORT (Redis port, default: 6379)"
    echo "   - DB_HOST (PostgreSQL hostname/IP)"
    echo "   - DB_PORT (PostgreSQL port, default: 5432)"
    echo "   - DB_USER, DB_PASSWORD"
    exit 1
fi

echo "   Redis: ${REDIS_HOST}:${REDIS_PORT:-6379}"
echo "   PostgreSQL: ${DB_HOST}:${DB_PORT:-5432}"

# Test Redis connection
echo ""
echo "🔌 Testing Redis connection..."
if command -v redis-cli &> /dev/null; then
    if redis-cli -h ${REDIS_HOST} -p ${REDIS_PORT:-6379} ping &> /dev/null; then
        echo "   ✅ Redis is accessible"
    else
        echo "   ⚠️  Cannot connect to Redis at ${REDIS_HOST}:${REDIS_PORT:-6379}"
        echo "   Make sure Redis is running and accessible"
    fi
else
    echo "   ⚠️  redis-cli not found - skipping Redis connection test"
    echo "   Make sure Redis is running at ${REDIS_HOST}:${REDIS_PORT:-6379}"
fi

# Test PostgreSQL connection
echo ""
echo "🔌 Testing PostgreSQL connection..."
if command -v pg_isready &> /dev/null; then
    if pg_isready -h ${DB_HOST} -p ${DB_PORT:-5432} &> /dev/null; then
        echo "   ✅ PostgreSQL is accessible"
    else
        echo "   ⚠️  Cannot connect to PostgreSQL at ${DB_HOST}:${DB_PORT:-5432}"
        echo "   Make sure PostgreSQL is running and accessible"
    fi
else
    echo "   ⚠️  pg_isready not found - skipping PostgreSQL connection test"
    echo "   Make sure PostgreSQL is running at ${DB_HOST}:${DB_PORT:-5432}"
fi

# Build and start services
echo ""
echo "🔨 Building Docker images..."
docker-compose build

echo ""
echo "🚀 Starting services..."
docker-compose up -d

echo ""
echo "⏳ Waiting for services to be healthy..."
sleep 5

# Check service health
echo ""
echo "🔍 Checking service status..."
docker-compose ps

echo ""
echo "✅ Application services started successfully!"
echo ""
echo "📡 Available endpoints:"
echo "   - API:            http://localhost:8000"
echo "   - API Docs:       http://localhost:8000/docs"
echo "   - RQ Dashboard:   http://localhost:9181"
echo ""
echo "🔗 External services (must be running):"
echo "   - Redis:          ${REDIS_HOST}:${REDIS_PORT:-6379}"
echo "   - PostgreSQL:     ${DB_HOST}:${DB_PORT:-5432}"
echo ""
echo "📊 View logs:"
echo "   docker-compose logs -f tbt-api"
echo "   docker-compose logs -f tbt-worker"
echo ""
echo "🛑 Stop services:"
echo "   docker-compose down"
echo ""
echo "🔄 Restart services:"
echo "   docker-compose restart"
echo ""

