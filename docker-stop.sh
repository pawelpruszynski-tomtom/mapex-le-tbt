#!/bin/bash

# TbT Inspection Pipeline - Docker Stop Script

set -e

echo "🛑 Stopping TbT Inspection Pipeline Docker Environment"
echo "=================================================="

# Stop services
echo "Stopping services..."
docker-compose down

echo ""
echo "✅ Services stopped successfully!"
echo ""
echo "💡 To remove volumes (data will be lost):"
echo "   docker-compose down -v"
echo ""

