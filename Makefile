# Makefile for TbT Inspection Pipeline

.PHONY: help build up down restart logs logs-api logs-worker logs-postgres logs-redis clean test health stats

# Default target
.DEFAULT_GOAL := help

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)TbT Inspection Pipeline - Make Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

# Setup
setup: ## Initial setup (copy .env.example to .env)
	@if [ ! -f .env ]; then \
		echo "$(YELLOW)Creating .env from .env.example...$(NC)"; \
		cp .env.example .env; \
		echo "$(GREEN)✓ .env created. Please edit it with your credentials.$(NC)"; \
	else \
		echo "$(YELLOW).env already exists.$(NC)"; \
	fi

# Docker commands
build: ## Build Docker images
	@echo "$(BLUE)Building Docker images...$(NC)"
	docker-compose build

up: setup ## Start all services
	@echo "$(BLUE)Starting services...$(NC)"
	docker-compose up -d
	@echo "$(GREEN)✓ Services started$(NC)"
	@echo "$(BLUE)API:$(NC)          http://localhost:8000"
	@echo "$(BLUE)API Docs:$(NC)     http://localhost:8000/docs"
	@echo "$(BLUE)RQ Dashboard:$(NC) http://localhost:9181"

down: ## Stop all services
	@echo "$(BLUE)Stopping services...$(NC)"
	docker-compose down
	@echo "$(GREEN)✓ Services stopped$(NC)"

restart: ## Restart all services
	@echo "$(BLUE)Restarting services...$(NC)"
	docker-compose restart
	@echo "$(GREEN)✓ Services restarted$(NC)"

# Service-specific restarts
restart-api: ## Restart API service only
	@echo "$(BLUE)Restarting API...$(NC)"
	docker-compose restart tbt-api

restart-worker: ## Restart worker service only
	@echo "$(BLUE)Restarting workers...$(NC)"
	docker-compose restart tbt-worker

# Logs
logs: ## Show logs from all services
	docker-compose logs -f

logs-api: ## Show logs from API service
	docker-compose logs -f tbt-api

logs-worker: ## Show logs from worker service
	docker-compose logs -f tbt-worker

logs-postgres: ## Show logs from PostgreSQL service
	docker-compose logs -f postgres

logs-redis: ## Show logs from Redis service
	docker-compose logs -f redis

# Status
ps: ## Show running containers
	docker-compose ps

health: ## Check API health
	@curl -s http://localhost:8000/health | python -m json.tool || echo "$(RED)API not responding$(NC)"

stats: ## Show queue statistics
	@curl -s http://localhost:8000/api/v1/inspection/queue/stats | python -m json.tool || echo "$(RED)API not responding$(NC)"

# Database
db-connect: ## Connect to PostgreSQL database
	docker-compose exec postgres psql -U $${DB_USER:-tbt_user} -d $${DB_NAME:-mapex_tbt}

db-backup: ## Backup PostgreSQL database
	@echo "$(BLUE)Creating database backup...$(NC)"
	docker-compose exec postgres pg_dump -U $${DB_USER:-tbt_user} $${DB_NAME:-mapex_tbt} > backup_$$(date +%Y%m%d_%H%M%S).sql
	@echo "$(GREEN)✓ Backup created$(NC)"

# Redis
redis-cli: ## Connect to Redis CLI
	docker-compose exec redis redis-cli

redis-flush: ## Flush all Redis data (⚠️ destructive!)
	@echo "$(RED)This will delete all data from Redis!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose exec redis redis-cli FLUSHALL; \
		echo "$(GREEN)✓ Redis flushed$(NC)"; \
	fi

# Scaling
scale-workers: ## Scale workers (usage: make scale-workers N=4)
	@if [ -z "$(N)" ]; then \
		echo "$(RED)Error: Please specify number of workers with N=<number>$(NC)"; \
		echo "Example: make scale-workers N=4"; \
		exit 1; \
	fi
	@echo "$(BLUE)Scaling workers to $(N)...$(NC)"
	docker-compose up -d --scale tbt-worker=$(N)
	@echo "$(GREEN)✓ Workers scaled to $(N)$(NC)"

# Testing
test-connection: ## Test database connection
	docker-compose exec tbt-api python scripts/test_database_connection.py

test-api: ## Test API with example file (requires GeoJSON file)
	@if [ ! -f "li_input/geojson/Routes2check.geojson" ]; then \
		echo "$(RED)Error: GeoJSON file not found at li_input/geojson/Routes2check.geojson$(NC)"; \
		exit 1; \
	fi
	python scripts/test_api.py li_input/geojson/Routes2check.geojson

# Cleanup
clean: ## Remove all containers, volumes, and images
	@echo "$(RED)This will remove all containers, volumes, and images!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v; \
		docker system prune -af; \
		echo "$(GREEN)✓ Cleanup complete$(NC)"; \
	fi

clean-data: ## Remove data directories (⚠️ destructive!)
	@echo "$(RED)This will delete all data in data/ and output/ directories!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		rm -rf data/tbt/inspection/* data/tbt/sampling/* output/*; \
		echo "$(GREEN)✓ Data directories cleaned$(NC)"; \
	fi

# Development
dev-install: ## Install development dependencies locally
	cd src && pip install -e . && pip install -r requirements.txt

dev-shell: ## Open shell in API container
	docker-compose exec tbt-api /bin/bash

# Documentation
docs: ## Open API documentation in browser
	@echo "$(BLUE)Opening API documentation...$(NC)"
	@command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8000/docs || \
	command -v open >/dev/null 2>&1 && open http://localhost:8000/docs || \
	echo "$(YELLOW)Please open http://localhost:8000/docs in your browser$(NC)"

dashboard: ## Open RQ Dashboard in browser
	@echo "$(BLUE)Opening RQ Dashboard...$(NC)"
	@command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:9181 || \
	command -v open >/dev/null 2>&1 && open http://localhost:9181 || \
	echo "$(YELLOW)Please open http://localhost:9181 in your browser$(NC)"

# All-in-one commands
fresh-start: clean setup build up ## Clean everything and start fresh
	@echo "$(GREEN)✓ Fresh start complete!$(NC)"

quick-start: setup up ## Quick start (setup + up)
	@echo "$(GREEN)✓ Quick start complete!$(NC)"
	@sleep 3
	@make health

