.PHONY: help setup up down logs health test test-integration bench seed lint clean

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup: ## Create .env from the template
	@test -f .env || cp .env.example .env
	@echo ".env ready"

up: setup ## Build and start the full stack
	docker compose up -d --build
	@echo "API: http://localhost:8000/docs"

down: ## Stop the stack and remove volumes
	docker compose down -v

logs: ## Tail API and worker logs
	docker compose logs -f api worker

health: ## Show dependency health
	@curl -s http://localhost:8000/health | python3 -m json.tool

test: ## Run the hermetic test suite (no docker required)
	pytest -q

test-integration: ## Run end-to-end tests against the running stack
	pytest -m integration -v

bench: ## Seed 5000 docs and benchmark search
	python3 scripts/bench.py --seed 5000 --requests 3000 --concurrency 50

seed: ## Index a small demo corpus
	python3 scripts/seed_demo.py

lint: ## Static checks
	ruff check app tests

clean: ## Remove caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache .ruff_cache
