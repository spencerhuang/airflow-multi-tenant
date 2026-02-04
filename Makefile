.PHONY: help install test lint format clean docker-up docker-down setup

help:
	@echo "Available commands:"
	@echo "  make install      - Install dependencies"
	@echo "  make test         - Run all tests"
	@echo "  make lint         - Run linters"
	@echo "  make format       - Format code with black and isort"
	@echo "  make clean        - Clean up temporary files"
	@echo "  make docker-up    - Start all Docker services"
	@echo "  make docker-down  - Stop all Docker services"
	@echo "  make setup        - Initial setup (install + env file)"

install:
	pip install -r requirements-dev.txt

test:
	pytest

test-coverage:
	pytest --cov=control_plane --cov=connectors --cov-report=html --cov-report=term

lint:
	flake8 control_plane connectors airflow/plugins
	mypy control_plane connectors

format:
	black control_plane connectors airflow/plugins
	isort control_plane connectors airflow/plugins

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache .coverage htmlcov .mypy_cache
	rm -f test.db

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

setup:
	cp .env.example .env
	pip install -r requirements-dev.txt
	@echo "Setup complete! Edit .env file with your configuration."

# Quick start for local development
dev:
	@echo "Starting local development environment..."
	docker-compose up -d mysql mongodb minio kafka
	@echo "Waiting for services to be ready..."
	sleep 10
	@echo "Services ready! You can now run the control plane locally:"
	@echo "  cd control_plane && uvicorn app.main:app --reload"
