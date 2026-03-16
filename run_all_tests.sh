#!/bin/bash

# Script to run all tests including integration tests with Docker services
# Usage: ./run_all_tests.sh

set -e

echo "=========================================="
echo "Multi-Tenant Airflow Test Suite"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo -e "${RED}✗ Docker is not running${NC}"
        echo "Please start Docker Desktop or OrbStack and try again"
        exit 1
    fi
    echo -e "${GREEN}✓ Docker is running${NC}"
}

# Function to check if services are up
check_services() {
    echo ""
    echo "Checking Docker services..."

    # Check if containers are running
    if ! docker-compose ps | grep -q "Up"; then
        echo -e "${YELLOW}⚠ Docker services are not running${NC}"
        echo "Starting Docker services..."
        docker-compose up -d
        echo "Waiting for services to be ready (60 seconds)..."
        sleep 60
    else
        echo -e "${GREEN}✓ Docker services are running${NC}"
    fi
}

# Function to wait for specific service
wait_for_service() {
    local service_name=$1
    local check_command=$2
    local max_retries=30
    local retry_count=0

    echo "Waiting for $service_name..."

    while [ $retry_count -lt $max_retries ]; do
        if eval $check_command > /dev/null 2>&1; then
            echo -e "${GREEN}✓ $service_name is ready${NC}"
            return 0
        fi
        retry_count=$((retry_count + 1))
        echo -n "."
        sleep 2
    done

    echo -e "${RED}✗ $service_name failed to start${NC}"
    return 1
}

# Function to check dependencies
check_dependencies() {
    echo ""
    echo "Checking Python dependencies..."

    if ! python -c "import pytest" 2>/dev/null; then
        echo -e "${YELLOW}⚠ Installing test dependencies...${NC}"
        echo "This will install lightweight test dependencies (no Airflow)..."
        uv pip install -r requirements-test.txt -q
        echo -e "${GREEN}✓ Dependencies installed${NC}"
    else
        echo -e "${GREEN}✓ Dependencies already installed${NC}"
    fi
}

# Function to run unit tests
run_unit_tests() {
    echo ""
    echo "=========================================="
    echo "Running Unit Tests"
    echo "=========================================="

    source venv/bin/activate

    pytest connectors/tests/test_s3_client.py \
           connectors/tests/test_s3_reader.py \
           connectors/tests/test_mongo_client.py \
           connectors/tests/test_azure_blob_client.py \
           control_plane/tests/test_integration_service.py \
           -v --cov=connectors --cov=control_plane/app/services \
           --cov-report=term-missing \
           --cov-report=html:htmlcov/unit

    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✓ Unit tests passed${NC}"
    else
        echo -e "${RED}✗ Unit tests failed${NC}"
        return $exit_code
    fi
}

# Function to run integration tests
run_integration_tests() {
    echo ""
    echo "=========================================="
    echo "Running Integration Tests"
    echo "=========================================="

    source venv/bin/activate

    # Wait for services
    wait_for_service "MySQL" "docker exec control-plane-mysql mysqladmin ping -h localhost -proot"
    wait_for_service "Control Plane API" "curl -s http://localhost:8000/api/v1/health"

    pytest control_plane/tests/test_integration_api_live.py -v -s

    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✓ Integration tests passed${NC}"
    else
        echo -e "${RED}✗ Integration tests failed${NC}"
        return $exit_code
    fi
}

# Function to run CDC Kafka tests
run_cdc_kafka_tests() {
    echo ""
    echo "=========================================="
    echo "Running CDC Kafka Tests"
    echo "=========================================="

    source venv/bin/activate

    # Wait for Kafka
    wait_for_service "Kafka" "docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list"

    pytest control_plane/tests/test_cdc_kafka.py -v -s

    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✓ CDC Kafka tests passed${NC}"
    else
        echo -e "${RED}✗ CDC Kafka tests failed${NC}"
        return $exit_code
    fi
}

# Function to generate coverage report
generate_coverage_report() {
    echo ""
    echo "=========================================="
    echo "Generating Coverage Report"
    echo "=========================================="

    source venv/bin/activate

    # Run all tests with coverage
    pytest connectors/tests/ \
           control_plane/tests/test_integration_service.py \
           control_plane/tests/test_integration_api_live.py \
           -v \
           --cov=connectors \
           --cov=control_plane/app/services \
           --cov=control_plane/app/api \
           --cov-report=term-missing \
           --cov-report=html:htmlcov/all \
           --ignore=airflow/tests

    echo ""
    echo -e "${GREEN}Coverage report generated in htmlcov/all/index.html${NC}"
}

# Main execution
main() {
    echo "Step 1: Checking Docker..."
    check_docker

    echo ""
    echo "Step 2: Checking Docker services..."
    check_services

    echo ""
    echo "Step 3: Checking dependencies..."
    check_dependencies

    echo ""
    echo "Step 4: Running test suites..."

    # Run unit tests
    if ! run_unit_tests; then
        echo -e "${RED}Unit tests failed. Stopping.${NC}"
        exit 1
    fi

    # Run integration tests
    if ! run_integration_tests; then
        echo -e "${YELLOW}⚠ Integration tests failed but continuing...${NC}"
    fi

    # Run CDC Kafka tests
    if ! run_cdc_kafka_tests; then
        echo -e "${YELLOW}⚠ CDC Kafka tests failed but continuing...${NC}"
    fi

    # Generate final coverage report
    generate_coverage_report

    echo ""
    echo "=========================================="
    echo -e "${GREEN}✓ All tests completed!${NC}"
    echo "=========================================="
    echo ""
    echo "Summary:"
    echo "  - Unit tests: ✓"
    echo "  - Integration tests: Check output above"
    echo "  - CDC Kafka tests: Check output above"
    echo "  - Coverage report: htmlcov/all/index.html"
    echo ""
}

# Run main function
main
