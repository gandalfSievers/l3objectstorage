.PHONY: build test test-unit test-integration test-integration-noauth test-integration-quick \
        test-integration-stress test-integration-concurrency test-mock test-all clean \
        docker-build docker-build-debian docker-build-alpine docker-push docker-clean docker-test \
        docker-up docker-up-noauth docker-down docker-logs \
        help fmt lint run run-release

# Read version from VERSION file
VERSION := $(shell cat VERSION 2>/dev/null || echo "0.1.0")
IMAGE_NAME := l3objectstorage
REGISTRY_IMAGE := gandalfsievers/l3objectstorage
TEST_CONTAINER := l3objectstorage-test
TEST_PORT := 9999

# Detect host architecture
HOST_ARCH := $(shell uname -m)
ifeq ($(HOST_ARCH),x86_64)
    HOST_DOCKER_ARCH := amd64
else ifeq ($(HOST_ARCH),aarch64)
    HOST_DOCKER_ARCH := arm64
else ifeq ($(HOST_ARCH),arm64)
    HOST_DOCKER_ARCH := arm64
else
    HOST_DOCKER_ARCH := amd64
endif

# Binary name
BINARY := l3-object-storage

# Default target
help:
	@echo "L3ObjectStorage - Available targets:"
	@echo ""
	@echo "  Build:"
	@echo "    make build                 - Build the project (debug)"
	@echo "    make build-release         - Build the project (release)"
	@echo ""
	@echo "  Testing:"
	@echo "    make test                  - Run unit tests (fast, no Docker)"
	@echo "    make test-unit             - Same as 'make test'"
	@echo "    make test-integration      - Run AWS SDK integration tests (auth enabled)"
	@echo "    make test-integration-noauth - Run integration tests (auth disabled)"
	@echo "    make test-integration-quick - Run integration tests (skip stress/concurrency)"
	@echo "    make test-integration-stress - Run stress tests (single-threaded)"
	@echo "    make test-integration-concurrency - Run concurrency tests"
	@echo "    make test-mock             - Run mock/offline tests (no server)"
	@echo "    make test-all              - Run all tests (unit + integration)"
	@echo ""
	@echo "  Docker Build:"
	@echo "    make docker-build          - Build all variants (debian + alpine)"
	@echo "    make docker-build-debian   - Build debian variant"
	@echo "    make docker-build-alpine   - Build alpine variant"
	@echo "    make docker-build-multi    - Build multi-arch images (amd64 + arm64)"
	@echo "    make docker-push           - Build & push multi-arch images to Docker Hub"
	@echo "    make docker-clean          - Remove build artifacts and images"
	@echo "    make docker-test           - Run integration tests with Docker"
	@echo ""
	@echo "  Docker Run:"
	@echo "    make docker-up             - Start Docker container (auth enabled)"
	@echo "    make docker-up-noauth      - Start Docker container (auth disabled)"
	@echo "    make docker-down           - Stop Docker container"
	@echo "    make docker-logs           - View Docker container logs"
	@echo ""
	@echo "  Development:"
	@echo "    make run                   - Run server locally (debug)"
	@echo "    make run-release           - Run server locally (release)"
	@echo "    make fmt                   - Format code"
	@echo "    make lint                  - Run clippy linter"
	@echo "    make clean                 - Clean build artifacts"
	@echo ""
	@echo "  Version: $(VERSION)"
	@echo "  Host arch: $(HOST_ARCH) ($(HOST_DOCKER_ARCH))"
	@echo ""

# =============================================================================
# Build
# =============================================================================

build:
	cargo build

build-release:
	cargo build --release

# =============================================================================
# Docker Build (Multi-stage, builds inside Docker)
# =============================================================================

# Ensure buildx builder exists
docker-buildx-setup:
	@docker buildx inspect l3objectstorage-builder >/dev/null 2>&1 || \
		docker buildx create --name l3objectstorage-builder --use

# Build all variants (debian + alpine) for native architecture
docker-build: docker-build-debian docker-build-alpine
	@echo "Build complete! Images:"
	@docker images $(IMAGE_NAME) --format "table {{.Repository}}:{{.Tag}}\t{{.Size}}"

# Build debian variant (native arch)
docker-build-debian:
	@echo "Building Debian image (version: $(VERSION))..."
	docker build \
		-f docker/Dockerfile \
		-t $(IMAGE_NAME):latest \
		-t $(IMAGE_NAME):debian \
		-t $(IMAGE_NAME):$(VERSION) \
		-t $(IMAGE_NAME):$(VERSION)-debian \
		-t $(REGISTRY_IMAGE):latest \
		-t $(REGISTRY_IMAGE):debian \
		-t $(REGISTRY_IMAGE):$(VERSION) \
		-t $(REGISTRY_IMAGE):$(VERSION)-debian \
		.

# Build alpine variant (native arch)
docker-build-alpine:
	@echo "Building Alpine image (version: $(VERSION))..."
	docker build \
		-f docker/Dockerfile.alpine \
		-t $(IMAGE_NAME):alpine \
		-t $(IMAGE_NAME):$(VERSION)-alpine \
		-t $(REGISTRY_IMAGE):alpine \
		-t $(REGISTRY_IMAGE):$(VERSION)-alpine \
		.

# Build multi-arch images (requires buildx, slower due to emulation)
docker-build-multi: docker-buildx-setup
	@echo "Building multi-arch Debian image..."
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		-f docker/Dockerfile \
		-t $(IMAGE_NAME):latest \
		-t $(IMAGE_NAME):debian \
		-t $(IMAGE_NAME):$(VERSION) \
		-t $(IMAGE_NAME):$(VERSION)-debian \
		-t $(REGISTRY_IMAGE):latest \
		-t $(REGISTRY_IMAGE):debian \
		-t $(REGISTRY_IMAGE):$(VERSION) \
		-t $(REGISTRY_IMAGE):$(VERSION)-debian \
		--load \
		.
	@echo "Building multi-arch Alpine image..."
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		-f docker/Dockerfile.alpine \
		-t $(IMAGE_NAME):alpine \
		-t $(IMAGE_NAME):$(VERSION)-alpine \
		-t $(REGISTRY_IMAGE):alpine \
		-t $(REGISTRY_IMAGE):$(VERSION)-alpine \
		--load \
		.

# Push all images to registry
docker-push: docker-buildx-setup
	@echo "Building and pushing multi-arch images to $(REGISTRY_IMAGE)..."
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		-f docker/Dockerfile \
		-t $(REGISTRY_IMAGE):latest \
		-t $(REGISTRY_IMAGE):debian \
		-t $(REGISTRY_IMAGE):$(VERSION) \
		-t $(REGISTRY_IMAGE):$(VERSION)-debian \
		--push \
		.
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		-f docker/Dockerfile.alpine \
		-t $(REGISTRY_IMAGE):alpine \
		-t $(REGISTRY_IMAGE):$(VERSION)-alpine \
		--push \
		.

# Clean Docker artifacts
docker-clean:
	@echo "Cleaning Docker artifacts..."
	-docker rmi $(IMAGE_NAME):latest $(IMAGE_NAME):debian $(IMAGE_NAME):alpine 2>/dev/null
	-docker rmi $(IMAGE_NAME):$(VERSION) $(IMAGE_NAME):$(VERSION)-debian $(IMAGE_NAME):$(VERSION)-alpine 2>/dev/null
	-docker rmi $(REGISTRY_IMAGE):latest $(REGISTRY_IMAGE):debian $(REGISTRY_IMAGE):alpine 2>/dev/null
	-docker rmi $(REGISTRY_IMAGE):$(VERSION) $(REGISTRY_IMAGE):$(VERSION)-debian $(REGISTRY_IMAGE):$(VERSION)-alpine 2>/dev/null
	-docker buildx rm l3objectstorage-builder 2>/dev/null

# =============================================================================
# Docker Run (for integration testing)
# =============================================================================

docker-up:
	-docker rm -f $(TEST_CONTAINER) 2>/dev/null
	docker run -d --rm --name $(TEST_CONTAINER) \
		-p $(TEST_PORT):9000 \
		-v $$(pwd)/data:/data \
		-e LOCAL_S3_REQUIRE_AUTH=true \
		$(IMAGE_NAME):latest

docker-up-noauth:
	-docker rm -f $(TEST_CONTAINER) 2>/dev/null
	docker run -d --rm --name $(TEST_CONTAINER) \
		-p $(TEST_PORT):9000 \
		-v $$(pwd)/data:/data \
		-e LOCAL_S3_REQUIRE_AUTH=false \
		$(IMAGE_NAME):latest

docker-up-alpine:
	-docker rm -f $(TEST_CONTAINER) 2>/dev/null
	docker run -d --rm --name $(TEST_CONTAINER) \
		-p $(TEST_PORT):9000 \
		-v $$(pwd)/data:/data \
		$(IMAGE_NAME):alpine

docker-down:
	-docker stop $(TEST_CONTAINER) 2>/dev/null

docker-logs:
	docker logs -f $(TEST_CONTAINER)

docker-restart: docker-down docker-up

# =============================================================================
# Testing
# =============================================================================

# Unit tests (fast, no Docker required)
test: test-unit

test-unit:
	cargo test

# Integration tests (requires Docker, auth enabled)
test-integration: docker-build-debian docker-up wait-ready
	@echo "Running AWS SDK integration tests (auth enabled)..."
	cargo test --test aws_sdk_tests -- --ignored --nocapture
	@$(MAKE) docker-down

# Integration tests with auth disabled (for anonymous access tests)
test-integration-noauth: docker-build-debian docker-up-noauth wait-ready
	@echo "Running AWS SDK integration tests (auth disabled)..."
	cargo test --test aws_sdk_tests --features noauth_tests -- --ignored --nocapture
	@$(MAKE) docker-down

# All tests
test-all: test-unit test-integration

# Quick integration tests (skip stress and concurrency)
test-integration-quick: docker-build-debian docker-up wait-ready
	@echo "Running quick integration tests (skipping stress/concurrency)..."
	cargo test --test aws_sdk_tests -- --ignored --skip stress --skip concurrent
	@$(MAKE) docker-down

# Stress tests (run with single thread to avoid resource contention)
test-integration-stress: docker-build-debian docker-up wait-ready
	@echo "Running stress tests (single-threaded)..."
	cargo test --test aws_sdk_tests stress -- --ignored --test-threads=1
	@$(MAKE) docker-down

# Concurrency tests
test-integration-concurrency: docker-build-debian docker-up wait-ready
	@echo "Running concurrency tests..."
	cargo test --test aws_sdk_tests concurrent -- --ignored
	@$(MAKE) docker-down

# Mock/offline tests (no server required)
test-mock:
	@echo "Running mock/offline tests..."
	cargo test --test mock_tests

# Docker test (alias for test-integration)
docker-test: test-integration

# Wait for server to be ready
wait-ready:
	@echo "Waiting for server to be ready on port $(TEST_PORT)..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
		if nc -z localhost $(TEST_PORT) 2>/dev/null; then \
			echo "Server is ready!"; \
			break; \
		fi; \
		echo "Waiting... ($$i/30)"; \
		sleep 1; \
	done
	@if ! nc -z localhost $(TEST_PORT) 2>/dev/null; then \
		echo "Error: Server did not start in time"; \
		$(MAKE) docker-logs || true; \
		$(MAKE) docker-down; \
		exit 1; \
	fi

# =============================================================================
# Development helpers
# =============================================================================

fmt:
	cargo fmt

lint:
	cargo clippy -- -W warnings

clean:
	cargo clean
	rm -rf data/

# Run server locally (without Docker)
run:
	cargo run

run-release:
	cargo run --release
