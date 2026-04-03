.PHONY: build test test-unit test-integration test-integration-noauth test-integration-quick \
        test-integration-stress test-integration-concurrency test-integration-notifications \
        test-integration-vhost test-integration-awsstyle test-mock test-all clean \
        docker-build docker-build-debian docker-build-alpine docker-push docker-clean docker-test \
        docker-up docker-up-noauth docker-down docker-logs \
        help fmt lint run run-release

# Read version from Cargo.toml (single source of truth)
VERSION := $(shell grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')
IMAGE_NAME := l3objectstorage
REGISTRY_IMAGE := gandalfsievers/l3objectstorage
TEST_PORT := 9999
TEST_COMPOSE := docker compose -f docker/docker-compose.test.yml
TEST_COMPOSE_VHOST := $(TEST_COMPOSE) -f docker/docker-compose.test.vhost.yml
TEST_COMPOSE_AWSSTYLE := $(TEST_COMPOSE) -f docker/docker-compose.test.awsstyle.yml

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
	@echo "    make test-integration-notifications - Run notification trigger tests"
	@echo "    make test-integration-vhost - Run virtual hosted-style tests (in Docker)"
	@echo "    make test-integration-awsstyle - Run AWS-style vhost tests (in Docker)"
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
	$(TEST_COMPOSE) up -d

docker-up-noauth:
	LOCAL_S3_REQUIRE_AUTH=false $(TEST_COMPOSE) up -d

docker-up-alpine:
	S3_IMAGE=$(IMAGE_NAME):alpine $(TEST_COMPOSE) up -d

docker-down:
	-$(TEST_COMPOSE_AWSSTYLE) down --remove-orphans 2>/dev/null
	-$(TEST_COMPOSE_VHOST) down --remove-orphans 2>/dev/null
	-$(TEST_COMPOSE) down --remove-orphans 2>/dev/null

docker-logs:
	$(TEST_COMPOSE) logs -f s3

docker-restart: docker-down docker-up

# =============================================================================
# Testing
# =============================================================================

# Unit tests (fast, no Docker required)
test: test-unit

test-unit:
	cargo test

# Integration tests (requires Docker, auth enabled)
# Virtual hosted-style tests are excluded (need Docker network); run via test-integration-vhost
# Notification trigger tests are excluded (need single-threaded); run via test-integration-notifications
test-integration: docker-build-debian docker-up wait-ready
	@echo "=========================================="
	@echo "  PATH-STYLE integration tests"
	@echo "=========================================="
	@exit_code=0; \
	cargo test --test aws_sdk_tests -- --ignored --nocapture --skip virtual_host --skip virtual_host_aws --skip notification_trigger || exit_code=$$?; \
	echo "Running notification trigger tests (single-threaded)..."; \
	cargo test --test aws_sdk_tests notification_trigger -- --ignored --nocapture --test-threads=1 || exit_code=$$?; \
	$(MAKE) docker-down; \
	exit $$exit_code

# Integration tests with auth disabled (for anonymous access tests)
test-integration-noauth: docker-build-debian docker-up-noauth wait-ready
	@echo "Running AWS SDK integration tests (auth disabled)..."
	cargo test --test aws_sdk_tests --features noauth_tests -- --ignored --nocapture
	@$(MAKE) docker-down

# All tests (unit + path-style integration with notifications + vhost + awsstyle)
test-all:
	@exit_code=0; \
	$(MAKE) test-unit || exit_code=$$?; \
	$(MAKE) test-integration || exit_code=$$?; \
	$(MAKE) test-integration-vhost || exit_code=$$?; \
	$(MAKE) test-integration-awsstyle || exit_code=$$?; \
	exit $$exit_code

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

# Virtual hosted-style tests (runs full suite inside Docker network with vhost addressing)
# Notification trigger tests are excluded (they hardcode localhost for SNS/SQS endpoints)
test-integration-vhost: docker-build-debian docker-up wait-ready
	@echo "=========================================="
	@echo "  VIRTUAL HOSTED-STYLE integration tests"
	@echo "=========================================="
	@exit_code=0; \
	$(TEST_COMPOSE_VHOST) run --rm test-runner \
		cargo test --test aws_sdk_tests -- --ignored --nocapture --skip notification_trigger --skip virtual_host --skip virtual_host_aws || exit_code=$$?; \
	$(MAKE) docker-down; \
	exit $$exit_code

# AWS-style virtual hosted tests (<bucket>.s3.<region>.amazonaws.com, runs inside Docker network)
# Runs the full suite (including notifications) with AWS-style addressing
test-integration-awsstyle: docker-build-debian docker-up wait-ready
	@echo "=========================================="
	@echo "  AWS-STYLE VIRTUAL HOSTED integration tests"
	@echo "=========================================="
	@exit_code=0; \
	$(TEST_COMPOSE_AWSSTYLE) run --rm test-runner \
		cargo test --test aws_sdk_tests -- --ignored --nocapture --skip virtual_host --test-threads=1 || exit_code=$$?; \
	$(MAKE) docker-down; \
	exit $$exit_code

# Notification trigger tests (requires SNS + SQS containers)
test-integration-notifications: docker-build-debian docker-up wait-ready
	@echo "Running notification trigger tests..."
	cargo test --test aws_sdk_tests notification_trigger -- --ignored --nocapture
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
	@echo "Waiting for SNS on port 9911..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do \
		if nc -z localhost 9911 2>/dev/null; then \
			echo "SNS is ready!"; \
			break; \
		fi; \
		echo "Waiting for SNS... ($$i/15)"; \
		sleep 1; \
	done
	@echo "Waiting for SQS on port 9324..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do \
		if nc -z localhost 9324 2>/dev/null; then \
			echo "SQS is ready!"; \
			break; \
		fi; \
		echo "Waiting for SQS... ($$i/15)"; \
		sleep 1; \
	done

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
