#!/bin/bash
set -e

echo "Building and starting container..."
docker-compose up -d --build

echo "Waiting for server to be ready..."
for i in {1..30}; do
    if nc -z localhost 9000 2>/dev/null; then
        echo "Server is ready!"
        break
    fi
    echo "Waiting... ($i/30)"
    sleep 1
done

if ! nc -z localhost 9000 2>/dev/null; then
    echo "Error: Server did not start in time"
    docker-compose logs
    docker-compose down
    exit 1
fi

echo "Running AWS SDK integration tests..."
cargo test --test aws_sdk_tests -- --ignored --nocapture

echo "Stopping container..."
docker-compose down

echo "Done!"
