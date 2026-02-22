# Multi-stage build: Debian (glibc)
# Stage 1: Build
FROM --platform=$BUILDPLATFORM rust:1.83-bookworm AS builder

ARG TARGETPLATFORM
ARG BUILDPLATFORM

# Install cross-compilation tools
RUN apt-get update && apt-get install -y \
    gcc-aarch64-linux-gnu \
    gcc-x86-64-linux-gnu \
    libc6-dev-arm64-cross \
    libc6-dev-amd64-cross \
    && rm -rf /var/lib/apt/lists/*

# Add Rust targets
RUN rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu

# Configure cross-compilation linkers
RUN mkdir -p /.cargo && printf '[target.aarch64-unknown-linux-gnu]\nlinker = "aarch64-linux-gnu-gcc"\n\n[target.x86_64-unknown-linux-gnu]\nlinker = "x86_64-linux-gnu-gcc"\n' > /.cargo/config.toml

WORKDIR /app

# Copy manifests first for better caching
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs

# Determine target based on TARGETPLATFORM
RUN case "$TARGETPLATFORM" in \
        "linux/amd64") echo "x86_64-unknown-linux-gnu" > /tmp/rust_target ;; \
        "linux/arm64") echo "aarch64-unknown-linux-gnu" > /tmp/rust_target ;; \
        *) echo "x86_64-unknown-linux-gnu" > /tmp/rust_target ;; \
    esac

# Pre-build dependencies
RUN cargo build --release --target $(cat /tmp/rust_target) || true

# Copy actual source and force rebuild by touching files
COPY src ./src
RUN touch src/main.rs src/lib.rs

# Build the actual binary (remove previous binary to force rebuild)
RUN rm -f target/$(cat /tmp/rust_target)/release/l3-object-storage && \
    cargo build --release --target $(cat /tmp/rust_target) && \
    cp target/$(cat /tmp/rust_target)/release/l3-object-storage /app/l3-object-storage

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install minimal runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/l3-object-storage /app/l3-object-storage
COPY docker/entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh /app/l3-object-storage

# Create data directory
RUN mkdir -p /data

# Environment defaults
ENV LOCAL_S3_HOST=0.0.0.0
ENV LOCAL_S3_PORT=9000
ENV LOCAL_S3_DATA_DIR=/data
ENV LOCAL_S3_REGION=us-east-1
ENV LOCAL_S3_ACCESS_KEY=localadmin
ENV LOCAL_S3_SECRET_KEY=localadmin

EXPOSE 9000
VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
  CMD test -f /proc/1/exe || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
