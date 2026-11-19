# Etapa 1: Build
FROM rust:bookworm AS builder

RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    pkg-config \
    libssl-dev \
    libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src

RUN cargo build --release

# Etapa 2: Runtime endurecido con usuario sin privilegios (non-root)
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libsasl2-2 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r swiftroute && useradd -r -g swiftroute -s /sbin/nologin swiftroute

WORKDIR /app
COPY --from=builder /app/target/release/swiftroute-backend /app/swiftroute-backend
RUN chown -R swiftroute:swiftroute /app

USER swiftroute
EXPOSE 8080
CMD ["/app/swiftroute-backend"]
