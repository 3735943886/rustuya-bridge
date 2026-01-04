# Stage 1: Build
FROM rust:slim AS builder
WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./
RUN mkdir src
COPY src ./src
RUN cargo build --release

# Stage 2: Runtime
FROM debian:trixie-slim
WORKDIR /app

COPY --from=builder /usr/src/app/target/release/rustuya-bridge /app/rustuya-bridge
EXPOSE 37358 37359
ENV ZMQ_COMMAND_ADDR="tcp://0.0.0.0:37358"
ENV ZMQ_EVENT_ADDR="tcp://0.0.0.0:37359"
ENV STATE_FILE="/data/rustuya.json"
RUN mkdir /data

ENTRYPOINT ["/app/rustuya-bridge"]
