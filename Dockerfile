FROM --platform=$BUILDPLATFORM rust:slim AS builder

# 1. Install Docker cross-compilation helpers (xx)
COPY --from=tonistiigi/xx:master / /

WORKDIR /usr/src/app

# 2. Install C compiler and libraries for the target platform ($TARGETPLATFORM)
ARG TARGETPLATFORM
RUN apt-get update && \
    apt-get install -y clang lld && \
    xx-apt-get install -y gcc g++ libc6-dev

COPY Cargo.toml Cargo.lock ./
RUN mkdir src
COPY src ./src

# 3. Perform cross-compilation
RUN --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/app/target \
    xx-cargo build --release --target-dir ./target && \
    # Move the binary to a common location (/bin) as the build path varies by architecture
    cp ./target/$(xx-cargo --print-target-triple)/release/rustuya-bridge /bin/rustuya-bridge

# [Stage 2: Runtime]
# The runtime image matches the target architecture (implicit default).
FROM debian:trixie-slim
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /bin/rustuya-bridge /app/rustuya-bridge

EXPOSE 37358 37359
ENV ZMQ_COMMAND_ADDR="tcp://0.0.0.0:37358"
ENV ZMQ_EVENT_ADDR="tcp://0.0.0.0:37359"
ENV STATE_FILE="/data/rustuya.json"
RUN mkdir /data

ENTRYPOINT ["/app/rustuya-bridge"]