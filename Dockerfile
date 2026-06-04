# Pinned to bookworm so the produced glibc-dynamic binary targets glibc 2.36,
# matching the distroless/cc-debian12 runtime below (avoids GLIBC_2.xx-not-found
# at runtime if the base ever floats to a newer debian than the runtime).
FROM --platform=$BUILDPLATFORM rust:slim-bookworm AS builder

# 1. Install Docker cross-compilation helpers (xx)
COPY --from=tonistiigi/xx:master / /

WORKDIR /usr/src/app

# 2. Install C compiler and libraries for the target platform ($TARGETPLATFORM)
ARG TARGETPLATFORM
RUN apt-get update && \
    apt-get install -y clang lld && \
    xx-apt-get install -y gcc g++ libc6-dev

COPY Cargo.toml ./
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
# distroless/cc: glibc + libgcc + ca-certificates, no shell or package manager.
# ca-certificates is required for `mqtts://` — rustls-native-certs reads the OS
# trust store, which the previous debian-slim image never installed (so TLS
# brokers silently failed there). Runtime arch matches the target implicitly.
FROM gcr.io/distroless/cc-debian12
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /bin/rustuya-bridge /app/rustuya-bridge

# NOTE: do NOT default MQTT_BROKER here. CLI/env take precedence over the
# config file (merge only fills None fields), so a baked-in MQTT_BROKER would
# silently override `mqtt_broker` set in /data/config.json — connecting to the
# wrong broker for any user who configures it via the file. Leave it unset:
# pass `-e MQTT_BROKER=...` or set it in config.json. STATE_FILE/CONFIG stay —
# they pin where persistent data lives inside the container's /data volume.
ENV STATE_FILE="/data/rustuya.json"
ENV CONFIG="/data/config.json"
# No `RUN mkdir /data` — distroless has no shell. The bridge creates the data
# dir itself on first run (apply_config_file / save_state mkdir -p the parent),
# and a mounted volume provides it otherwise.

ENTRYPOINT ["/app/rustuya-bridge"]
