FROM rust:1.82-bookworm AS builder

WORKDIR /workspace

# Copy manifests first to maximize Docker layer cache usage.
COPY Cargo.toml Cargo.lock ./
COPY logger/Cargo.toml logger/Cargo.toml
COPY tcp_source/Cargo.toml tcp_source/Cargo.toml
COPY tcp_sink/Cargo.toml tcp_sink/Cargo.toml

# Add sources and build both binaries.
COPY logger/src logger/src
COPY tcp_source/src tcp_source/src
COPY tcp_sink/src tcp_sink/src

RUN cargo build --release --package tcp_source --package tcp_sink

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    bash \
    && rm -rf /var/lib/apt/lists/*

ENV TCP_HOME=/opt/tcp
ENV PATH="$TCP_HOME/bin:$PATH"

WORKDIR $TCP_HOME

COPY --from=builder /workspace/target/release/tcp_source $TCP_HOME/bin/tcp_source
COPY --from=builder /workspace/target/release/tcp_sink $TCP_HOME/bin/tcp_sink
COPY docker-entrypoint.sh $TCP_HOME/bin/docker-entrypoint.sh

RUN chmod +x $TCP_HOME/bin/tcp_source \
    $TCP_HOME/bin/tcp_sink \
    $TCP_HOME/bin/docker-entrypoint.sh \
    && mkdir -p /data $TCP_HOME/logs

# Persist logs across container restarts/removals.
VOLUME ["/opt/tcp/logs"]

# Common default ports used by both source and sink binaries.
EXPOSE 9000

# Usage:
#   docker run ... <image> source /data/input.parquet [source args...]
#   docker run ... <image> sink [sink args...]
ENTRYPOINT ["docker-entrypoint.sh"]
