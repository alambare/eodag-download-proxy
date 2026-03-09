FROM rust:1.86-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    clang \
    musl-tools \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release --target x86_64-unknown-linux-musl --bin eodag-data-proxy
RUN rm -rf src

COPY src ./src
RUN touch src/main.rs && cargo build --release --target x86_64-unknown-linux-musl --bin eodag-data-proxy

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=builder /build/target/x86_64-unknown-linux-musl/release/eodag-data-proxy /eodag-data-proxy
COPY config.toml /config.toml

EXPOSE 8080

ENTRYPOINT ["/eodag-data-proxy"]
