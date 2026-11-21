FROM rust:1.93-alpine3.21 AS builder
RUN apk add --no-cache musl-dev
WORKDIR /build
COPY Cargo.toml ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=cascadelake -C target-feature=-avx512f"
RUN cargo build --release

FROM busybox:1.37-musl
USER 1000:1000
COPY --from=builder /build/target/release/shopping-sessions /app/shopping-sessions
WORKDIR /app
CMD ["/app/shopping-sessions"]
