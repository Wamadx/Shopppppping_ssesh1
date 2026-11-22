FROM rust:1.93-bookworm AS builder
WORKDIR /build
COPY Cargo.toml ./
COPY src ./src

ENV CPU_FLAGS="-C target-cpu=cascadelake -C target-feature=-avx512f,+crt-static"
# Static glibc instead of musl: glibc memcmp/memcpy are vectorised, musl's are byte loops.
ENV CARGO_BUILD_TARGET=x86_64-unknown-linux-gnu
RUN RUSTFLAGS="$CPU_FLAGS" cargo build --release --bin shopping-sessions

FROM busybox:1.37-musl
USER 1000:1000
COPY --from=builder /build/target/x86_64-unknown-linux-gnu/release/shopping-sessions /app/shopping-sessions
WORKDIR /app
CMD ["/app/shopping-sessions"]
