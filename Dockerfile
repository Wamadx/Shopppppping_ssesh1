FROM rust:1.93-bookworm AS builder
RUN rustup component add llvm-tools
WORKDIR /build
COPY Cargo.toml ./
COPY src ./src

# PGO is mandatory: any failure aborts the build instead of silently shipping a non-PGO binary.
# Profile generation and use share the same target flags so the profile matches the final code.
ENV CPU_FLAGS="-C target-cpu=cascadelake -C target-feature=-avx512f,+crt-static"
# Static glibc instead of musl: glibc memcmp/memcpy are vectorised, musl's are byte loops.
ENV CARGO_BUILD_TARGET=x86_64-unknown-linux-gnu
RUN set -eux; \
    PROFDATA="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/^host: //p')/bin/llvm-profdata"; \
    RUSTFLAGS="" CARGO_PROFILE_RELEASE_LTO=off CARGO_PROFILE_RELEASE_CODEGEN_UNITS=8 \
        cargo build --release --bin synth --target-dir /tmp/synth-target; \
    /tmp/synth-target/x86_64-unknown-linux-gnu/release/synth /tmp/train; \
    mkdir -p /pgo/raw /tmp/o1 /tmp/o2; \
    RUSTFLAGS="--cfg pgo_gen $CPU_FLAGS -Cprofile-generate=/pgo/raw" \
        cargo build --release --bin shopping-sessions; \
    LLVM_PROFILE_FILE="/pgo/raw/r_%p_%m.profraw" NCPU=8 /build/target/x86_64-unknown-linux-gnu/release/shopping-sessions /tmp/train /tmp/o1; \
    LLVM_PROFILE_FILE="/pgo/raw/r_%p_%m.profraw" NCPU=4 /build/target/x86_64-unknown-linux-gnu/release/shopping-sessions /tmp/train /tmp/o2; \
    "$PROFDATA" merge -o /pgo/merged.profdata /pgo/raw; \
    test -s /pgo/merged.profdata; \
    rm -rf /build/target /tmp/train /tmp/o1 /tmp/o2 /tmp/synth-target; \
    RUSTFLAGS="$CPU_FLAGS -Cprofile-use=/pgo/merged.profdata" \
        cargo build --release --bin shopping-sessions; \
    rm -rf /pgo

FROM busybox:1.37-musl
USER 1000:1000
COPY --from=builder /build/target/x86_64-unknown-linux-gnu/release/shopping-sessions /app/shopping-sessions
WORKDIR /app
ENV SHOP_SNAPPY=0
CMD ["/app/shopping-sessions"]
