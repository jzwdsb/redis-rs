#! /usr/bin/env bash

# follow https://doc.rust-lang.org/rustc/instrument-coverage.html#creating-coverage-reports
# to create coverage report

set -x

OUTPUT_DIR=./target/coverage
BIN_NAME=redis_rs
LLVM_PROFILE_FILE="$BIN_NAME.profraw"

echo "cleaning up old coverage data" 
cargo clean

echo "building with coverage instrumentation"

RUSTFLAGS="-C instrument-coverage" LLVM_PROFILE_FILE=$LLVM_PROFILE_FILE cargo test

# replace llvm-profdata with cargo-profdata
cargo-profdata -- merge -sparse $LLVM_PROFILE_FILE -o $BIN_NAME.profdata


for BIN_FILE in $(ls target/debug/deps/$BIN_NAME* | grep -v "\."); do
    cargo-cov -- report \
        --use-color --ignore-filename-regex='/.cargo/registry' \
        --instr-profile=$BIN_NAME.profdata \
        --object $BIN_FILE \
        --show-instantiations --show-line-counts-or-regions \
        --Xdemangler=rustfilt > $(basename $BIN_FILE).txt

    cargo-cov -- show \
        --use-color --ignore-filename-regex='/.cargo/registry' \
        --instr-profile=$BIN_NAME.profdata \
        --object $BIN_FILE \
        --show-instantiations --show-line-counts-or-regions \
        --Xdemangler=rustfilt > $(basename $BIN_FILE).txt
done