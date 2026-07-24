.PHONY: build test lint fmt

build:
	cargo build --release

test:
	cargo test --all-features

lint:
	cargo fmt --all --check
	cargo clippy --all-targets --all-features -- -D warnings

fmt:
	cargo fmt --all
	cargo clippy --all-targets --fix --allow-dirty
