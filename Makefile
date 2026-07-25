.PHONY: build server bench-server test lint fmt

build:
	cargo build --release

server: build
	./target/release/lsmkv-server --dir data

bench-server: build
	./scripts/bench-server.sh

test:
	cargo test --all-features

lint:
	cargo fmt --all --check
	cargo clippy --all-targets --all-features -- -D warnings

fmt:
	cargo fmt --all
	cargo clippy --all-targets --fix --allow-dirty
