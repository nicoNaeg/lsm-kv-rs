.PHONY: build server bench bench-server flamegraph test lint fmt

build:
	cargo build --release

server: build
	./target/release/lsmkv-server --dir data

bench:
	cargo bench

bench-server: build
	./scripts/bench-server.sh

flamegraph: build
	./scripts/flamegraph-server.sh

test:
	cargo test --all-features

lint:
	cargo fmt --all --check
	cargo clippy --all-targets --all-features -- -D warnings

fmt:
	cargo fmt --all
	cargo clippy --all-targets --fix --allow-dirty
