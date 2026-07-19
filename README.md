# RaftMage

A distributed, strongly-consistent key-value store written in Go, built on the Raft consensus algorithm.

Work in progress. See [docs/architecture.md](docs/architecture.md) for the system design and current implementation status.

## Building

```
go build ./...
go vet ./...
go test ./...
```

Or, on a system with `make`:

```
make build
make vet
make test
```

`make race` / `go test ./... -race` requires a cgo-capable toolchain (works in CI; may not work on every local machine).

## License

[MIT](LICENSE)
