<div align="center">

# RaftMage

**A distributed key-value store built from scratch in Go, implementing the Raft consensus algorithm.**

[![Go](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)](go.mod)
[![CI](https://github.com/KayBee1880/RaftMage/actions/workflows/ci.yml/badge.svg)](https://github.com/KayBee1880/RaftMage/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-early%20development-yellow.svg)](docs/architecture.md)

*Every claim in this README is either true right now (verifiable with the commands below) or explicitly marked planned.*

</div>

## What this is

RaftMage is a small, from-scratch distributed key-value store. For infrastructure like this, the `Get`/`Put`/`Delete` API isn't really the product; the guarantee behind it is the product: that a write the cluster acknowledges survives node crashes, leader failures, and network partitions without being lost or silently contradicted. Raft, the consensus algorithm this project implements from first principles, is what makes that guarantee real instead of aspirational. Everything here is built and tested to prove the guarantee holds, not just to look like it does.

## Why this project exists

A single-node key-value store is easy. Making it survive a crash without losing data, or without serving stale or contradictory answers during a leader failure or network partition, is the actual problem distributed databases exist to solve, and it's provably impossible to solve perfectly, only to engineer a deliberate, defensible tradeoff for. RaftMage exists to build and prove one specific, well-understood answer to that problem at a small enough scale that every line of the consensus logic can be read, tested, and explained.

See [docs/architecture.md](docs/architecture.md) for the full design rationale, including why Raft was chosen and how the pieces fit together.

## Engineering approach

Principles this build has actually practiced, each one checkable against the commit history or the code itself, not aspirational:

- **No invented numbers.** Every count in this README (tests, components) comes straight from running `go test`/reading the code, not an estimate.
- **Reasoning is documented as "naive approach → why it breaks → the fix,"** not just "here's the code." Design docs and code-level rationale consistently explain why a simpler approach, such as a fixed (non-randomized) election timeout, or counting replicas without Raft's current-term-only commit rule, would be unsafe or livelock the cluster, before describing what's actually implemented.
- **Safety is enforced at the core, not left to callers.** All node-state mutation goes through a single mutex-guarded path inside the `Node` type; a `*Locked` naming convention makes it explicit which internal functions assume the lock is already held, so correctness doesn't depend on every caller remembering to lock correctly.
- **Deferred and rejected scope is stated explicitly, not silently absent.** [docs/architecture.md](docs/architecture.md) tracks Implemented vs. Planned per component; the Roadmap below does the same.
- **Tests exercise real logic, not mocks pretending to be the system.** Two things still get faked where a unit test doesn't need the real thing: the network (`fakeTransport`) and, in most tests, persistence (`fakeStorage`). The consensus logic itself, including real goroutines, timers, vote counting, and log persistence, runs for real; the actual `FileStorage` implementation is tested directly against the filesystem, and the actual `GRPCTransport`/`GRPCServer` are tested directly over real TCP sockets, neither one just faked.
- **CI runs the full suite with Go's race detector** (`go test ./... -race`) on every push and pull request; concurrency bugs are a CI gate, not something caught by hoping.

## What's actually working right now

- **Leader election** (`RequestVote` RPC) implementing Raft's full safety rules: one vote per term, and a candidate's log must be at least as up to date as the voter's before it gets a vote.
- **Automatic elections**: a randomized election-timeout loop (150–300ms per node) that triggers a new election with no manual intervention, specifically to avoid every node timing out simultaneously and splitting the vote forever.
- **Leader liveness**: `AppendEntries` heartbeats every 50ms that keep a healthy elected leader in power. Proven with three real `Node` instances in a genuine election, not just asserted: `TestLeaderHeartbeatsPreventFollowerReelection`.
- **Log replication (follower side)**: consistency checking against `PrevLogIndex`/`PrevLogTerm`, truncation of conflicting entries, idempotent handling of retried/duplicate RPCs, and commit-index advancement bounded by what's actually stored locally.
- **Log replication (leader side)**: per-follower `nextIndex`/`matchIndex` tracking, immediate retry at an earlier log position on rejection, and commit-index advancement gated by Raft's current-term-only rule (a leader can only directly commit an entry from its own term, never an earlier one, no matter how widely replicated). Proven with three real `Node` instances, not just asserted: `TestLeaderReplicatesAndCommitsAcrossRealNodes`.
- **Client-facing write API (`Propose`)**: the first way to actually get a write into the cluster. Rejected outright on a non-leader (`isLeader == false`, so a client knows to look elsewhere), otherwise appended to the leader's log and replicated immediately rather than waiting for the next 50ms heartbeat tick. A single-node cluster commits its own proposal instantly, since the leader alone is already a majority.
- **Crash recovery**: `currentTerm`, `votedFor`, and the log are persisted to disk (`FileStorage`, an atomic JSON snapshot per write, using a temp file, `fsync`, and rename, so a crash mid-write can never corrupt the on-disk state) before a node responds to any RPC that depends on that state surviving a restart: granting a vote, replicating an entry, or accepting a client's `Propose`. A restarted node reloads its term, vote, and log from disk instead of starting blank, which is what actually makes it safe to keep participating in the cluster afterward rather than risking a repeated vote or a forgotten entry.
- **Log compaction (`Node.Compact`)**: trims committed log entries a node no longer needs to keep in memory or on disk, replacing them with a `(lastIncludedIndex, lastIncludedTerm)` boundary the rest of the code treats as a normal (if unusually old) log position. Bounded only by `commitIndex`, matching standard Raft: any node, leader or follower, may compact anything it knows is committed, whether or not every peer has caught up to that point. When a `StateMachine` is attached and caught up, `Compact` also captures a real serialized snapshot of it (see the state machine row below), not just log metadata anymore.
- **`InstallSnapshot` RPC**: what makes the compaction rule above safe. When a leader's `replicatePeer` discovers a follower's `nextIndex` has fallen behind the leader's own `lastIncludedIndex`, meaning it can no longer explain that follower's log position through ordinary `AppendEntries`, it sends an `InstallSnapshot` instead: the boundary *and* the leader's own captured state-machine snapshot, adopted directly, followed immediately by ordinary replication for everything after it. A follower that already has entries consistent with the offered boundary keeps them rather than discarding needlessly, the Raft paper's retain trailing entries optimization. Proven end-to-end with three real nodes, one deliberately disconnected before any entries are proposed, then reconnected after the leader has compacted well past what it ever received: `TestLeaderInstallsSnapshotToCatchUpFarBehindFollower`.
- **Real gRPC transport**: `GRPCTransport`/`GRPCServer` (`internal/transport`) implement the exact same `Transport` interface the consensus core already depends on, backed by real `google.golang.org/grpc` clients and servers instead of an in-process fake. Every RPC's wire format is generated from a `.proto` definition, not hand-rolled. Proven with three real nodes electing a leader and replicating a committed write over actual TCP sockets, not just an in-process call: `TestThreeNodeClusterElectsLeaderAndReplicatesOverRealGRPC`.
- **Cluster membership changes (`Node.AddServer`/`Node.RemoveServer`)**: adds or removes one server at a time, the standard simplification (from Ongaro's Raft thesis) that avoids needing full joint consensus, since any two configurations differing by one member always share an overlapping majority. A configuration change is just another log entry (`EntryConfig`), replicated, retried, and committed by the exact same code path as an ordinary write, adopted the instant it's appended, not only once committed, per Raft's own rule. A leader that commits its own removal steps down automatically. Proven with real, concurrently-running nodes both growing a cluster (`TestLeaderAddsServerAndReplicatesToNewMember`, a brand new node joins and catches up on replication) and shrinking one (`TestLeaderRemovesFollowerAndContinuesOperating`, the remaining nodes keep functioning correctly).
- **Fault-injection testing (`internal/sim`)**: `SimTransport` wraps real `Node`s in a seeded random fault injector, random per-message delay, random message drops, and node isolation/restore, so a fixed seed reproduces the same aggregate fault behavior across runs. Not a virtual-time deterministic simulator (real goroutines and real wall-clock scheduling still introduce some nondeterminism in exactly which message draws which random outcome, an honestly stated gap, not a claim this project makes); see [docs/architecture.md](docs/architecture.md) for the precise boundary. Its flagship test, `TestClusterMaintainsSafetyUnderRandomFaults`, runs a real 5-node cluster for 3 seconds under continuous random delay, drops, and single-node isolation while proposing writes, and checks two Raft safety properties on every poll, not just at the end: election safety (never two leaders in the same term) and state machine safety (no two nodes ever disagree about a committed entry).
- **Observability (structured logs + metrics)**: `Node.SetLogger` attaches a standard-library `*slog.Logger` (nil by default, so every existing test is unaffected) that emits one structured log line per state transition, elections starting/won/lost, votes granted/denied with a reason, stepping down to follower, log compaction, snapshot installs, membership changes, proposed entries, never on routine per-heartbeat traffic. `Node.Metrics()` returns a small counter snapshot (elections, votes, entries proposed/committed, compactions, snapshots installed, membership changes) incremented at the same points. Deliberately a library feature, not a running service: no `cmd/` binary or HTTP `/metrics` endpoint exists yet to expose either one externally, an honest boundary, not an oversight; see [docs/architecture.md](docs/architecture.md).
- **State machine (`internal/kvstore.Store`)**: a real, in-memory key-value store, `Put`/`Delete` commands JSON-encoded into the same opaque `[]byte` `Propose` already accepted, applied to every node's own store the instant an entry commits (`Node.SetStateMachine`, nil by default, no existing test affected). `StateMachine` also has `Snapshot()`/`Restore()`, so `Compact` captures a real snapshot and `InstallSnapshot` carries it to a far-behind follower, and a node that restarts after ever compacting restores its own state machine from disk too, closing what was originally a deliberately scoped, named gap in a follow-up milestone rather than left open. Proven end to end across three real, concurrently-running nodes, each with its own independent store that has to converge without sharing memory: `TestLeaderAppliesProposedEntriesToStateMachineAcrossRealNodes`.
- **Client API (gRPC `Get`/`Put`/`Delete`, `internal/clientapi`)**: the first network surface for anything outside the cluster's own peers, a genuinely separate `KV` gRPC service from the peer-to-peer `Raft` one, wire format generated from its own `.proto` file. `Put`/`Delete` call `Propose` and then wait for the entry to actually be applied (`Node.LastApplied()` reaching the proposed index), not just accepted by the leader, so a successful reply means the same thing this project's own stated guarantee means everywhere else. Rejected on a non-leader with a `codes.FailedPrecondition` status naming `Node.CurrentLeader()` (a new accessor, tracking the last known leader from every legitimate `AppendEntries`/`InstallSnapshot`/election win) as a retry hint when one is known. Uses the caller's own gRPC request context for cancellation, a `codes.DeadlineExceeded` status if it expires before commitment, rather than inventing a fixed timeout the way the peer transport layer had to. `Get` reads directly from the local node's attached store, no leader check, no waiting, an explicitly eventually-consistent read, matching `internal/kvstore`'s own already-stated limitation, not a stronger guarantee than what's actually implemented. Proven end to end with three real nodes and a real gRPC client reaching a real leader over TCP: `TestThreeNodeClusterCommitsPutOverRealGRPCClientAPI`.
- **140 tests, all passing** (105 in `internal/raft`, 10 in `internal/kvstore`, 4 in `internal/storage`, 6 in `internal/transport`, 8 in `internal/sim`, 7 in `internal/clientapi`), run automatically on every push and pull request via GitHub Actions (`go build`, `go vet`, `go test -race`).

A cluster can now be written to and read from over a real network connection, `internal/clientapi`'s tests are proof, but there is still no standalone server binary: every node, transport, and client in this project is still wired up and torn down entirely inside Go's test runner. See Roadmap.

## Architecture

**Current state**: what's really running today. A real client can now reach a real cluster over gRPC, `internal/clientapi`'s `KV` service, but there's still no standalone process to run it from: `Node` instances, their peer-to-peer transport, and this new client-facing service are all still started and stopped entirely inside Go's test runner, not a long-running binary. `Node` instances talk to each other over a real network (`GRPCTransport`/`GRPCServer`, real `google.golang.org/grpc` sockets) or an in-process fake, persist through a real `Storage` implementation, apply committed writes to a real `StateMachine` (`internal/kvstore.Store`), and, new this milestone, can be reached directly by an external gRPC client through `internal/clientapi.GRPCServer`, a second, separate gRPC service from the peer-to-peer one.

```mermaid
graph LR
    Client["gRPC client<br/>(kvpb.KVClient)"]
    subgraph "Driven entirely by Go tests, no standalone binary yet"
        CA["internal/clientapi.GRPCServer<br/>(KV service: Get/Put/Delete)"]
        N1["Node"]
        N2["Node"]
        N3["Node"]
    end
    TP["Transport interface<br/>(fakeTransport/loopbackTransport in most tests,<br/>real GRPCTransport over TCP in internal/transport's tests)"]
    ST["Storage interface<br/>(fakeStorage in most tests,<br/>real FileStorage where persistence is tested)"]
    SM["StateMachine interface<br/>(nil in most tests,<br/>real internal/kvstore.Store where applying is tested)"]
    Client --> CA
    CA --> N1
    N1 --> TP
    N2 --> TP
    N3 --> TP
    N1 --> ST
    N2 --> ST
    N3 --> ST
    N1 --> SM
    N2 --> SM
    N3 --> SM
    CA --> SM
```

**Target state**: the eventual system this is building toward. Every piece inside the box below now exists as real, tested Go code; what's still missing is the box itself, a single long-running process wiring them all up and listening on real ports without a test runner driving it. Shown for direction, not to claim the binary exists yet.

```mermaid
graph TB
    Client["Client"]
    subgraph Node["RaftMage Node (cmd/ binary still planned)"]
        API["Client API<br/>(gRPC: Get / Put / Delete)"]
        Raft["Raft Core<br/>(election + full log replication)"]
        Storage["Storage<br/>(write-ahead log + snapshots)"]
        SM["State Machine<br/>(the key-value store)"]
        Transport["Transport<br/>(gRPC)"]
    end
    Peer1["Peer Node"]
    Peer2["Peer Node"]
    Client --> API --> Raft
    Raft --> Storage
    Raft --> SM
    Raft --> Transport
    Transport <--> Peer1
    Transport <--> Peer2
```

## Tech stack

**In use today**

| Component | Why |
|---|---|
| Go 1.26 | Goroutines/channels map directly onto Raft's concurrent RPC fan-out and per-role background timers; strong static typing catches a class of consensus bugs at compile time. |
| Go's standard `testing` package | Sufficient for the current unit + integration test needs; no external framework justified yet. |
| gRPC + Protocol Buffers | `internal/transport`'s real `GRPCTransport`/`GRPCServer` (peer-to-peer, `internal/transport/raftpb/raft.proto`) and `internal/clientapi`'s real `GRPCServer` (client-facing `KV` service, `internal/clientapi/kvpb/kv.proto`), two genuinely separate services generated from two separate `.proto` files, sharing nothing but the toolchain. |
| GitHub Actions | Free, native CI for a GitHub-hosted repo; runs build, `vet`, and race-detector tests on every push/PR. |
| Go's standard `math/rand` (seeded) | `internal/sim`'s `SimTransport` draws every fault-injection decision (delay, drop) from one seeded `*rand.Rand`, so a fixed seed reproduces the same aggregate fault behavior without needing an external fuzzing framework. |
| Go's standard `log/slog` | `Node.SetLogger` accepts a `*slog.Logger` directly, no custom logging interface invented; structured, leveled logging with pluggable output handlers (text, JSON, or a custom one) comes from the standard library alone. |
| Go's standard `encoding/json` | `internal/kvstore`'s `Command`/`Op` encoding, the same reasoning `internal/storage.FileStorage` already chose JSON for: human-readable, standard-library only, and fast enough at this project's scale. |

**Planned**

| Component | Milestone |
|---|---|
| Virtual-time deterministic simulator | A distinct, larger step past `internal/sim`'s current seeded-fault-injection-over-real-time approach |
| Metrics/log exposition (HTTP `/metrics`, log shipping) | Needs the `cmd/`/`Client API` milestone first; there's no running service yet to expose them from |

## Roadmap

- [x] Raft node core state (roles, terms, log)
- [x] Leader election (`RequestVote` RPC, full safety rules)
- [x] Randomized election-timeout loop (automatic elections)
- [x] `AppendEntries` heartbeats (leader liveness)
- [x] Log replication, follower side (consistency check, append/truncate, commit index)
- [x] Log replication, leader side (`nextIndex`/`matchIndex`, retry, commit advancement)
- [x] Client-facing write API (`Propose`)
- [x] Persistent write-ahead log + crash recovery
- [x] Log compaction (bounded only by `commitIndex`, matching standard Raft)
- [x] `InstallSnapshot` RPC (catch up a follower that's fallen behind the compaction point)
- [x] Real gRPC transport
- [x] Cluster membership changes (`AddServer`/`RemoveServer`, one server at a time)
- [x] Fault-injection testing (`internal/sim`'s seeded `SimTransport` + safety-invariant checking; a full virtual-time deterministic simulator remains a distinct, larger future step)
- [x] Observability (structured logs via `log/slog`, `Node.Metrics()` counters; not yet exposed externally, no service exists to expose them from)
- [x] State machine (`internal/kvstore.Store`, applied on every ordinary commit)
- [x] `InstallSnapshot` real state-machine payload (`StateMachine.Snapshot`/`Restore`, `Compact` captures it, the RPC carries it; also fixed a related gap, restoring the state machine after a restart that followed compaction)
- [x] Client API (gRPC `Get`/`Put`/`Delete`, `internal/clientapi`; waits for real commitment, not just leader acceptance, and gives a rejected write a `Node.CurrentLeader()` retry hint)
- [ ] `cmd/` entrypoint (a real, runnable server binary; everything today runs only inside Go's test runner) ← current
- [ ] Sharding (stretch goal, past the core single-group KV store)

Fuller status, including what's implemented vs. planned per component: [docs/architecture.md](docs/architecture.md).

## Repository structure

Reflects the actual current tree; nothing listed here that doesn't exist yet.

```
raftmage/
├── go.mod
├── Makefile
├── LICENSE
├── README.md
├── docs/
│   └── architecture.md
├── .github/workflows/
│   └── ci.yml
└── internal/
    ├── raft/                    # the consensus core
    │   ├── raft.go              # Node state: roles, terms, log
    │   ├── election.go          # RequestVote RPC, leader election
    │   ├── election_timer.go    # randomized election-timeout loop
    │   ├── append_entries.go    # AppendEntries RPC: heartbeats + log replication (both sides)
    │   ├── propose.go           # Propose: the client-facing write API
    │   ├── compact.go           # Node.Compact: log compaction, bounded only by commitIndex
    │   ├── install_snapshot.go  # InstallSnapshot RPC: catches up a far-behind follower
    │   ├── membership.go        # AddServer/RemoveServer: one-server-at-a-time cluster membership changes
    │   ├── observability.go     # SetLogger (log/slog) + Metrics() counters
    │   ├── state_machine.go     # StateMachine interface + the apply loop (commit -> applied)
    │   ├── transport.go         # Transport interface: the network dependency-inversion boundary
    │   ├── storage.go           # Storage interface: the persistence dependency-inversion boundary
    │   └── *_test.go            # 105 tests, including seven 3-node integration tests
    ├── kvstore/                 # the real, in-memory StateMachine implementation
    │   ├── store.go             # Store: Put/Delete key-value store, JSON command encoding, Snapshot/Restore
    │   └── store_test.go        # 10 tests
    ├── storage/                 # the real, disk-backed Storage implementation
    │   ├── file_storage.go      # FileStorage: atomic JSON-snapshot persistence to disk
    │   └── file_storage_test.go # 4 tests
    ├── transport/               # the real, network-backed Transport implementation
    │   ├── raftpb/
    │   │   ├── raft.proto       # wire format for RequestVote/AppendEntries/InstallSnapshot
    │   │   ├── raft.pb.go       # generated by protoc, not hand-written
    │   │   └── raft_grpc.pb.go  # generated by protoc, not hand-written
    │   ├── client.go            # GRPCTransport: satisfies raft.Transport over real gRPC
    │   ├── server.go            # GRPCServer: dispatches incoming RPCs to a real *raft.Node
    │   └── *_test.go            # 6 tests, including a 3-node cluster over real TCP sockets
    ├── sim/                     # seeded fault-injecting Transport, for chaos testing
    │   ├── transport.go         # SimTransport: satisfies raft.Transport with random delay/drop/isolation
    │   └── *_test.go            # 8 tests, including a 5-node safety-invariant chaos test
    └── clientapi/                # the client-facing gRPC service (Get/Put/Delete), separate from peer-to-peer transport
        ├── kvpb/
        │   ├── kv.proto          # wire format for the KV service: Get/Put/Delete
        │   ├── kv.pb.go          # generated by protoc, not hand-written
        │   └── kv_grpc.pb.go     # generated by protoc, not hand-written
        ├── server.go             # GRPCServer: Propose + wait for commitment, structured gRPC status errors
        └── *_test.go             # 7 tests, including a 3-node cluster with a real client over real TCP sockets
```

No `cmd/` entrypoint yet; there is nothing to `go run`. See Roadmap.

## Local setup

Requires Go 1.26+. Every command below has been run against this exact repo state.

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

`make race` / `go test ./... -race` requires a cgo-capable toolchain. Works in CI, may not work on every local machine.

The closest thing to a running demo right now: three real nodes electing a leader, staying stable under it, and replicating and committing a real client write via `Propose`, end to end:

```
go test ./internal/raft -run TestLeaderHeartbeatsPreventFollowerReelection -v
go test ./internal/raft -run TestLeaderReplicatesAndCommitsAcrossRealNodes -v
go test ./internal/raft -run TestLeaderCompactsLogSafelyWithoutStrandingFollowers -v
go test ./internal/raft -run TestLeaderInstallsSnapshotToCatchUpFarBehindFollower -v
go test ./internal/transport -run TestThreeNodeClusterElectsLeaderAndReplicatesOverRealGRPC -v
go test ./internal/raft -run TestLeaderAddsServerAndReplicatesToNewMember -v
go test ./internal/raft -run TestLeaderRemovesFollowerAndContinuesOperating -v
go test ./internal/sim -run TestClusterMaintainsSafetyUnderRandomFaults -v
go test ./internal/raft -run TestSetLoggerReceivesVoteGrantedLog -v
go test ./internal/raft -run TestLeaderAppliesProposedEntriesToStateMachineAcrossRealNodes -v
go test ./internal/clientapi -run TestThreeNodeClusterCommitsPutOverRealGRPCClientAPI -v
```

The last command's own test output includes a real `log/slog` structured log line (`node=... term=... role=... candidate=... msg="granted vote"`), the smallest possible look at what `Node.SetLogger` actually produces.

## Documentation

- [docs/architecture.md](docs/architecture.md): system design, component status, and diagrams.

## License

[MIT](LICENSE)
