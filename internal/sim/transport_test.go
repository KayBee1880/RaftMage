package sim

import (
	"errors"
	"testing"
	"time"

	"raftmage/internal/raft"
)

func TestSimTransportDeliversMessageToRegisteredPeer(t *testing.T) {
	target := raft.NewNode("node-2", nil, nil, nil)
	transport := NewSimTransport(Config{Seed: 1})
	transport.Register("node-2", target)

	reply, err := transport.SendRequestVote("node-2", raft.RequestVoteArgs{Term: 1, CandidateID: "node-1"})
	if err != nil {
		t.Fatalf("SendRequestVote returned error: %v", err)
	}
	if !reply.VoteGranted {
		t.Fatalf("expected vote granted, got denied: %+v", reply)
	}
}

func TestSimTransportReturnsErrorForUnknownPeer(t *testing.T) {
	transport := NewSimTransport(Config{Seed: 1})

	_, err := transport.SendRequestVote("ghost", raft.RequestVoteArgs{Term: 1})
	if !errors.Is(err, ErrUnknownPeer) {
		t.Fatalf("expected ErrUnknownPeer, got %v", err)
	}
}

func TestSimTransportIsolateBlocksDelivery(t *testing.T) {
	target := raft.NewNode("node-2", nil, nil, nil)
	transport := NewSimTransport(Config{Seed: 1})
	transport.Register("node-2", target)
	transport.Isolate("node-2")

	_, err := transport.SendRequestVote("node-2", raft.RequestVoteArgs{Term: 1})
	if !errors.Is(err, ErrPeerUnreachable) {
		t.Fatalf("expected ErrPeerUnreachable, got %v", err)
	}
}

func TestSimTransportRestoreReenablesDelivery(t *testing.T) {
	target := raft.NewNode("node-2", nil, nil, nil)
	transport := NewSimTransport(Config{Seed: 1})
	transport.Register("node-2", target)
	transport.Isolate("node-2")
	transport.Restore("node-2")

	_, err := transport.SendRequestVote("node-2", raft.RequestVoteArgs{Term: 1})
	if err != nil {
		t.Fatalf("expected delivery to succeed after restore, got error: %v", err)
	}
}

func TestSimTransportDelayFallsWithinConfiguredRange(t *testing.T) {
	target := raft.NewNode("node-2", nil, nil, nil)
	transport := NewSimTransport(Config{
		Seed:     1,
		MinDelay: 5 * time.Millisecond,
		MaxDelay: 15 * time.Millisecond,
	})
	transport.Register("node-2", target)

	for i := 0; i < 20; i++ {
		start := time.Now()
		if _, err := transport.SendRequestVote("node-2", raft.RequestVoteArgs{Term: 1}); err != nil {
			t.Fatalf("SendRequestVote returned error: %v", err)
		}
		elapsed := time.Since(start)
		if elapsed < 5*time.Millisecond {
			t.Fatalf("call #%d took %v, want at least MinDelay (5ms)", i, elapsed)
		}
		if elapsed > 65*time.Millisecond {
			t.Fatalf("call #%d took %v, want well under MaxDelay (15ms) plus generous scheduling slack", i, elapsed)
		}
	}
}

func TestSimTransportDropProbabilityRoughlyMatchesConfiguredRate(t *testing.T) {
	target := raft.NewNode("node-2", nil, nil, nil)
	transport := NewSimTransport(Config{Seed: 1, DropProbability: 0.3})
	transport.Register("node-2", target)

	const trials = 2000
	dropped := 0
	for i := 0; i < trials; i++ {
		if _, err := transport.SendRequestVote("node-2", raft.RequestVoteArgs{Term: 1}); err != nil {
			dropped++
		}
	}

	rate := float64(dropped) / float64(trials)
	if rate < 0.2 || rate > 0.4 {
		t.Fatalf("observed drop rate %.3f, want roughly 0.3 (0.2-0.4 tolerance band over %d trials)", rate, trials)
	}
}

func TestSimTransportSameSeedProducesSameFaultSequenceWhenCallsAreSerialized(t *testing.T) {
	cfg := Config{Seed: 42, MinDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond, DropProbability: 0.5}

	targetA := raft.NewNode("node-2", nil, nil, nil)
	transportA := NewSimTransport(cfg)
	transportA.Register("node-2", targetA)

	targetB := raft.NewNode("node-2", nil, nil, nil)
	transportB := NewSimTransport(cfg)
	transportB.Register("node-2", targetB)

	for i := 0; i < 50; i++ {
		_, delayA, errA := transportA.resolve("node-2")
		_, delayB, errB := transportB.resolve("node-2")
		if delayA != delayB {
			t.Fatalf("call #%d: delay diverged, %v vs %v", i, delayA, delayB)
		}
		if !errors.Is(errA, errB) && errA != errB {
			t.Fatalf("call #%d: error diverged, %v vs %v", i, errA, errB)
		}
	}
}
