package sim

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"raftmage/internal/raft"
)

func TestClusterMaintainsSafetyUnderRandomFaults(t *testing.T) {
	const seed = 42

	transport := NewSimTransport(Config{
		Seed:            seed,
		MinDelay:        1 * time.Millisecond,
		MaxDelay:        15 * time.Millisecond,
		DropProbability: 0.1,
	})

	ids := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	nodes := make([]*raft.Node, len(ids))
	for i, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		n := raft.NewNode(id, peers, transport, nil)
		transport.Register(id, n)
		nodes[i] = n
	}

	for _, n := range nodes {
		n.Run()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(seed + 1))
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(50+rng.Intn(100)) * time.Millisecond):
			}
			id := ids[rng.Intn(len(ids))]
			transport.Isolate(id)
			select {
			case <-stop:
				transport.Restore(id)
				return
			case <-time.After(time.Duration(50+rng.Intn(150)) * time.Millisecond):
			}
			transport.Restore(id)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, n := range nodes {
				if n.Role() == raft.Leader {
					n.Propose([]byte(fmt.Sprintf("entry-%d", i)))
					i++
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		checkElectionSafety(t, ids, nodes)
		checkCommittedEntriesAgree(t, nodes)
		time.Sleep(20 * time.Millisecond)
	}

	close(stop)
	wg.Wait()

	maxCommitted := uint64(0)
	for _, n := range nodes {
		if c := n.CommitIndex(); c > maxCommitted {
			maxCommitted = c
		}
	}
	if maxCommitted < 3 {
		t.Fatalf("expected meaningful progress under fault injection, best commitIndex across the cluster was only %d", maxCommitted)
	}
}

func checkElectionSafety(t *testing.T, ids []string, nodes []*raft.Node) {
	t.Helper()
	leadersByTerm := make(map[uint64][]string)
	for i, n := range nodes {
		if n.Role() == raft.Leader {
			term := n.Term()
			leadersByTerm[term] = append(leadersByTerm[term], ids[i])
		}
	}
	for term, leaders := range leadersByTerm {
		if len(leaders) > 1 {
			t.Fatalf("election safety violated: term %d has multiple leaders: %v", term, leaders)
		}
	}
}

func checkCommittedEntriesAgree(t *testing.T, nodes []*raft.Node) {
	t.Helper()

	type committed struct {
		start   uint64
		entries []raft.LogEntry
	}
	all := make([]committed, len(nodes))
	for i, n := range nodes {
		start, entries := n.CommittedEntries()
		all[i] = committed{start, entries}
	}

	for i := 0; i < len(all); i++ {
		for j := i + 1; j < len(all); j++ {
			a, b := all[i], all[j]
			lo := max(a.start, b.start)
			hi := min(a.start+uint64(len(a.entries)), b.start+uint64(len(b.entries)))
			for idx := lo; idx < hi; idx++ {
				ea := a.entries[idx-a.start]
				eb := b.entries[idx-b.start]
				if ea.Term != eb.Term || string(ea.Command) != string(eb.Command) {
					t.Fatalf("state machine safety violated at index %d: %+v vs %+v", idx, ea, eb)
				}
			}
		}
	}
}
