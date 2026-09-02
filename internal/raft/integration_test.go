package raft

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type loopbackTransport struct {
	mu    sync.Mutex
	nodes map[string]*Node
}

func newLoopbackTransport() *loopbackTransport {
	return &loopbackTransport{nodes: make(map[string]*Node)}
}

func (lt *loopbackTransport) register(n *Node) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.nodes[n.id] = n
}

func (lt *loopbackTransport) SendRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	lt.mu.Lock()
	n, ok := lt.nodes[peer]
	lt.mu.Unlock()
	if !ok {
		return RequestVoteReply{}, errors.New("unknown peer: " + peer)
	}
	return n.HandleRequestVote(args), nil
}

func (lt *loopbackTransport) SendAppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	lt.mu.Lock()
	n, ok := lt.nodes[peer]
	lt.mu.Unlock()
	if !ok {
		return AppendEntriesReply{}, errors.New("unknown peer: " + peer)
	}
	return n.HandleAppendEntries(args), nil
}

func waitForAnyLeader(t *testing.T, nodes []*Node, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.Role() == Leader {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

func TestLeaderHeartbeatsPreventFollowerReelection(t *testing.T) {
	transport := newLoopbackTransport()
	n1 := NewNode("node-1", []string{"node-2", "node-3"}, transport)
	n2 := NewNode("node-2", []string{"node-1", "node-3"}, transport)
	n3 := NewNode("node-3", []string{"node-1", "node-2"}, transport)
	nodes := []*Node{n1, n2, n3}

	for _, n := range nodes {
		transport.register(n)
	}
	for _, n := range nodes {
		n.Run()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	leader := waitForAnyLeader(t, nodes, 2*time.Second)
	leaderID := leader.id
	leaderTerm := leader.Term()

	time.Sleep(electionTimeoutMax * 3)

	if got := leader.Role(); got != Leader {
		t.Fatalf("leader %s stepped down unexpectedly: role = %v", leaderID, got)
	}
	if got := leader.Term(); got != leaderTerm {
		t.Fatalf("leader %s term changed from %d to %d — re-election happened despite heartbeats", leaderID, leaderTerm, got)
	}
}

func TestLeaderReplicatesAndCommitsAcrossRealNodes(t *testing.T) {
	transport := newLoopbackTransport()
	n1 := NewNode("node-1", []string{"node-2", "node-3"}, transport)
	n2 := NewNode("node-2", []string{"node-1", "node-3"}, transport)
	n3 := NewNode("node-3", []string{"node-1", "node-2"}, transport)
	nodes := []*Node{n1, n2, n3}

	for _, n := range nodes {
		transport.register(n)
	}
	for _, n := range nodes {
		n.Run()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	leader := waitForAnyLeader(t, nodes, 2*time.Second)

	leader.mu.Lock()
	leader.log = append(leader.log, LogEntry{Term: leader.currentTerm, Command: []byte("set x=1")})
	leader.mu.Unlock()

	deadline := time.Now().Add(2 * time.Second)
	for {
		allCaughtUp := true
		for _, n := range nodes {
			n.mu.Lock()
			caughtUp := len(n.log) == 1 &&
				string(n.log[0].Command) == "set x=1" &&
				n.commitIndex == 1
			n.mu.Unlock()
			if !caughtUp {
				allCaughtUp = false
				break
			}
		}
		if allCaughtUp {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("not every node replicated and committed the leader's entry within timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
