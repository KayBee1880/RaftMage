package raft

import (
	"testing"
	"time"
)

func waitForRole(t *testing.T, n *Node, want Role, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.Role() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("role = %v after %s, want %v", n.Role(), timeout, want)
}

func TestRunSingleNodeBecomesLeaderAutomatically(t *testing.T) {
	n := NewNode("node-1", nil, nil)
	n.Run()
	defer n.Stop()

	waitForRole(t, n, Leader, time.Second)
}

func TestRunThreeNodeClusterElectsLeaderAutomatically(t *testing.T) {
	transport := &fakeTransport{
		replies: map[string]RequestVoteReply{
			"node-2": {VoteGranted: true},
			"node-3": {VoteGranted: true},
		},
	}
	n := NewNode("node-1", []string{"node-2", "node-3"}, transport)
	n.Run()
	defer n.Stop()

	waitForRole(t, n, Leader, time.Second)
}

func TestStopPreventsFurtherElections(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, &fakeTransport{})
	n.Run()
	n.Stop()

	time.Sleep(electionTimeoutMax + 50*time.Millisecond)

	if got := n.Role(); got != Follower {
		t.Fatalf("role = %v after Stop, want %v", got, Follower)
	}
}

func TestRunCalledTwiceIsIdempotent(t *testing.T) {
	n := NewNode("node-1", nil, nil)
	n.Run()
	n.Run()
	defer n.Stop()

	waitForRole(t, n, Leader, time.Second)
}
