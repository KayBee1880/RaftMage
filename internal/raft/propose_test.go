package raft

import (
	"testing"
	"time"
)

func TestProposeRejectsWhenNotLeader(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)

	index, term, isLeader := n.Propose([]byte("x"))

	if isLeader {
		t.Fatalf("expected isLeader = false for a follower")
	}
	if index != 0 || term != 0 {
		t.Errorf("index/term = %d/%d, want 0/0 on rejection", index, term)
	}
	if len(n.log) != 0 {
		t.Fatalf("log should be unchanged, got %+v", n.log)
	}
}

func TestProposeAppendsToLeaderLogAndReturnsIndexTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, &fakeTransport{})
	n.currentTerm = 3
	n.role = Leader
	n.initLeaderStateLocked()

	index, term, isLeader := n.Propose([]byte("set x=1"))

	if !isLeader {
		t.Fatalf("expected isLeader = true for a leader")
	}
	if index != 1 || term != 3 {
		t.Errorf("index/term = %d/%d, want 1/3", index, term)
	}
	if len(n.log) != 1 || string(n.log[0].Command) != "set x=1" || n.log[0].Term != 3 {
		t.Fatalf("log = %+v, want one entry {Term:3 Command:\"set x=1\"}", n.log)
	}
}

func TestProposeOnSingleNodeClusterCommitsImmediately(t *testing.T) {
	n := NewNode("node-1", nil, nil)
	n.StartElection()

	index, _, isLeader := n.Propose([]byte("x"))

	if !isLeader {
		t.Fatalf("expected isLeader = true")
	}
	if n.commitIndex != index {
		t.Fatalf("commitIndex = %d, want %d (a single-node cluster is its own majority)", n.commitIndex, index)
	}
}

func TestProposeTriggersImmediateReplicationWithoutWaitingForHeartbeat(t *testing.T) {
	transport := &fakeTransport{}
	n := NewNode("node-1", []string{"node-2"}, transport)
	n.currentTerm = 1
	n.role = Leader
	n.initLeaderStateLocked()

	n.Propose([]byte("x"))

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		transport.mu.Lock()
		sent := len(transport.appendEntriesLog)
		transport.mu.Unlock()
		if sent >= 1 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("expected Propose to trigger replication immediately, not wait for the next heartbeat tick")
}
