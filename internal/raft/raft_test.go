package raft

import "testing"

func TestNewNodeStartsAsFollower(t *testing.T) {
	n := NewNode("node-1", []string{"node-2", "node-3"}, nil)

	if got := n.Role(); got != Follower {
		t.Errorf("new node role = %v, want %v", got, Follower)
	}
	if got := n.Term(); got != 0 {
		t.Errorf("new node term = %d, want 0", got)
	}
}
