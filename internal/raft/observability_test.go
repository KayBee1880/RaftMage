package raft

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestNodeWithNoLoggerDoesNotPanic(t *testing.T) {
	n := NewNode("node-1", nil, nil, nil)
	n.StartElection()

	if n.Role() != Leader {
		t.Fatalf("expected the single-node cluster to elect itself leader")
	}
}

func TestSetLoggerReceivesVoteGrantedLog(t *testing.T) {
	var buf bytes.Buffer
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
	n.SetLogger(slog.New(slog.NewTextHandler(&buf, nil)))

	n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node-2"})

	if !strings.Contains(buf.String(), "granted vote") {
		t.Fatalf("expected log output to contain \"granted vote\", got: %s", buf.String())
	}
}

func TestSetLoggerReceivesVoteDeniedLog(t *testing.T) {
	var buf bytes.Buffer
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
	n.currentTerm = 2
	n.SetLogger(slog.New(slog.NewTextHandler(&buf, nil)))

	n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node-2"})

	if !strings.Contains(buf.String(), "denied vote") {
		t.Fatalf("expected log output to contain \"denied vote\", got: %s", buf.String())
	}
}

func TestMetricsTracksVotesGrantedAndDenied(t *testing.T) {
	n := NewNode("node-1", []string{"node-2", "node-3"}, nil, nil)

	n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node-2"})
	n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node-3"})

	m := n.Metrics()
	if m.VotesGranted != 1 {
		t.Errorf("VotesGranted = %d, want 1", m.VotesGranted)
	}
	if m.VotesDenied != 1 {
		t.Errorf("VotesDenied = %d, want 1", m.VotesDenied)
	}
}

func TestMetricsTracksElectionsStartedAndWon(t *testing.T) {
	n := NewNode("node-1", nil, nil, nil)

	n.StartElection()

	m := n.Metrics()
	if m.ElectionsStarted != 1 {
		t.Errorf("ElectionsStarted = %d, want 1", m.ElectionsStarted)
	}
	if m.ElectionsWon != 1 {
		t.Errorf("ElectionsWon = %d, want 1", m.ElectionsWon)
	}
}

func TestMetricsTracksEntriesProposedAndCommitted(t *testing.T) {
	n := NewNode("node-1", nil, nil, nil)
	n.StartElection()

	n.Propose([]byte("x"))

	m := n.Metrics()
	if m.EntriesProposed != 1 {
		t.Errorf("EntriesProposed = %d, want 1", m.EntriesProposed)
	}
	if m.EntriesCommitted != 1 {
		t.Errorf("EntriesCommitted = %d, want 1", m.EntriesCommitted)
	}
}

func TestMetricsTracksLogCompactions(t *testing.T) {
	n := NewNode("node-1", nil, nil, nil)
	n.log = []LogEntry{{Term: 1, Command: []byte("a")}}
	n.commitIndex = 1

	if err := n.Compact(1); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	if m := n.Metrics(); m.LogCompactions != 1 {
		t.Errorf("LogCompactions = %d, want 1", m.LogCompactions)
	}
}

func TestMetricsTracksSnapshotsInstalled(t *testing.T) {
	n := NewNode("node-2", []string{"node-1"}, nil, nil)

	n.HandleInstallSnapshot(InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "node-1",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Config:            []string{"node-1", "node-2"},
	})

	if m := n.Metrics(); m.SnapshotsInstalled != 1 {
		t.Errorf("SnapshotsInstalled = %d, want 1", m.SnapshotsInstalled)
	}
}

func TestMetricsTracksMembershipChanges(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, &fakeTransport{}, nil)
	n.currentTerm = 1
	n.role = Leader
	n.initLeaderStateLocked()

	if _, _, ok := n.AddServer("node-3"); !ok {
		t.Fatalf("expected AddServer to succeed")
	}

	if m := n.Metrics(); m.MembershipChanges != 1 {
		t.Errorf("MembershipChanges = %d, want 1", m.MembershipChanges)
	}
}
