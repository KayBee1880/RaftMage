package raft

import (
	"errors"
	"testing"
)

type fakeTransport struct {
	replies map[string]RequestVoteReply
	errPeer map[string]bool
}

func (f *fakeTransport) SendRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	if f.errPeer[peer] {
		return RequestVoteReply{}, errors.New("simulated network error")
	}
	return f.replies[peer], nil
}

func (f *fakeTransport) SendAppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	if f.errPeer[peer] {
		return AppendEntriesReply{}, errors.New("simulated network error")
	}
	return AppendEntriesReply{Term: args.Term, Success: true}, nil
}

func TestHandleRequestVoteGrantsWhenTermIsNewer(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)

	reply := n.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node-2"})

	if !reply.VoteGranted {
		t.Fatalf("expected vote granted, got denied")
	}
	if got := n.Term(); got != 5 {
		t.Errorf("term = %d, want 5", got)
	}
	if got := n.VotedFor(); got != "node-2" {
		t.Errorf("votedFor = %q, want %q", got, "node-2")
	}
}

func TestHandleRequestVoteDeniesStaleTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)
	n.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node-2"})

	reply := n.HandleRequestVote(RequestVoteArgs{Term: 3, CandidateID: "node-3"})

	if reply.VoteGranted {
		t.Fatalf("expected vote denied for stale term")
	}
	if reply.Term != 5 {
		t.Errorf("reply term = %d, want 5", reply.Term)
	}
}

func TestHandleRequestVoteDeniesSecondCandidateSameTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2", "node-3"}, nil)
	n.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node-2"})

	reply := n.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node-3"})

	if reply.VoteGranted {
		t.Fatalf("expected vote denied, already voted for node-2 this term")
	}
}

func TestHandleRequestVoteRegrantsSameCandidateSameTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)
	n.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node-2"})

	reply := n.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node-2"})

	if !reply.VoteGranted {
		t.Fatalf("expected repeated vote for same candidate/term to be granted")
	}
}

func TestHandleRequestVoteDeniesOutOfDateLog(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)
	n.log = []LogEntry{{Term: 3}, {Term: 4}}

	reply := n.HandleRequestVote(RequestVoteArgs{
		Term:         5,
		CandidateID:  "node-2",
		LastLogIndex: 1,
		LastLogTerm:  3,
	})

	if reply.VoteGranted {
		t.Fatalf("expected vote denied for out-of-date candidate log")
	}
}

func TestStartElectionSingleNodeClusterBecomesLeaderImmediately(t *testing.T) {
	n := NewNode("node-1", nil, nil)

	n.StartElection()

	if got := n.Role(); got != Leader {
		t.Fatalf("role = %v, want %v", got, Leader)
	}
	if got := n.Term(); got != 1 {
		t.Errorf("term = %d, want 1", got)
	}
}

func TestStartElectionWinsWithMajority(t *testing.T) {
	transport := &fakeTransport{
		replies: map[string]RequestVoteReply{
			"node-2": {Term: 1, VoteGranted: true},
			"node-3": {Term: 1, VoteGranted: false},
		},
	}
	n := NewNode("node-1", []string{"node-2", "node-3"}, transport)

	n.StartElection()

	if got := n.Role(); got != Leader {
		t.Fatalf("role = %v, want %v", got, Leader)
	}
}

func TestStartElectionLosesWithoutMajority(t *testing.T) {
	transport := &fakeTransport{
		replies: map[string]RequestVoteReply{
			"node-2": {Term: 1, VoteGranted: false},
			"node-3": {Term: 1, VoteGranted: false},
		},
	}
	n := NewNode("node-1", []string{"node-2", "node-3"}, transport)

	n.StartElection()

	if got := n.Role(); got != Candidate {
		t.Fatalf("role = %v, want %v", got, Candidate)
	}
}

func TestStartElectionStepsDownOnHigherTerm(t *testing.T) {
	transport := &fakeTransport{
		replies: map[string]RequestVoteReply{
			"node-2": {Term: 9, VoteGranted: false},
			"node-3": {Term: 1, VoteGranted: true},
		},
	}
	n := NewNode("node-1", []string{"node-2", "node-3"}, transport)

	n.StartElection()

	if got := n.Role(); got != Follower {
		t.Fatalf("role = %v, want %v", got, Follower)
	}
	if got := n.Term(); got != 9 {
		t.Errorf("term = %d, want 9", got)
	}
}

func TestStartElectionIgnoresTransportErrors(t *testing.T) {
	transport := &fakeTransport{
		replies: map[string]RequestVoteReply{
			"node-2": {Term: 1, VoteGranted: true},
		},
		errPeer: map[string]bool{"node-3": true},
	}
	n := NewNode("node-1", []string{"node-2", "node-3"}, transport)

	n.StartElection()

	if got := n.Role(); got != Leader {
		t.Fatalf("role = %v, want %v", got, Leader)
	}
}
