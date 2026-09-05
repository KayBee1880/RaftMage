package raft

import (
	"errors"
	"sync"
	"testing"
)

type fakeTransport struct {
	replies map[string]RequestVoteReply
	errPeer map[string]bool

	appendEntriesFn func(peer string, args AppendEntriesArgs) (AppendEntriesReply, error)
	appendReplies   map[string]AppendEntriesReply

	mu               sync.Mutex
	appendEntriesLog []AppendEntriesArgs
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

	f.mu.Lock()
	f.appendEntriesLog = append(f.appendEntriesLog, args)
	f.mu.Unlock()

	if f.appendEntriesFn != nil {
		return f.appendEntriesFn(peer, args)
	}
	if reply, ok := f.appendReplies[peer]; ok {
		return reply, nil
	}
	return AppendEntriesReply{Term: args.Term, Success: true}, nil
}

var errSimulatedStorageFailure = errors.New("simulated storage failure")

type fakeStorage struct {
	mu      sync.Mutex
	saved   []PersistentState
	loadErr error
	saveErr error
}

func (f *fakeStorage) Save(state PersistentState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return f.saveErr
	}
	f.saved = append(f.saved, state)
	return nil
}

func (f *fakeStorage) Load() (PersistentState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loadErr != nil {
		return PersistentState{}, f.loadErr
	}
	if len(f.saved) == 0 {
		return PersistentState{}, nil
	}
	return f.saved[len(f.saved)-1], nil
}

func TestHandleRequestVoteGrantsWhenTermIsNewer(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)

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
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
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
	n := NewNode("node-1", []string{"node-2", "node-3"}, nil, nil)
	n.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node-2"})

	reply := n.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node-3"})

	if reply.VoteGranted {
		t.Fatalf("expected vote denied, already voted for node-2 this term")
	}
}

func TestHandleRequestVoteRegrantsSameCandidateSameTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
	n.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node-2"})

	reply := n.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node-2"})

	if !reply.VoteGranted {
		t.Fatalf("expected repeated vote for same candidate/term to be granted")
	}
}

func TestHandleRequestVoteDeniesOutOfDateLog(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil, nil)
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
	n := NewNode("node-1", nil, nil, nil)

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
	n := NewNode("node-1", []string{"node-2", "node-3"}, transport, nil)

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
	n := NewNode("node-1", []string{"node-2", "node-3"}, transport, nil)

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
	n := NewNode("node-1", []string{"node-2", "node-3"}, transport, nil)

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
	n := NewNode("node-1", []string{"node-2", "node-3"}, transport, nil)

	n.StartElection()

	if got := n.Role(); got != Leader {
		t.Fatalf("role = %v, want %v", got, Leader)
	}
}
