package raft

import "testing"

func TestHandleAppendEntriesRejectsStaleTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)
	n.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node-2"})

	reply := n.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: "node-3"})

	if reply.Success {
		t.Fatalf("expected AppendEntries rejected for stale term")
	}
	if reply.Term != 5 {
		t.Errorf("reply term = %d, want 5", reply.Term)
	}
}

func TestHandleAppendEntriesAcceptsCurrentTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)

	reply := n.HandleAppendEntries(AppendEntriesArgs{Term: 0, LeaderID: "node-2"})

	if !reply.Success {
		t.Fatalf("expected AppendEntries accepted")
	}
}

func TestHandleAppendEntriesAdoptsHigherTerm(t *testing.T) {
	n := NewNode("node-1", []string{"node-2"}, nil)

	reply := n.HandleAppendEntries(AppendEntriesArgs{Term: 7, LeaderID: "node-2"})

	if !reply.Success {
		t.Fatalf("expected AppendEntries accepted")
	}
	if got := n.Term(); got != 7 {
		t.Errorf("term = %d, want 7", got)
	}
	if got := n.Role(); got != Follower {
		t.Errorf("role = %v, want %v", got, Follower)
	}
}

func TestHandleAppendEntriesCandidateStepsDownOnEqualTerm(t *testing.T) {
	transport := &fakeTransport{
		replies: map[string]RequestVoteReply{
			"node-2": {Term: 1, VoteGranted: false},
			"node-3": {Term: 1, VoteGranted: false},
		},
	}
	n := NewNode("node-1", []string{"node-2", "node-3"}, transport)
	n.StartElection()

	if got := n.Role(); got != Candidate {
		t.Fatalf("precondition failed: role = %v, want %v", got, Candidate)
	}

	reply := n.HandleAppendEntries(AppendEntriesArgs{Term: n.Term(), LeaderID: "node-2"})

	if !reply.Success {
		t.Fatalf("expected AppendEntries accepted")
	}
	if got := n.Role(); got != Follower {
		t.Fatalf("role = %v after AppendEntries at same term, want %v", got, Follower)
	}
}
