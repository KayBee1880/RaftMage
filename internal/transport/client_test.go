package transport

import (
	"net"
	"testing"

	"raftmage/internal/raft"
)

func startTestServer(t *testing.T, node *raft.Node) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	server := NewServer(node)
	go server.Serve(lis)
	return lis.Addr().String(), server.Stop
}

func TestGRPCTransportSendRequestVoteRoundTrips(t *testing.T) {
	node := raft.NewNode("node-1", nil, nil, nil)
	addr, stop := startTestServer(t, node)
	defer stop()

	transport := NewGRPCTransport(map[string]string{"node-1": addr})
	defer transport.Close()

	reply, err := transport.SendRequestVote("node-1", raft.RequestVoteArgs{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("SendRequestVote returned error: %v", err)
	}
	if !reply.VoteGranted {
		t.Fatalf("expected vote granted, got denied: %+v", reply)
	}
	if reply.Term != 1 {
		t.Fatalf("expected reply term 1, got %d", reply.Term)
	}
}

func TestGRPCTransportSendAppendEntriesRoundTrips(t *testing.T) {
	node := raft.NewNode("node-1", nil, nil, nil)
	// Make node-1 a term-1 follower first, the same way a real leader's heartbeat would.
	node.HandleRequestVote(raft.RequestVoteArgs{Term: 1, CandidateID: "node-2"})

	addr, stop := startTestServer(t, node)
	defer stop()

	transport := NewGRPCTransport(map[string]string{"node-1": addr})
	defer transport.Close()

	reply, err := transport.SendAppendEntries("node-1", raft.AppendEntriesArgs{
		Term:     1,
		LeaderID: "node-2",
		Entries: []raft.LogEntry{
			{Term: 1, Command: []byte("set x=1")},
		},
	})
	if err != nil {
		t.Fatalf("SendAppendEntries returned error: %v", err)
	}
	if !reply.Success {
		t.Fatalf("expected success, got rejected: %+v", reply)
	}
	if reply.Term != 1 {
		t.Fatalf("expected reply term 1, got %d", reply.Term)
	}
}

func TestGRPCTransportSendInstallSnapshotRoundTrips(t *testing.T) {
	node := raft.NewNode("node-1", nil, nil, nil)
	node.HandleRequestVote(raft.RequestVoteArgs{Term: 1, CandidateID: "node-2"})

	addr, stop := startTestServer(t, node)
	defer stop()

	transport := NewGRPCTransport(map[string]string{"node-1": addr})
	defer transport.Close()

	reply, err := transport.SendInstallSnapshot("node-1", raft.InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "node-2",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
	})
	if err != nil {
		t.Fatalf("SendInstallSnapshot returned error: %v", err)
	}
	if reply.Term != 1 {
		t.Fatalf("expected reply term 1, got %d", reply.Term)
	}
	if node.CommitIndex() != 5 {
		t.Fatalf("expected commit index 5 after InstallSnapshot, got %d", node.CommitIndex())
	}
}

func TestGRPCTransportReturnsErrorForUnknownPeer(t *testing.T) {
	transport := NewGRPCTransport(map[string]string{})
	defer transport.Close()

	if _, err := transport.SendRequestVote("ghost", raft.RequestVoteArgs{Term: 1}); err == nil {
		t.Fatal("expected an error for a peer with no known address, got nil")
	}
}

func TestGRPCTransportReturnsErrorWhenPeerUnreachable(t *testing.T) {
	// Reserve a real local port, then close it immediately so nothing is listening there.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	transport := NewGRPCTransport(map[string]string{"ghost": addr})
	defer transport.Close()

	if _, err := transport.SendRequestVote("ghost", raft.RequestVoteArgs{Term: 1}); err == nil {
		t.Fatal("expected an error for an unreachable peer, got nil")
	}
}
