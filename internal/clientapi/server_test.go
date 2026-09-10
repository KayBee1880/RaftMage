package clientapi

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"raftmage/internal/clientapi/kvpb"
	"raftmage/internal/kvstore"
	"raftmage/internal/raft"
)

func startTestServer(t *testing.T, node *raft.Node, store Getter) (client kvpb.KVClient, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	server := NewServer(node, store)
	go server.Serve(lis)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	stop = func() {
		conn.Close()
		server.Stop()
	}
	return kvpb.NewKVClient(conn), stop
}

func leaderNode(t *testing.T) *raft.Node {
	t.Helper()
	n := raft.NewNode("node-1", nil, &fakeTransport{}, nil)
	n.StartElection()
	if n.Role() != raft.Leader {
		t.Fatalf("expected the single-node cluster to elect itself leader")
	}
	return n
}

func TestGetReturnsStoredValue(t *testing.T) {
	store := kvstore.NewStore()
	cmd, err := kvstore.EncodeCommand(kvstore.Command{Op: kvstore.OpPut, Key: "x", Value: []byte("1")})
	if err != nil {
		t.Fatalf("EncodeCommand failed: %v", err)
	}
	if err := store.Apply(cmd); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	client, stop := startTestServer(t, raft.NewNode("node-1", nil, nil, nil), store)
	defer stop()

	reply, err := client.Get(context.Background(), &kvpb.GetRequest{Key: "x"})
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if string(reply.GetValue()) != "1" {
		t.Fatalf("Get value = %q, want %q", reply.GetValue(), "1")
	}
}

func TestGetReturnsNotFoundForMissingKey(t *testing.T) {
	store := kvstore.NewStore()
	client, stop := startTestServer(t, raft.NewNode("node-1", nil, nil, nil), store)
	defer stop()

	_, err := client.Get(context.Background(), &kvpb.GetRequest{Key: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("Get error = %v, want NotFound", err)
	}
}

func TestPutOnLeaderCommitsAndReturnsIndex(t *testing.T) {
	store := kvstore.NewStore()
	node := leaderNode(t)
	node.SetStateMachine(store)

	client, stop := startTestServer(t, node, store)
	defer stop()

	reply, err := client.Put(context.Background(), &kvpb.PutRequest{Key: "x", Value: []byte("1")})
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if reply.GetIndex() != 1 {
		t.Fatalf("Put index = %d, want 1", reply.GetIndex())
	}
	if got, ok := store.Get("x"); !ok || string(got) != "1" {
		t.Fatalf("store.Get(\"x\") = (%q, %v), want (\"1\", true)", got, ok)
	}
}

func TestDeleteOnLeaderCommitsAndRemovesKey(t *testing.T) {
	store := kvstore.NewStore()
	node := leaderNode(t)
	node.SetStateMachine(store)

	client, stop := startTestServer(t, node, store)
	defer stop()

	putCmd, _ := kvstore.EncodeCommand(kvstore.Command{Op: kvstore.OpPut, Key: "x", Value: []byte("1")})
	if _, _, ok := node.Propose(putCmd); !ok {
		t.Fatalf("expected the seeding Propose to succeed on the leader")
	}

	reply, err := client.Delete(context.Background(), &kvpb.DeleteRequest{Key: "x"})
	if err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if reply.GetIndex() != 2 {
		t.Fatalf("Delete index = %d, want 2", reply.GetIndex())
	}
	if _, ok := store.Get("x"); ok {
		t.Fatalf("expected key %q to be gone after Delete", "x")
	}
}

func TestPutOnFollowerReturnsFailedPreconditionWithLeaderHint(t *testing.T) {
	store := kvstore.NewStore()
	node := raft.NewNode("node-1", []string{"node-2"}, nil, nil)
	node.HandleAppendEntries(raft.AppendEntriesArgs{Term: 1, LeaderID: "node-2"})

	client, stop := startTestServer(t, node, store)
	defer stop()

	_, err := client.Put(context.Background(), &kvpb.PutRequest{Key: "x", Value: []byte("1")})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Put error = %v, want FailedPrecondition", err)
	}
	if want := "node-2"; !strings.Contains(status.Convert(err).Message(), want) {
		t.Fatalf("error message %q does not mention leader hint %q", status.Convert(err).Message(), want)
	}
}

func TestPutReturnsDeadlineExceededWhenContextExpiresBeforeCommit(t *testing.T) {
	store := kvstore.NewStore()
	// A two-node cluster where the peer always votes but never acknowledges
	// AppendEntries: the leader can never reach a majority for a new entry,
	// so it never commits, letting proposeAndWait observe a real deadline.
	node := raft.NewNode("node-1", []string{"node-2"}, &voteOnlyTransport{}, nil)
	node.StartElection()
	if node.Role() != raft.Leader {
		t.Fatalf("expected node-1 to win the election against a voting-only peer")
	}
	node.SetStateMachine(store)

	client, stop := startTestServer(t, node, store)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := client.Put(ctx, &kvpb.PutRequest{Key: "x", Value: []byte("1")})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Put error = %v, want DeadlineExceeded", err)
	}
}

type fakeTransport struct{}

func (fakeTransport) SendRequestVote(string, raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	return raft.RequestVoteReply{Term: 1, VoteGranted: true}, nil
}

func (fakeTransport) SendAppendEntries(string, raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	return raft.AppendEntriesReply{Term: 1, Success: true}, nil
}

func (fakeTransport) SendInstallSnapshot(string, raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	return raft.InstallSnapshotReply{Term: 1}, nil
}

// voteOnlyTransport grants every vote request but fails every AppendEntries
// and InstallSnapshot call, simulating a peer that is reachable for
// elections but unreachable for replication.
type voteOnlyTransport struct{}

func (voteOnlyTransport) SendRequestVote(string, raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	return raft.RequestVoteReply{Term: 1, VoteGranted: true}, nil
}

func (voteOnlyTransport) SendAppendEntries(string, raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	return raft.AppendEntriesReply{}, errUnreachablePeer
}

func (voteOnlyTransport) SendInstallSnapshot(string, raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	return raft.InstallSnapshotReply{}, errUnreachablePeer
}

var errUnreachablePeer = errors.New("simulated: peer unreachable for replication")
