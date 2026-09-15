package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raftmage/internal/clientapi"
	"raftmage/internal/clientapi/kvpb"
	"raftmage/internal/kvstore"
	"raftmage/internal/raft"
)

type fakeKVClient struct{}

func (fakeKVClient) Get(context.Context, *kvpb.GetRequest, ...grpc.CallOption) (*kvpb.GetReply, error) {
	return nil, errors.New("fakeKVClient: Get not expected to be called")
}

func (fakeKVClient) Put(context.Context, *kvpb.PutRequest, ...grpc.CallOption) (*kvpb.PutReply, error) {
	return nil, errors.New("fakeKVClient: Put not expected to be called")
}

func (fakeKVClient) Delete(context.Context, *kvpb.DeleteRequest, ...grpc.CallOption) (*kvpb.DeleteReply, error) {
	return nil, errors.New("fakeKVClient: Delete not expected to be called")
}

func TestRunCommandRejectsNoArgs(t *testing.T) {
	var stdout bytes.Buffer
	err := runCommand(context.Background(), fakeKVClient{}, nil, &stdout)
	if err == nil {
		t.Fatal("expected an error for no command, got nil")
	}
}

func TestRunCommandRejectsUnknownCommand(t *testing.T) {
	var stdout bytes.Buffer
	err := runCommand(context.Background(), fakeKVClient{}, []string{"frobnicate", "x"}, &stdout)
	if err == nil {
		t.Fatal("expected an error for an unknown command, got nil")
	}
}

func TestRunCommandRejectsWrongArgCounts(t *testing.T) {
	tests := [][]string{
		{"get"},
		{"get", "x", "y"},
		{"put", "x"},
		{"put", "x", "y", "z"},
		{"delete"},
		{"delete", "x", "y"},
	}
	for _, args := range tests {
		var stdout bytes.Buffer
		if err := runCommand(context.Background(), fakeKVClient{}, args, &stdout); err == nil {
			t.Fatalf("args %v: expected an error for the wrong argument count, got nil", args)
		}
	}
}

func leaderNode(t *testing.T) *raft.Node {
	t.Helper()
	node := raft.NewNode("node-1", nil, nil, nil)
	node.StartElection()
	if node.Role() != raft.Leader {
		t.Fatalf("expected the single-node cluster to elect itself leader")
	}
	return node
}

func followerNode(t *testing.T) *raft.Node {
	t.Helper()
	node := raft.NewNode("node-1", []string{"node-2"}, nil, nil)
	node.HandleAppendEntries(raft.AppendEntriesArgs{Term: 1, LeaderID: "node-2"})
	return node
}

func startClientAPIServer(t *testing.T, node *raft.Node) (client kvpb.KVClient, stop func()) {
	t.Helper()
	store := kvstore.NewStore()
	node.SetStateMachine(store)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	server := clientapi.NewServer(node, store)
	go server.Serve(lis)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	return kvpb.NewKVClient(conn), func() {
		conn.Close()
		server.Stop()
	}
}

func TestRunCommandPutThenGetRoundTripsOverRealGRPC(t *testing.T) {
	client, stop := startClientAPIServer(t, leaderNode(t))
	defer stop()

	var putOut bytes.Buffer
	if err := runCommand(context.Background(), client, []string{"put", "foo", "bar"}, &putOut); err != nil {
		t.Fatalf("put returned error: %v", err)
	}
	if !strings.Contains(putOut.String(), "committed at index") {
		t.Fatalf("put output = %q, want it to mention the committed index", putOut.String())
	}

	var getOut bytes.Buffer
	if err := runCommand(context.Background(), client, []string{"get", "foo"}, &getOut); err != nil {
		t.Fatalf("get returned error: %v", err)
	}
	if strings.TrimSpace(getOut.String()) != "bar" {
		t.Fatalf("get output = %q, want %q", getOut.String(), "bar")
	}
}

func TestRunCommandDeleteRemovesKeyOverRealGRPC(t *testing.T) {
	client, stop := startClientAPIServer(t, leaderNode(t))
	defer stop()

	if err := runCommand(context.Background(), client, []string{"put", "foo", "bar"}, &bytes.Buffer{}); err != nil {
		t.Fatalf("seeding put returned error: %v", err)
	}

	var deleteOut bytes.Buffer
	if err := runCommand(context.Background(), client, []string{"delete", "foo"}, &deleteOut); err != nil {
		t.Fatalf("delete returned error: %v", err)
	}

	err := runCommand(context.Background(), client, []string{"get", "foo"}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected get to fail with NotFound after delete, got nil")
	}
}

func TestKeyForRoutingExtractsKeyFromArgs(t *testing.T) {
	key, err := keyForRouting([]string{"put", "foo", "bar"})
	if err != nil {
		t.Fatalf("keyForRouting returned error: %v", err)
	}
	if key != "foo" {
		t.Fatalf("keyForRouting = %q, want %q", key, "foo")
	}
}

func TestKeyForRoutingRejectsTooFewArgs(t *testing.T) {
	if _, err := keyForRouting([]string{"get"}); err == nil {
		t.Fatal("expected an error for a command with no key, got nil")
	}
	if _, err := keyForRouting(nil); err == nil {
		t.Fatal("expected an error for no args at all, got nil")
	}
}

type erroringKVClient struct{ err error }

func (c erroringKVClient) Get(context.Context, *kvpb.GetRequest, ...grpc.CallOption) (*kvpb.GetReply, error) {
	return nil, c.err
}

func (c erroringKVClient) Put(context.Context, *kvpb.PutRequest, ...grpc.CallOption) (*kvpb.PutReply, error) {
	return nil, c.err
}

func (c erroringKVClient) Delete(context.Context, *kvpb.DeleteRequest, ...grpc.CallOption) (*kvpb.DeleteReply, error) {
	return nil, c.err
}

type succeedingKVClient struct{}

func (succeedingKVClient) Get(context.Context, *kvpb.GetRequest, ...grpc.CallOption) (*kvpb.GetReply, error) {
	return &kvpb.GetReply{Value: []byte("ok")}, nil
}

func (succeedingKVClient) Put(context.Context, *kvpb.PutRequest, ...grpc.CallOption) (*kvpb.PutReply, error) {
	return &kvpb.PutReply{Index: 1}, nil
}

func (succeedingKVClient) Delete(context.Context, *kvpb.DeleteRequest, ...grpc.CallOption) (*kvpb.DeleteReply, error) {
	return &kvpb.DeleteReply{Index: 1}, nil
}

func TestTryReplicasReturnsNilOnFirstSuccess(t *testing.T) {
	clients := []kvpb.KVClient{succeedingKVClient{}}
	if err := tryReplicas(context.Background(), clients, []string{"get", "foo"}, &bytes.Buffer{}); err != nil {
		t.Fatalf("tryReplicas returned error: %v", err)
	}
}

func TestTryReplicasSkipsFailingReplicasUntilSuccess(t *testing.T) {
	clients := []kvpb.KVClient{
		erroringKVClient{err: errors.New("not the leader, try \"node-2\"")},
		erroringKVClient{err: errors.New("not the leader, try \"node-3\"")},
		succeedingKVClient{},
	}
	if err := tryReplicas(context.Background(), clients, []string{"get", "foo"}, &bytes.Buffer{}); err != nil {
		t.Fatalf("tryReplicas returned error: %v", err)
	}
}

func TestTryReplicasReturnsLastErrorWhenAllFail(t *testing.T) {
	wantErr := errors.New("replica 2 failed")
	clients := []kvpb.KVClient{
		erroringKVClient{err: errors.New("replica 1 failed")},
		erroringKVClient{err: wantErr},
	}
	err := tryReplicas(context.Background(), clients, []string{"get", "foo"}, &bytes.Buffer{})
	if err == nil || err.Error() != wantErr.Error() {
		t.Fatalf("tryReplicas error = %v, want %v (the last replica's own error)", err, wantErr)
	}
}

func TestTryReplicasRejectsEmptyClientList(t *testing.T) {
	if err := tryReplicas(context.Background(), nil, []string{"get", "foo"}, &bytes.Buffer{}); err == nil {
		t.Fatal("expected an error for an empty replica list, got nil")
	}
}

func TestTryReplicasSucceedsAgainstLeaderAfterFollowerRejectsOverRealGRPC(t *testing.T) {
	followerClient, stopFollower := startClientAPIServer(t, followerNode(t))
	defer stopFollower()
	leaderClient, stopLeader := startClientAPIServer(t, leaderNode(t))
	defer stopLeader()

	var stdout bytes.Buffer
	err := tryReplicas(context.Background(), []kvpb.KVClient{followerClient, leaderClient}, []string{"put", "foo", "bar"}, &stdout)
	if err != nil {
		t.Fatalf("tryReplicas returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "committed at index") {
		t.Fatalf("stdout = %q, want it to mention the committed index", stdout.String())
	}
}
