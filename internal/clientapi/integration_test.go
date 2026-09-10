package clientapi

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raftmage/internal/clientapi/kvpb"
	"raftmage/internal/kvstore"
	"raftmage/internal/raft"
	"raftmage/internal/transport"
)

func waitForAnyLeader(t *testing.T, nodes []*raft.Node, timeout time.Duration) *raft.Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.Role() == raft.Leader {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

func TestThreeNodeClusterCommitsPutOverRealGRPCClientAPI(t *testing.T) {
	ids := []string{"node-1", "node-2", "node-3"}
	peerAddrs := make(map[string]string)
	peerListeners := make(map[string]net.Listener)
	for _, id := range ids {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to listen for %s: %v", id, err)
		}
		peerListeners[id] = lis
		peerAddrs[id] = lis.Addr().String()
	}

	var nodes []*raft.Node
	var peerServers []*grpc.Server
	var peerTransports []*transport.GRPCTransport
	stores := make(map[string]*kvstore.Store)
	clientAddrs := make(map[string]string)
	var clientServers []*grpc.Server

	for _, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		tr := transport.NewGRPCTransport(peerAddrs)
		node := raft.NewNode(id, peers, tr, nil)
		peerServer := transport.NewServer(node)
		go peerServer.Serve(peerListeners[id])

		store := kvstore.NewStore()
		node.SetStateMachine(store)
		stores[id] = store

		clientLis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to listen for %s's client API: %v", id, err)
		}
		clientServer := NewServer(node, store)
		go clientServer.Serve(clientLis)
		clientAddrs[id] = clientLis.Addr().String()

		nodes = append(nodes, node)
		peerServers = append(peerServers, peerServer)
		peerTransports = append(peerTransports, tr)
		clientServers = append(clientServers, clientServer)
	}

	for _, n := range nodes {
		n.Run()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
		for _, s := range peerServers {
			s.Stop()
		}
		for _, s := range clientServers {
			s.Stop()
		}
		for _, tr := range peerTransports {
			tr.Close()
		}
	}()

	leader := waitForAnyLeader(t, nodes, 5*time.Second)

	var leaderID string
	for i, n := range nodes {
		if n == leader {
			leaderID = ids[i]
			break
		}
	}
	if leaderID == "" {
		t.Fatalf("could not map the elected leader node back to its ID")
	}

	conn, err := grpc.NewClient(clientAddrs[leaderID], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial the leader's client API: %v", err)
	}
	defer conn.Close()
	client := kvpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Put(ctx, &kvpb.PutRequest{Key: "x", Value: []byte("1")}); err != nil {
		t.Fatalf("Put over real gRPC client API returned error: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		allCaughtUp := true
		for _, id := range ids {
			value, ok := stores[id].Get("x")
			if !ok || string(value) != "1" {
				allCaughtUp = false
				break
			}
		}
		if allCaughtUp {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("not every node's local store observed the committed write within timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
