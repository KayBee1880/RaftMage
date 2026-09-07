package transport

import (
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"raftmage/internal/raft"
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

func TestThreeNodeClusterElectsLeaderAndReplicatesOverRealGRPC(t *testing.T) {
	ids := []string{"node-1", "node-2", "node-3"}
	addrs := make(map[string]string)
	listeners := make(map[string]net.Listener)
	for _, id := range ids {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to listen for %s: %v", id, err)
		}
		listeners[id] = lis
		addrs[id] = lis.Addr().String()
	}

	var nodes []*raft.Node
	var servers []*grpc.Server
	var transports []*GRPCTransport

	for _, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		tr := NewGRPCTransport(addrs)
		node := raft.NewNode(id, peers, tr, nil)
		server := NewServer(node)
		go server.Serve(listeners[id])

		nodes = append(nodes, node)
		servers = append(servers, server)
		transports = append(transports, tr)
	}

	for _, n := range nodes {
		n.Run()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
		for _, s := range servers {
			s.Stop()
		}
		for _, tr := range transports {
			tr.Close()
		}
	}()

	leader := waitForAnyLeader(t, nodes, 5*time.Second)

	if _, _, isLeader := leader.Propose([]byte("set x=1")); !isLeader {
		t.Fatalf("Propose on the elected leader reported isLeader = false")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		allCommitted := true
		for _, n := range nodes {
			if n.CommitIndex() != 1 {
				allCommitted = false
				break
			}
		}
		if allCommitted {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("not every node committed the leader's entry over real gRPC within timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
