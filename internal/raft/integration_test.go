package raft

import (
	"errors"
	"sync"
	"testing"
	"time"

	"raftmage/internal/kvstore"
)

type loopbackTransport struct {
	mu           sync.Mutex
	nodes        map[string]*Node
	disconnected map[string]bool
}

func newLoopbackTransport() *loopbackTransport {
	return &loopbackTransport{
		nodes:        make(map[string]*Node),
		disconnected: make(map[string]bool),
	}
}

func (lt *loopbackTransport) register(n *Node) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.nodes[n.id] = n
}

func (lt *loopbackTransport) disconnect(peer string) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.disconnected[peer] = true
}

func (lt *loopbackTransport) reconnect(peer string) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	delete(lt.disconnected, peer)
}

func (lt *loopbackTransport) SendRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	lt.mu.Lock()
	n, ok := lt.nodes[peer]
	unreachable := lt.disconnected[peer]
	lt.mu.Unlock()
	if unreachable {
		return RequestVoteReply{}, errors.New("peer unreachable: " + peer)
	}
	if !ok {
		return RequestVoteReply{}, errors.New("unknown peer: " + peer)
	}
	return n.HandleRequestVote(args), nil
}

func (lt *loopbackTransport) SendAppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	lt.mu.Lock()
	n, ok := lt.nodes[peer]
	unreachable := lt.disconnected[peer]
	lt.mu.Unlock()
	if unreachable {
		return AppendEntriesReply{}, errors.New("peer unreachable: " + peer)
	}
	if !ok {
		return AppendEntriesReply{}, errors.New("unknown peer: " + peer)
	}
	return n.HandleAppendEntries(args), nil
}

func (lt *loopbackTransport) SendInstallSnapshot(peer string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	lt.mu.Lock()
	n, ok := lt.nodes[peer]
	unreachable := lt.disconnected[peer]
	lt.mu.Unlock()
	if unreachable {
		return InstallSnapshotReply{}, errors.New("peer unreachable: " + peer)
	}
	if !ok {
		return InstallSnapshotReply{}, errors.New("unknown peer: " + peer)
	}
	return n.HandleInstallSnapshot(args), nil
}

func waitForAnyLeader(t *testing.T, nodes []*Node, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.Role() == Leader {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

func TestLeaderHeartbeatsPreventFollowerReelection(t *testing.T) {
	transport := newLoopbackTransport()
	n1 := NewNode("node-1", []string{"node-2", "node-3"}, transport, nil)
	n2 := NewNode("node-2", []string{"node-1", "node-3"}, transport, nil)
	n3 := NewNode("node-3", []string{"node-1", "node-2"}, transport, nil)
	nodes := []*Node{n1, n2, n3}

	for _, n := range nodes {
		transport.register(n)
	}
	for _, n := range nodes {
		n.Run()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	leader := waitForAnyLeader(t, nodes, 2*time.Second)
	leaderID := leader.id
	leaderTerm := leader.Term()

	time.Sleep(electionTimeoutMax * 3)

	if got := leader.Role(); got != Leader {
		t.Fatalf("leader %s stepped down unexpectedly: role = %v", leaderID, got)
	}
	if got := leader.Term(); got != leaderTerm {
		t.Fatalf("leader %s term changed from %d to %d — re-election happened despite heartbeats", leaderID, leaderTerm, got)
	}
}

func TestLeaderReplicatesAndCommitsAcrossRealNodes(t *testing.T) {
	transport := newLoopbackTransport()
	n1 := NewNode("node-1", []string{"node-2", "node-3"}, transport, nil)
	n2 := NewNode("node-2", []string{"node-1", "node-3"}, transport, nil)
	n3 := NewNode("node-3", []string{"node-1", "node-2"}, transport, nil)
	nodes := []*Node{n1, n2, n3}

	for _, n := range nodes {
		transport.register(n)
	}
	for _, n := range nodes {
		n.Run()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	leader := waitForAnyLeader(t, nodes, 2*time.Second)

	if _, _, isLeader := leader.Propose([]byte("set x=1")); !isLeader {
		t.Fatalf("Propose on the elected leader reported isLeader = false")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		allCaughtUp := true
		for _, n := range nodes {
			n.mu.Lock()
			caughtUp := len(n.log) == 1 &&
				string(n.log[0].Command) == "set x=1" &&
				n.commitIndex == 1
			n.mu.Unlock()
			if !caughtUp {
				allCaughtUp = false
				break
			}
		}
		if allCaughtUp {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("not every node replicated and committed the leader's entry within timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLeaderCompactsLogSafelyWithoutStrandingFollowers(t *testing.T) {
	transport := newLoopbackTransport()
	n1 := NewNode("node-1", []string{"node-2", "node-3"}, transport, nil)
	n2 := NewNode("node-2", []string{"node-1", "node-3"}, transport, nil)
	n3 := NewNode("node-3", []string{"node-1", "node-2"}, transport, nil)
	nodes := []*Node{n1, n2, n3}

	for _, n := range nodes {
		transport.register(n)
	}
	for _, n := range nodes {
		n.Run()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	leader := waitForAnyLeader(t, nodes, 2*time.Second)

	for i := 0; i < 3; i++ {
		if _, _, isLeader := leader.Propose([]byte("entry")); !isLeader {
			t.Fatalf("Propose #%d on the elected leader reported isLeader = false", i)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		allCommitted := true
		for _, n := range nodes {
			n.mu.Lock()
			committed := n.commitIndex == 3
			n.mu.Unlock()
			if !committed {
				allCommitted = false
				break
			}
		}
		if allCommitted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("not every node committed all 3 entries within timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := leader.Compact(2); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	if _, _, isLeader := leader.Propose([]byte("post-compaction entry")); !isLeader {
		t.Fatalf("Propose after compaction reported isLeader = false")
	}

	deadline = time.Now().Add(2 * time.Second)
	for {
		allCaughtUp := true
		for _, n := range nodes {
			n.mu.Lock()
			caughtUp := n.commitIndex == 4 && n.lastLogIndexLocked() == 4
			n.mu.Unlock()
			if !caughtUp {
				allCaughtUp = false
				break
			}
		}
		if allCaughtUp {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("not every node caught up on the post-compaction entry within timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLeaderInstallsSnapshotToCatchUpFarBehindFollower(t *testing.T) {
	transport := newLoopbackTransport()
	n1 := NewNode("node-1", []string{"node-2", "node-3"}, transport, nil)
	n2 := NewNode("node-2", []string{"node-1", "node-3"}, transport, nil)
	n3 := NewNode("node-3", []string{"node-1", "node-2"}, transport, nil)
	nodes := []*Node{n1, n2, n3}

	for _, n := range nodes {
		transport.register(n)
	}
	for _, n := range nodes {
		n.Run()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	leader := waitForAnyLeader(t, nodes, 2*time.Second)

	var behind *Node
	for _, n := range nodes {
		if n != leader {
			behind = n
			break
		}
	}

	transport.disconnect(behind.id)
	defer transport.reconnect(behind.id)

	for i := 0; i < 3; i++ {
		if _, _, isLeader := leader.Propose([]byte("entry")); !isLeader {
			t.Fatalf("Propose #%d on the elected leader reported isLeader = false", i)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		allCommitted := true
		for _, n := range nodes {
			if n == behind {
				continue
			}
			n.mu.Lock()
			committed := n.commitIndex == 3
			n.mu.Unlock()
			if !committed {
				allCommitted = false
				break
			}
		}
		if allCommitted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the reachable nodes did not commit all 3 entries within timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := leader.Compact(3); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	transport.reconnect(behind.id)

	if _, _, isLeader := leader.Propose([]byte("after reconnect")); !isLeader {
		t.Fatalf("Propose after reconnect reported isLeader = false")
	}

	deadline = time.Now().Add(2 * time.Second)
	for {
		allCaughtUp := true
		for _, n := range nodes {
			n.mu.Lock()
			caughtUp := n.commitIndex == 4 && n.lastLogIndexLocked() == 4
			n.mu.Unlock()
			if !caughtUp {
				allCaughtUp = false
				break
			}
		}
		if allCaughtUp {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the previously far behind follower did not catch up via InstallSnapshot within timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLeaderAddsServerAndReplicatesToNewMember(t *testing.T) {
	transport := newLoopbackTransport()
	n1 := NewNode("node-1", []string{"node-2", "node-3"}, transport, nil)
	n2 := NewNode("node-2", []string{"node-1", "node-3"}, transport, nil)
	n3 := NewNode("node-3", []string{"node-1", "node-2"}, transport, nil)
	nodes := []*Node{n1, n2, n3}

	for _, n := range nodes {
		transport.register(n)
	}
	for _, n := range nodes {
		n.Run()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	leader := waitForAnyLeader(t, nodes, 2*time.Second)

	n4 := NewNode("node-4", nil, transport, nil)
	transport.register(n4)
	n4.Run()
	defer n4.Stop()

	if _, _, ok := leader.AddServer("node-4"); !ok {
		t.Fatalf("AddServer on the elected leader reported ok = false")
	}

	if _, _, isLeader := leader.Propose([]byte("entry")); !isLeader {
		t.Fatalf("Propose after AddServer reported isLeader = false")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		n4.mu.Lock()
		caughtUp := n4.commitIndex >= 2
		n4.mu.Unlock()
		if caughtUp {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the newly added node-4 did not catch up on replication within timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLeaderRemovesFollowerAndContinuesOperating(t *testing.T) {
	transport := newLoopbackTransport()
	n1 := NewNode("node-1", []string{"node-2", "node-3"}, transport, nil)
	n2 := NewNode("node-2", []string{"node-1", "node-3"}, transport, nil)
	n3 := NewNode("node-3", []string{"node-1", "node-2"}, transport, nil)
	nodes := []*Node{n1, n2, n3}

	for _, n := range nodes {
		transport.register(n)
	}
	for _, n := range nodes {
		n.Run()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	leader := waitForAnyLeader(t, nodes, 2*time.Second)

	var removed, remaining *Node
	for _, n := range nodes {
		if n != leader {
			if removed == nil {
				removed = n
			} else {
				remaining = n
			}
		}
	}

	if _, _, ok := leader.RemoveServer(removed.id); !ok {
		t.Fatalf("RemoveServer on the elected leader reported ok = false")
	}
	// A real deployment stops the removed process once it's out of the config;
	// this project's Raft core doesn't manage process lifecycle, so the test
	// does the equivalent here to avoid the removed node's own election timer
	// starting a disruptive, doomed-to-lose election once heartbeats stop.
	removed.Stop()

	if _, _, isLeader := leader.Propose([]byte("entry")); !isLeader {
		t.Fatalf("Propose after RemoveServer reported isLeader = false")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		leader.mu.Lock()
		leaderCaughtUp := leader.commitIndex == 2
		leader.mu.Unlock()
		remaining.mu.Lock()
		remainingCaughtUp := remaining.commitIndex == 2
		remaining.mu.Unlock()
		if leaderCaughtUp && remainingCaughtUp {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the leader and remaining follower did not both commit within timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}

	leader.mu.Lock()
	got := leader.currentConfigLocked()
	leader.mu.Unlock()
	if len(got) != 1 || got[0] != remaining.id {
		t.Fatalf("leader's currentConfigLocked() = %v, want [%s]", got, remaining.id)
	}
}

func TestLeaderAppliesProposedEntriesToStateMachineAcrossRealNodes(t *testing.T) {
	transport := newLoopbackTransport()
	n1 := NewNode("node-1", []string{"node-2", "node-3"}, transport, nil)
	n2 := NewNode("node-2", []string{"node-1", "node-3"}, transport, nil)
	n3 := NewNode("node-3", []string{"node-1", "node-2"}, transport, nil)
	nodes := []*Node{n1, n2, n3}

	stores := make([]*kvstore.Store, len(nodes))
	for i, n := range nodes {
		store := kvstore.NewStore()
		n.SetStateMachine(store)
		stores[i] = store
	}

	for _, n := range nodes {
		transport.register(n)
	}
	for _, n := range nodes {
		n.Run()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	leader := waitForAnyLeader(t, nodes, 2*time.Second)

	command, err := kvstore.EncodeCommand(kvstore.Command{Op: kvstore.OpPut, Key: "x", Value: []byte("1")})
	if err != nil {
		t.Fatalf("EncodeCommand failed: %v", err)
	}
	if _, _, isLeader := leader.Propose(command); !isLeader {
		t.Fatalf("Propose on the elected leader reported isLeader = false")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		allApplied := true
		for _, store := range stores {
			value, ok := store.Get("x")
			if !ok || string(value) != "1" {
				allApplied = false
				break
			}
		}
		if allApplied {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("not every node's state machine reflected the committed write within timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
