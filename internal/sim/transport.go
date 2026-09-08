package sim

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"raftmage/internal/raft"
)

var (
	ErrUnknownPeer     = errors.New("sim: unknown peer")
	ErrPeerUnreachable = errors.New("sim: peer unreachable")
)

type Config struct {
	Seed            int64
	MinDelay        time.Duration
	MaxDelay        time.Duration
	DropProbability float64
}

type SimTransport struct {
	mu       sync.Mutex
	rng      *rand.Rand
	cfg      Config
	nodes    map[string]*raft.Node
	isolated map[string]bool
}

func NewSimTransport(cfg Config) *SimTransport {
	return &SimTransport{
		rng:      rand.New(rand.NewSource(cfg.Seed)),
		cfg:      cfg,
		nodes:    make(map[string]*raft.Node),
		isolated: make(map[string]bool),
	}
}

func (st *SimTransport) Register(id string, n *raft.Node) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.nodes[id] = n
}

func (st *SimTransport) Isolate(id string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.isolated[id] = true
}

func (st *SimTransport) Restore(id string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.isolated, id)
}

func (st *SimTransport) resolve(peer string) (*raft.Node, time.Duration, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	node, ok := st.nodes[peer]
	if !ok {
		return nil, 0, ErrUnknownPeer
	}

	delay := st.cfg.MinDelay
	if span := st.cfg.MaxDelay - st.cfg.MinDelay; span > 0 {
		delay += time.Duration(st.rng.Int63n(int64(span)))
	}

	if st.isolated[peer] || st.rng.Float64() < st.cfg.DropProbability {
		return nil, delay, ErrPeerUnreachable
	}

	return node, delay, nil
}

func (st *SimTransport) SendRequestVote(peer string, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	node, delay, err := st.resolve(peer)
	time.Sleep(delay)
	if err != nil {
		return raft.RequestVoteReply{}, err
	}
	return node.HandleRequestVote(args), nil
}

func (st *SimTransport) SendAppendEntries(peer string, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	node, delay, err := st.resolve(peer)
	time.Sleep(delay)
	if err != nil {
		return raft.AppendEntriesReply{}, err
	}
	return node.HandleAppendEntries(args), nil
}

func (st *SimTransport) SendInstallSnapshot(peer string, args raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	node, delay, err := st.resolve(peer)
	time.Sleep(delay)
	if err != nil {
		return raft.InstallSnapshotReply{}, err
	}
	return node.HandleInstallSnapshot(args), nil
}
