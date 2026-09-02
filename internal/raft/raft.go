package raft

import (
	"context"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type LogEntry struct {
	Term    uint64
	Command []byte
}

type Node struct {
	mu sync.Mutex

	id        string
	peers     []string
	transport Transport

	role Role

	currentTerm uint64
	votedFor    string
	log         []LogEntry

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	electionResetAt time.Time
	running         bool
	ctx             context.Context
	cancel          context.CancelFunc
}

func NewNode(id string, peers []string, transport Transport) *Node {
	ctx, cancel := context.WithCancel(context.Background())
	return &Node{
		id:              id,
		peers:           peers,
		transport:       transport,
		role:            Follower,
		electionResetAt: time.Now(),
		ctx:             ctx,
		cancel:          cancel,
	}
}

func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

func (n *Node) VotedFor() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.votedFor
}

func (n *Node) lastLogIndexLocked() uint64 {
	return uint64(len(n.log))
}

func (n *Node) lastLogTermLocked() uint64 {
	return n.logTermAtLocked(n.lastLogIndexLocked())
}

func (n *Node) logTermAtLocked(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	return n.log[index-1].Term
}
