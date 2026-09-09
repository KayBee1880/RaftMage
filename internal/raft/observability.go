package raft

import "log/slog"

type Metrics struct {
	ElectionsStarted   uint64
	ElectionsWon       uint64
	VotesGranted       uint64
	VotesDenied        uint64
	EntriesProposed    uint64
	EntriesCommitted   uint64
	LogCompactions     uint64
	SnapshotsInstalled uint64
	MembershipChanges  uint64
}

func (n *Node) SetLogger(logger *slog.Logger) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.logger = logger
}

func (n *Node) Metrics() Metrics {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.metrics
}

func (n *Node) logLocked(msg string, args ...any) {
	if n.logger == nil {
		return
	}
	base := []any{"node", n.id, "term", n.currentTerm, "role", n.role.String()}
	n.logger.Info(msg, append(base, args...)...)
}
