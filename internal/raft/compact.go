package raft

import "errors"

var ErrCompactPastCommitIndex = errors.New("raft: cannot compact past commitIndex")

func (n *Node) Compact(upToIndex uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if upToIndex <= n.lastIncludedIndex {
		return nil
	}
	if upToIndex > n.commitIndex {
		return ErrCompactPastCommitIndex
	}

	lastIncludedTerm := n.logTermAtLocked(upToIndex)
	remaining := append([]LogEntry(nil), n.log[upToIndex-n.lastIncludedIndex:]...)

	n.lastIncludedIndex = upToIndex
	n.lastIncludedTerm = lastIncludedTerm
	n.log = remaining
	n.persistStateLocked()

	return nil
}
