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
	trimmed := n.log[:upToIndex-n.lastIncludedIndex]
	remaining := append([]LogEntry(nil), n.log[upToIndex-n.lastIncludedIndex:]...)

	for i := len(trimmed) - 1; i >= 0; i-- {
		if trimmed[i].Type == EntryConfig {
			n.baseConfig = filterOut(trimmed[i].Config, n.id)
			break
		}
	}

	n.lastIncludedIndex = upToIndex
	n.lastIncludedTerm = lastIncludedTerm
	n.log = remaining
	n.persistStateLocked()
	n.metrics.LogCompactions++
	n.logLocked("compacted log", "upToIndex", upToIndex)

	return nil
}
