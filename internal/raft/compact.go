package raft

import "errors"

var ErrCompactPastCommitIndex = errors.New("raft: cannot compact past commitIndex")
var ErrCompactPastFollowerMatchIndex = errors.New("raft: cannot compact past a follower's matchIndex")

func (n *Node) Compact(upToIndex uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if upToIndex <= n.lastIncludedIndex {
		return nil
	}
	if upToIndex > n.commitIndex {
		return ErrCompactPastCommitIndex
	}
	if n.role == Leader {
		for _, peer := range n.peers {
			if n.matchIndex[peer] < upToIndex {
				return ErrCompactPastFollowerMatchIndex
			}
		}
	}

	lastIncludedTerm := n.logTermAtLocked(upToIndex)
	remaining := append([]LogEntry(nil), n.log[upToIndex-n.lastIncludedIndex:]...)

	n.lastIncludedIndex = upToIndex
	n.lastIncludedTerm = lastIncludedTerm
	n.log = remaining
	n.persistStateLocked()

	return nil
}
