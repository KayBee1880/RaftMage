package raft

func (n *Node) Propose(command []byte) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, 0, false
	}
	term = n.currentTerm
	n.log = append(n.log, LogEntry{Term: term, Command: command})
	index = n.lastLogIndexLocked()
	n.advanceCommitIndexLocked(term)
	n.mu.Unlock()

	n.sendHeartbeats(term)

	return index, term, true
}
