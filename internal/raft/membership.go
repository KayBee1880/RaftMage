package raft

func (n *Node) AddServer(id string) (index, term uint64, ok bool) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, 0, false
	}
	if n.hasPendingConfigChangeLocked() {
		n.mu.Unlock()
		return 0, 0, false
	}
	full := n.currentFullConfigLocked()
	if containsString(full, id) {
		n.mu.Unlock()
		return 0, 0, false
	}
	newConfig := append(append([]string(nil), full...), id)
	index, term = n.appendConfigEntryLocked(newConfig)
	n.mu.Unlock()

	n.sendHeartbeats(term)
	return index, term, true
}

func (n *Node) RemoveServer(id string) (index, term uint64, ok bool) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, 0, false
	}
	if n.hasPendingConfigChangeLocked() {
		n.mu.Unlock()
		return 0, 0, false
	}
	full := n.currentFullConfigLocked()
	if !containsString(full, id) {
		n.mu.Unlock()
		return 0, 0, false
	}
	newConfig := filterOut(full, id)
	index, term = n.appendConfigEntryLocked(newConfig)
	n.mu.Unlock()

	n.sendHeartbeats(term)
	return index, term, true
}

func (n *Node) appendConfigEntryLocked(config []string) (index, term uint64) {
	term = n.currentTerm
	n.log = append(n.log, LogEntry{Term: term, Type: EntryConfig, Config: config})
	index = n.lastLogIndexLocked()
	n.persistStateLocked()
	n.reconcileLeaderStateForConfigChangeLocked()
	n.metrics.MembershipChanges++
	n.logLocked("proposed membership change", "config", config)
	n.advanceCommitIndexLocked(term)
	return index, term
}

func (n *Node) hasPendingConfigChangeLocked() bool {
	for i := n.commitIndex + 1; i <= n.lastLogIndexLocked(); i++ {
		if n.log[i-n.lastIncludedIndex-1].Type == EntryConfig {
			return true
		}
	}
	return false
}
