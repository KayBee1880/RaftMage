package raft

type StateMachine interface {
	Apply(command []byte) error
}

func (n *Node) SetStateMachine(sm StateMachine) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stateMachine = sm
}

func (n *Node) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

func (n *Node) applyCommittedLocked() {
	if n.lastApplied < n.lastIncludedIndex {
		n.lastApplied = n.lastIncludedIndex
	}
	if n.stateMachine == nil {
		return
	}
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-n.lastIncludedIndex-1]
		if entry.Type == EntryCommand {
			if err := n.stateMachine.Apply(entry.Command); err != nil {
				panic("raft: failed to apply committed entry: " + err.Error())
			}
		}
	}
}
