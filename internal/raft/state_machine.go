package raft

type StateMachine interface {
	Apply(command []byte) error
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

func (n *Node) SetStateMachine(sm StateMachine) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stateMachine = sm
	n.applyCommittedLocked()
}

func (n *Node) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

func (n *Node) applyCommittedLocked() {
	if n.lastApplied < n.lastIncludedIndex {
		if n.stateMachine != nil {
			if err := n.stateMachine.Restore(n.stateMachineSnapshot); err != nil {
				panic("raft: failed to restore state machine snapshot: " + err.Error())
			}
		}
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
