package raft

import "time"

type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
}

type InstallSnapshotReply struct {
	Term uint64
}

func (n *Node) HandleInstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return InstallSnapshotReply{Term: n.currentTerm}
	}

	if args.Term > n.currentTerm || n.role == Candidate {
		n.becomeFollowerLocked(args.Term)
	}
	n.electionResetAt = time.Now()

	if args.LastIncludedIndex <= n.lastIncludedIndex {
		return InstallSnapshotReply{Term: n.currentTerm}
	}

	if args.LastIncludedIndex <= n.lastLogIndexLocked() && n.logTermAtLocked(args.LastIncludedIndex) == args.LastIncludedTerm {
		n.log = append([]LogEntry(nil), n.log[args.LastIncludedIndex-n.lastIncludedIndex:]...)
	} else {
		n.log = nil
	}

	n.lastIncludedIndex = args.LastIncludedIndex
	n.lastIncludedTerm = args.LastIncludedTerm
	if n.commitIndex < args.LastIncludedIndex {
		n.commitIndex = args.LastIncludedIndex
	}
	n.persistStateLocked()

	return InstallSnapshotReply{Term: n.currentTerm}
}
