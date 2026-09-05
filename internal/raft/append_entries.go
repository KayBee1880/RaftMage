package raft

import "time"

const heartbeatInterval = 50 * time.Millisecond

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	if args.Term > n.currentTerm || n.role == Candidate {
		n.becomeFollowerLocked(args.Term)
	}
	n.electionResetAt = time.Now()

	if args.PrevLogIndex < n.lastIncludedIndex || args.PrevLogIndex > n.lastLogIndexLocked() || n.logTermAtLocked(args.PrevLogIndex) != args.PrevLogTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	n.appendNewEntriesLocked(args.PrevLogIndex, args.Entries)
	if len(args.Entries) > 0 {
		n.persistStateLocked()
	}

	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, n.lastLogIndexLocked())
	}

	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

func (n *Node) appendNewEntriesLocked(prevLogIndex uint64, entries []LogEntry) {
	for i, entry := range entries {
		sliceIndex := prevLogIndex + uint64(i) - n.lastIncludedIndex
		if sliceIndex < uint64(len(n.log)) {
			if n.log[sliceIndex].Term == entry.Term {
				continue
			}
			n.log = n.log[:sliceIndex]
		}
		n.log = append(n.log, entries[i:]...)
		return
	}
}

func (n *Node) runHeartbeats(term uint64) {
	n.sendHeartbeats(term)

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()
			stillLeader := n.role == Leader && n.currentTerm == term
			n.mu.Unlock()
			if !stillLeader {
				return
			}
			n.sendHeartbeats(term)
		}
	}
}

func (n *Node) sendHeartbeats(term uint64) {
	n.mu.Lock()
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicatePeer(term, peer)
	}
}

func (n *Node) initLeaderStateLocked() {
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	for _, peer := range n.peers {
		n.nextIndex[peer] = n.lastLogIndexLocked() + 1
		n.matchIndex[peer] = 0
	}
}

func (n *Node) replicatePeer(term uint64, peer string) {
	for {
		n.mu.Lock()
		if n.role != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}
		prevLogIndex := n.nextIndex[peer] - 1
		args := AppendEntriesArgs{
			Term:         term,
			LeaderID:     n.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  n.logTermAtLocked(prevLogIndex),
			Entries:      append([]LogEntry(nil), n.log[prevLogIndex-n.lastIncludedIndex:]...),
			LeaderCommit: n.commitIndex,
		}
		transport := n.transport
		n.mu.Unlock()

		reply, err := transport.SendAppendEntries(peer, args)
		if err != nil {
			return
		}

		n.mu.Lock()
		if n.role != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}
		if reply.Term > n.currentTerm {
			n.becomeFollowerLocked(reply.Term)
			n.mu.Unlock()
			return
		}
		if reply.Success {
			n.matchIndex[peer] = prevLogIndex + uint64(len(args.Entries))
			n.nextIndex[peer] = n.matchIndex[peer] + 1
			n.advanceCommitIndexLocked(term)
			n.mu.Unlock()
			return
		}
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
		n.mu.Unlock()
	}
}

func (n *Node) advanceCommitIndexLocked(term uint64) {
	for N := n.lastLogIndexLocked(); N > n.commitIndex; N-- {
		if n.logTermAtLocked(N) != term {
			break
		}
		replicated := 1
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= N {
				replicated++
			}
		}
		if replicated*2 > len(n.peers)+1 {
			n.commitIndex = N
			return
		}
	}
}
