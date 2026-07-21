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

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > n.lastLogIndexLocked() || n.logTermAtLocked(args.PrevLogIndex) != args.PrevLogTerm {
			return AppendEntriesReply{Term: n.currentTerm, Success: false}
		}
	}

	n.appendNewEntriesLocked(args.PrevLogIndex, args.Entries)

	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, n.lastLogIndexLocked())
	}

	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

func (n *Node) appendNewEntriesLocked(prevLogIndex uint64, entries []LogEntry) {
	for i, entry := range entries {
		logIndex := prevLogIndex + uint64(i)
		if logIndex < uint64(len(n.log)) {
			if n.log[logIndex].Term == entry.Term {
				continue
			}
			n.log = n.log[:logIndex]
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
	transport := n.transport
	leaderID := n.id
	n.mu.Unlock()

	args := AppendEntriesArgs{Term: term, LeaderID: leaderID}

	for _, peer := range peers {
		go func(peer string) {
			reply, err := transport.SendAppendEntries(peer, args)
			if err != nil {
				return
			}
			if reply.Term > term {
				n.mu.Lock()
				if reply.Term > n.currentTerm {
					n.becomeFollowerLocked(reply.Term)
				}
				n.mu.Unlock()
			}
		}(peer)
	}
}
