package raft

import "time"

const heartbeatInterval = 50 * time.Millisecond

type AppendEntriesArgs struct {
	Term     uint64
	LeaderID string
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
	return AppendEntriesReply{Term: n.currentTerm, Success: true}
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
