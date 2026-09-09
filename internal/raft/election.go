package raft

import "time"

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}

	if args.Term < n.currentTerm {
		n.metrics.VotesDenied++
		n.logLocked("denied vote", "candidate", args.CandidateID, "reason", "stale term")
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}

	if n.votedFor != "" && n.votedFor != args.CandidateID {
		n.metrics.VotesDenied++
		n.logLocked("denied vote", "candidate", args.CandidateID, "reason", "already voted")
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}

	if !n.candidateLogIsUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		n.metrics.VotesDenied++
		n.logLocked("denied vote", "candidate", args.CandidateID, "reason", "log not up to date")
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}

	n.votedFor = args.CandidateID
	n.persistStateLocked()
	n.electionResetAt = time.Now()
	n.metrics.VotesGranted++
	n.logLocked("granted vote", "candidate", args.CandidateID)
	return RequestVoteReply{Term: n.currentTerm, VoteGranted: true}
}

func (n *Node) StartElection() {
	n.mu.Lock()
	n.role = Candidate
	n.currentTerm++
	term := n.currentTerm
	n.votedFor = n.id
	n.persistStateLocked()
	n.metrics.ElectionsStarted++
	n.logLocked("starting election")
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: n.lastLogIndexLocked(),
		LastLogTerm:  n.lastLogTermLocked(),
	}
	peers := n.currentConfigLocked()
	transport := n.transport
	n.mu.Unlock()

	votes := 1
	votesNeeded := len(peers)/2 + 1
	replies := make(chan RequestVoteReply, len(peers))

	for _, peer := range peers {
		go func(peer string) {
			reply, err := transport.SendRequestVote(peer, args)
			if err != nil {
				replies <- RequestVoteReply{Term: term, VoteGranted: false}
				return
			}
			replies <- reply
		}(peer)
	}

	for i := 0; i < len(peers); i++ {
		reply := <-replies

		n.mu.Lock()
		if reply.Term > n.currentTerm {
			n.becomeFollowerLocked(reply.Term)
			n.mu.Unlock()
			return
		}
		if reply.VoteGranted {
			votes++
		}
		n.mu.Unlock()
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Candidate || n.currentTerm != term {
		return
	}
	if votes >= votesNeeded {
		n.role = Leader
		n.initLeaderStateLocked()
		n.metrics.ElectionsWon++
		n.logLocked("won election", "votes", votes)
		if n.running {
			go n.runHeartbeats(term)
		}
		return
	}
	n.electionResetAt = time.Now()
	n.logLocked("election lost, retrying", "votes", votes)
	if n.running {
		go n.runElectionTimer(term)
	}
}

func (n *Node) becomeFollowerLocked(term uint64) {
	n.role = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.persistStateLocked()
	n.electionResetAt = time.Now()
	n.logLocked("stepping down to follower")
	if n.running {
		go n.runElectionTimer(term)
	}
}

func (n *Node) candidateLogIsUpToDateLocked(candidateLastIndex, candidateLastTerm uint64) bool {
	lastTerm := n.lastLogTermLocked()
	if candidateLastTerm != lastTerm {
		return candidateLastTerm > lastTerm
	}
	return candidateLastIndex >= n.lastLogIndexLocked()
}
