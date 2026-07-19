package raft

import (
	"math/rand"
	"time"
)

const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	electionTimerTick  = 10 * time.Millisecond
)

func randomElectionTimeout() time.Duration {
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
}

func (n *Node) Run() {
	n.mu.Lock()
	if n.running {
		n.mu.Unlock()
		return
	}
	n.running = true
	n.electionResetAt = time.Now()
	term := n.currentTerm
	n.mu.Unlock()

	go n.runElectionTimer(term)
}

func (n *Node) Stop() {
	n.mu.Lock()
	n.running = false
	n.mu.Unlock()
	n.cancel()
}

func (n *Node) runElectionTimer(termStarted uint64) {
	timeout := randomElectionTimeout()
	ticker := time.NewTicker(electionTimerTick)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.role == Leader || n.currentTerm != termStarted {
				n.mu.Unlock()
				return
			}
			elapsed := time.Since(n.electionResetAt)
			n.mu.Unlock()

			if elapsed >= timeout {
				n.StartElection()
				return
			}
		}
	}
}
