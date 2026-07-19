package raft

type Transport interface {
	SendRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error)
}
