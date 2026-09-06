package raft

type Transport interface {
	SendRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error)
	SendInstallSnapshot(peer string, args InstallSnapshotArgs) (InstallSnapshotReply, error)
}
