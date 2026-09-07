package transport

import (
	"context"

	"google.golang.org/grpc"

	"raftmage/internal/raft"
	"raftmage/internal/transport/raftpb"
)

type GRPCServer struct {
	raftpb.UnimplementedRaftServer
	node *raft.Node
}

func NewGRPCServer(node *raft.Node) *GRPCServer {
	return &GRPCServer{node: node}
}

func (s *GRPCServer) RequestVote(_ context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteReply, error) {
	reply := s.node.HandleRequestVote(raft.RequestVoteArgs{
		Term:         req.GetTerm(),
		CandidateID:  req.GetCandidateId(),
		LastLogIndex: req.GetLastLogIndex(),
		LastLogTerm:  req.GetLastLogTerm(),
	})
	return &raftpb.RequestVoteReply{Term: reply.Term, VoteGranted: reply.VoteGranted}, nil
}

func (s *GRPCServer) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesReply, error) {
	entries := make([]raft.LogEntry, len(req.GetEntries()))
	for i, e := range req.GetEntries() {
		entries[i] = raft.LogEntry{Term: e.GetTerm(), Command: e.GetCommand()}
	}
	reply := s.node.HandleAppendEntries(raft.AppendEntriesArgs{
		Term:         req.GetTerm(),
		LeaderID:     req.GetLeaderId(),
		PrevLogIndex: req.GetPrevLogIndex(),
		PrevLogTerm:  req.GetPrevLogTerm(),
		Entries:      entries,
		LeaderCommit: req.GetLeaderCommit(),
	})
	return &raftpb.AppendEntriesReply{Term: reply.Term, Success: reply.Success}, nil
}

func (s *GRPCServer) InstallSnapshot(_ context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotReply, error) {
	reply := s.node.HandleInstallSnapshot(raft.InstallSnapshotArgs{
		Term:              req.GetTerm(),
		LeaderID:          req.GetLeaderId(),
		LastIncludedIndex: req.GetLastIncludedIndex(),
		LastIncludedTerm:  req.GetLastIncludedTerm(),
	})
	return &raftpb.InstallSnapshotReply{Term: reply.Term}, nil
}

func NewServer(node *raft.Node) *grpc.Server {
	s := grpc.NewServer()
	raftpb.RegisterRaftServer(s, NewGRPCServer(node))
	return s
}
