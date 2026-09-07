package transport

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raftmage/internal/raft"
	"raftmage/internal/transport/raftpb"
)

const rpcTimeout = 5 * time.Second

type GRPCTransport struct {
	mu    sync.Mutex
	addrs map[string]string
	conns map[string]*grpc.ClientConn
}

func NewGRPCTransport(addrs map[string]string) *GRPCTransport {
	return &GRPCTransport{
		addrs: addrs,
		conns: make(map[string]*grpc.ClientConn),
	}
}

func (t *GRPCTransport) client(peer string) (raftpb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, ok := t.conns[peer]; ok {
		return raftpb.NewRaftClient(conn), nil
	}
	addr, ok := t.addrs[peer]
	if !ok {
		return nil, fmt.Errorf("transport: unknown peer %q", peer)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conns[peer] = conn
	return raftpb.NewRaftClient(conn), nil
}

func (t *GRPCTransport) SendRequestVote(peer string, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	client, err := t.client(peer)
	if err != nil {
		return raft.RequestVoteReply{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	reply, err := client.RequestVote(ctx, &raftpb.RequestVoteRequest{
		Term:         args.Term,
		CandidateId:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return raft.RequestVoteReply{}, err
	}
	return raft.RequestVoteReply{Term: reply.GetTerm(), VoteGranted: reply.GetVoteGranted()}, nil
}

func (t *GRPCTransport) SendAppendEntries(peer string, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	client, err := t.client(peer)
	if err != nil {
		return raft.AppendEntriesReply{}, err
	}
	entries := make([]*raftpb.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = &raftpb.LogEntry{Term: e.Term, Command: e.Command}
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	reply, err := client.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term:         args.Term,
		LeaderId:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return raft.AppendEntriesReply{}, err
	}
	return raft.AppendEntriesReply{Term: reply.GetTerm(), Success: reply.GetSuccess()}, nil
}

func (t *GRPCTransport) SendInstallSnapshot(peer string, args raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	client, err := t.client(peer)
	if err != nil {
		return raft.InstallSnapshotReply{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	reply, err := client.InstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		Term:              args.Term,
		LeaderId:          args.LeaderID,
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
	})
	if err != nil {
		return raft.InstallSnapshotReply{}, err
	}
	return raft.InstallSnapshotReply{Term: reply.GetTerm()}, nil
}

func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var firstErr error
	for _, conn := range t.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
