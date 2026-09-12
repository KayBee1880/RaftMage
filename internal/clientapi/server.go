package clientapi

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"raftmage/internal/clientapi/kvpb"
	"raftmage/internal/kvstore"
	"raftmage/internal/raft"
)

const pollInterval = 5 * time.Millisecond

var ErrNotLeader = errors.New("clientapi: not the leader")

type Getter interface {
	Get(key string) ([]byte, bool)
}

type GRPCServer struct {
	kvpb.UnimplementedKVServer
	node  *raft.Node
	store Getter
}

func NewGRPCServer(node *raft.Node, store Getter) *GRPCServer {
	return &GRPCServer{node: node, store: store}
}

func (s *GRPCServer) Get(_ context.Context, req *kvpb.GetRequest) (*kvpb.GetReply, error) {
	value, ok := s.store.Get(req.GetKey())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "key %q not found", req.GetKey())
	}
	return &kvpb.GetReply{Value: value}, nil
}

func (s *GRPCServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutReply, error) {
	command, err := kvstore.EncodeCommand(kvstore.Command{Op: kvstore.OpPut, Key: req.GetKey(), Value: req.GetValue()})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	index, err := s.proposeAndWait(ctx, command)
	if err != nil {
		return nil, s.statusFor(err)
	}
	return &kvpb.PutReply{Index: index}, nil
}

func (s *GRPCServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteReply, error) {
	command, err := kvstore.EncodeCommand(kvstore.Command{Op: kvstore.OpDelete, Key: req.GetKey()})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	index, err := s.proposeAndWait(ctx, command)
	if err != nil {
		return nil, s.statusFor(err)
	}
	return &kvpb.DeleteReply{Index: index}, nil
}

func (s *GRPCServer) proposeAndWait(ctx context.Context, command []byte) (uint64, error) {
	index, _, isLeader := s.node.Propose(command)
	if !isLeader {
		return 0, ErrNotLeader
	}
	for s.node.LastApplied() < index {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
	return index, nil
}

func (s *GRPCServer) statusFor(err error) error {
	switch {
	case errors.Is(err, ErrNotLeader):
		if leader := s.node.CurrentLeader(); leader != "" {
			return status.Errorf(codes.FailedPrecondition, "not the leader, try %q", leader)
		}
		return status.Error(codes.FailedPrecondition, "not the leader, current leader unknown")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "timed out waiting for the write to commit")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled while waiting for the write to commit")
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func NewServer(node *raft.Node, store Getter) *grpc.Server {
	s := grpc.NewServer()
	kvpb.RegisterKVServer(s, NewGRPCServer(node, store))
	reflection.Register(s)
	return s
}
