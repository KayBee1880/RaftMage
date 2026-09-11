package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"raftmage/internal/clientapi"
	"raftmage/internal/kvstore"
	"raftmage/internal/raft"
	"raftmage/internal/storage"
	"raftmage/internal/transport"
)

func main() {
	var (
		id         = flag.String("id", "", "this node's unique ID (required)")
		raftAddr   = flag.String("raft-addr", "", "address to listen on for peer Raft traffic, e.g. 127.0.0.1:9001 (required)")
		clientAddr = flag.String("client-addr", "", "address to listen on for the client-facing KV API, e.g. 127.0.0.1:9101 (required)")
		peers      = flag.String("peers", "", "comma-separated peer list: id=address,id=address,...")
		dataFile   = flag.String("data-file", "", "path to this node's persistent state file (empty means no persistence)")
	)
	flag.Parse()

	if *id == "" || *raftAddr == "" || *clientAddr == "" {
		fmt.Fprintln(os.Stderr, "raftmaged: -id, -raft-addr, and -client-addr are all required")
		flag.Usage()
		os.Exit(2)
	}

	peerAddrs, peerIDs, err := parsePeers(*peers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "raftmaged: %v\n", err)
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	var nodeStorage raft.Storage
	if *dataFile != "" {
		nodeStorage = storage.NewFileStorage(*dataFile)
	}

	peerTransport := transport.NewGRPCTransport(peerAddrs)
	defer peerTransport.Close()

	node := raft.NewNode(*id, peerIDs, peerTransport, nodeStorage)
	node.SetLogger(logger)

	store := kvstore.NewStore()
	node.SetStateMachine(store)

	raftListener, err := net.Listen("tcp", *raftAddr)
	if err != nil {
		logger.Error("failed to listen for peer traffic", "addr", *raftAddr, "err", err)
		os.Exit(1)
	}
	raftServer := transport.NewServer(node)
	go func() {
		_ = raftServer.Serve(raftListener)
	}()

	clientListener, err := net.Listen("tcp", *clientAddr)
	if err != nil {
		logger.Error("failed to listen for client traffic", "addr", *clientAddr, "err", err)
		os.Exit(1)
	}
	clientServer := clientapi.NewServer(node, store)
	go func() {
		_ = clientServer.Serve(clientListener)
	}()

	node.Run()
	logger.Info("raftmaged started", "id", *id, "raftAddr", *raftAddr, "clientAddr", *clientAddr, "peers", peerIDs)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	logger.Info("shutting down")
	node.Stop()
	clientServer.GracefulStop()
	raftServer.GracefulStop()
}

func parsePeers(raw string) (addrs map[string]string, ids []string, err error) {
	addrs = make(map[string]string)
	if raw == "" {
		return addrs, nil, nil
	}
	for _, pair := range strings.Split(raw, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, nil, fmt.Errorf("invalid -peers entry %q, want id=address", pair)
		}
		addrs[parts[0]] = parts[1]
		ids = append(ids, parts[0])
	}
	return addrs, ids, nil
}
