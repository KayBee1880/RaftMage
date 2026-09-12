package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"raftmage/internal/clientapi"
	"raftmage/internal/kvstore"
	"raftmage/internal/metrics"
	"raftmage/internal/raft"
	"raftmage/internal/storage"
	"raftmage/internal/transport"
)

func main() {
	var (
		id          = flag.String("id", "", "this node's unique ID (required unless -config is set)")
		raftAddr    = flag.String("raft-addr", "", "address to listen on for peer Raft traffic, e.g. 127.0.0.1:9001 (required unless -config is set)")
		clientAddr  = flag.String("client-addr", "", "address to listen on for the client-facing KV API, e.g. 127.0.0.1:9101 (required unless -config is set)")
		peers       = flag.String("peers", "", "comma-separated peer list: id=address,id=address,... (ignored if -config is set)")
		dataFile    = flag.String("data-file", "", "path to this node's persistent state file, empty means no persistence (ignored if -config is set)")
		metricsAddr = flag.String("metrics-addr", "", "address to serve Prometheus-format metrics on, e.g. 127.0.0.1:9201 (empty means no metrics server; ignored if -config is set)")
		configPath  = flag.String("config", "", "path to a JSON config file (id, raft_addr, client_addr, peers, data_file, metrics_addr); when set, every other flag above is ignored")
	)
	flag.Parse()

	cfg, err := resolveConfig(*configPath, *id, *raftAddr, *clientAddr, *peers, *dataFile, *metricsAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "raftmaged: %v\n", err)
		if *configPath == "" {
			flag.Usage()
		}
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	var nodeStorage raft.Storage
	if cfg.DataFile != "" {
		nodeStorage = storage.NewFileStorage(cfg.DataFile)
	}

	peerTransport := transport.NewGRPCTransport(cfg.PeerAddrs)
	defer peerTransport.Close()

	node := raft.NewNode(cfg.ID, cfg.PeerIDs, peerTransport, nodeStorage)
	node.SetLogger(logger)

	store := kvstore.NewStore()
	node.SetStateMachine(store)

	raftListener, err := net.Listen("tcp", cfg.RaftAddr)
	if err != nil {
		logger.Error("failed to listen for peer traffic", "addr", cfg.RaftAddr, "err", err)
		os.Exit(1)
	}
	raftServer := transport.NewServer(node)
	go func() {
		_ = raftServer.Serve(raftListener)
	}()

	clientListener, err := net.Listen("tcp", cfg.ClientAddr)
	if err != nil {
		logger.Error("failed to listen for client traffic", "addr", cfg.ClientAddr, "err", err)
		os.Exit(1)
	}
	clientServer := clientapi.NewServer(node, store)
	go func() {
		_ = clientServer.Serve(clientListener)
	}()

	var metricsServer *http.Server
	if cfg.MetricsAddr != "" {
		metricsServer = &http.Server{Addr: cfg.MetricsAddr, Handler: metrics.Handler(node)}
		go func() {
			if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("metrics server stopped", "err", err)
			}
		}()
	}

	node.Run()
	logger.Info("raftmaged started", "id", cfg.ID, "raftAddr", cfg.RaftAddr, "clientAddr", cfg.ClientAddr, "peers", cfg.PeerIDs, "metricsAddr", cfg.MetricsAddr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	logger.Info("shutting down")
	node.Stop()
	clientServer.GracefulStop()
	raftServer.GracefulStop()
	if metricsServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdownCtx)
	}
}

type nodeConfig struct {
	ID          string
	RaftAddr    string
	ClientAddr  string
	PeerAddrs   map[string]string
	PeerIDs     []string
	DataFile    string
	MetricsAddr string
}

func resolveConfig(configPath, id, raftAddr, clientAddr, peers, dataFile, metricsAddr string) (nodeConfig, error) {
	if configPath != "" {
		return loadConfigFile(configPath)
	}

	if id == "" || raftAddr == "" || clientAddr == "" {
		return nodeConfig{}, fmt.Errorf("-id, -raft-addr, and -client-addr are all required when -config is not given")
	}
	peerAddrs, peerIDs, err := parsePeers(peers)
	if err != nil {
		return nodeConfig{}, err
	}
	return nodeConfig{
		ID:          id,
		RaftAddr:    raftAddr,
		ClientAddr:  clientAddr,
		PeerAddrs:   peerAddrs,
		PeerIDs:     peerIDs,
		DataFile:    dataFile,
		MetricsAddr: metricsAddr,
	}, nil
}

func loadConfigFile(path string) (nodeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nodeConfig{}, fmt.Errorf("failed to read config file %q: %w", path, err)
	}

	var raw struct {
		ID          string            `json:"id"`
		RaftAddr    string            `json:"raft_addr"`
		ClientAddr  string            `json:"client_addr"`
		Peers       map[string]string `json:"peers"`
		DataFile    string            `json:"data_file"`
		MetricsAddr string            `json:"metrics_addr"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nodeConfig{}, fmt.Errorf("failed to parse config file %q: %w", path, err)
	}
	if raw.ID == "" || raw.RaftAddr == "" || raw.ClientAddr == "" {
		return nodeConfig{}, fmt.Errorf("config file %q: id, raft_addr, and client_addr are all required", path)
	}

	peerAddrs := raw.Peers
	if peerAddrs == nil {
		peerAddrs = make(map[string]string)
	}
	var peerIDs []string
	for peerID := range peerAddrs {
		peerIDs = append(peerIDs, peerID)
	}
	sort.Strings(peerIDs)

	return nodeConfig{
		ID:          raw.ID,
		RaftAddr:    raw.RaftAddr,
		ClientAddr:  raw.ClientAddr,
		PeerAddrs:   peerAddrs,
		PeerIDs:     peerIDs,
		DataFile:    raw.DataFile,
		MetricsAddr: raw.MetricsAddr,
	}, nil
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
