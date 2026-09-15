package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"raftmage/internal/clientapi/kvpb"
	"raftmage/internal/sharding"
)

func main() {
	addr := flag.String("addr", "", "address of a raftmaged instance's client API, e.g. 127.0.0.1:9101 (mutually exclusive with -shard-map)")
	shardMapPath := flag.String("shard-map", "", "path to a JSON shard map file; routes by key to the owning shard's replicas (mutually exclusive with -addr)")
	timeout := flag.Duration("timeout", 5*time.Second, "how long to wait for the RPC to complete")
	flag.Parse()

	if (*addr == "") == (*shardMapPath == "") {
		fmt.Fprintln(os.Stderr, "raftctl: exactly one of -addr or -shard-map is required")
		printUsage(os.Stderr)
		os.Exit(2)
	}

	args := flag.Args()

	var addrs []string
	if *addr != "" {
		addrs = []string{*addr}
	} else {
		sm, err := sharding.LoadShardMap(*shardMapPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "raftctl: %v\n", err)
			os.Exit(2)
		}
		key, err := keyForRouting(args)
		if err != nil {
			fmt.Fprintf(os.Stderr, "raftctl: %v\n", err)
			printUsage(os.Stderr)
			os.Exit(2)
		}
		addrs = sm.ReplicasForKey(key)
	}

	clients, closeAll, err := dialAll(addrs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "raftctl: %v\n", err)
		os.Exit(1)
	}
	defer closeAll()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if err := tryReplicas(ctx, clients, args, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "raftctl: %v\n", status.Convert(err).Message())
		os.Exit(1)
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: raftctl -addr=host:port <get|put|delete> <key> [value]")
	fmt.Fprintln(w, "   or: raftctl -shard-map=shards.json <get|put|delete> <key> [value]")
}

func keyForRouting(args []string) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("usage: <get|put|delete> <key> [value]")
	}
	return args[1], nil
}

func dialAll(addrs []string) (clients []kvpb.KVClient, closeAll func(), err error) {
	var conns []*grpc.ClientConn
	closeAll = func() {
		for _, conn := range conns {
			conn.Close()
		}
	}
	for _, addr := range addrs {
		conn, dialErr := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr != nil {
			closeAll()
			return nil, func() {}, fmt.Errorf("failed to dial %s: %w", addr, dialErr)
		}
		conns = append(conns, conn)
		clients = append(clients, kvpb.NewKVClient(conn))
	}
	return clients, closeAll, nil
}

func tryReplicas(ctx context.Context, clients []kvpb.KVClient, args []string, stdout io.Writer) error {
	if len(clients) == 0 {
		return fmt.Errorf("no replicas available")
	}
	var lastErr error
	for _, client := range clients {
		if err := runCommand(ctx, client, args, stdout); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

func runCommand(ctx context.Context, client kvpb.KVClient, args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("no command given, want get|put|delete")
	}

	switch cmd, rest := args[0], args[1:]; cmd {
	case "get":
		if len(rest) != 1 {
			return fmt.Errorf("usage: get <key>")
		}
		reply, err := client.Get(ctx, &kvpb.GetRequest{Key: rest[0]})
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, string(reply.GetValue()))
		return nil

	case "put":
		if len(rest) != 2 {
			return fmt.Errorf("usage: put <key> <value>")
		}
		reply, err := client.Put(ctx, &kvpb.PutRequest{Key: rest[0], Value: []byte(rest[1])})
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "committed at index %d\n", reply.GetIndex())
		return nil

	case "delete":
		if len(rest) != 1 {
			return fmt.Errorf("usage: delete <key>")
		}
		reply, err := client.Delete(ctx, &kvpb.DeleteRequest{Key: rest[0]})
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "committed at index %d\n", reply.GetIndex())
		return nil

	default:
		return fmt.Errorf("unknown command %q, want get|put|delete", cmd)
	}
}
