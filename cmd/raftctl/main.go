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
)

func main() {
	addr := flag.String("addr", "", "address of a raftmaged instance's client API, e.g. 127.0.0.1:9101 (required)")
	timeout := flag.Duration("timeout", 5*time.Second, "how long to wait for the RPC to complete")
	flag.Parse()

	if *addr == "" {
		fmt.Fprintln(os.Stderr, "raftctl: -addr is required")
		printUsage(os.Stderr)
		os.Exit(2)
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "raftctl: failed to dial %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if err := runCommand(ctx, kvpb.NewKVClient(conn), flag.Args(), os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "raftctl: %v\n", status.Convert(err).Message())
		os.Exit(1)
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: raftctl -addr=host:port <get|put|delete> <key> [value]")
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
