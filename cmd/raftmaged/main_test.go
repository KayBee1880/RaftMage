package main

import (
	"reflect"
	"testing"
)

func TestParsePeersOnEmptyStringReturnsNoPeers(t *testing.T) {
	addrs, ids, err := parsePeers("")
	if err != nil {
		t.Fatalf("parsePeers returned error: %v", err)
	}
	if len(addrs) != 0 {
		t.Fatalf("addrs = %v, want empty", addrs)
	}
	if ids != nil {
		t.Fatalf("ids = %v, want nil", ids)
	}
}

func TestParsePeersParsesMultipleEntries(t *testing.T) {
	addrs, ids, err := parsePeers("node-2=127.0.0.1:9002,node-3=127.0.0.1:9003")
	if err != nil {
		t.Fatalf("parsePeers returned error: %v", err)
	}
	wantAddrs := map[string]string{"node-2": "127.0.0.1:9002", "node-3": "127.0.0.1:9003"}
	if !reflect.DeepEqual(addrs, wantAddrs) {
		t.Fatalf("addrs = %v, want %v", addrs, wantAddrs)
	}
	wantIDs := []string{"node-2", "node-3"}
	if !reflect.DeepEqual(ids, wantIDs) {
		t.Fatalf("ids = %v, want %v", ids, wantIDs)
	}
}

func TestParsePeersRejectsEntryMissingEquals(t *testing.T) {
	_, _, err := parsePeers("node-2:127.0.0.1:9002")
	if err == nil {
		t.Fatal("expected an error for an entry with no '=', got nil")
	}
}

func TestParsePeersRejectsEmptyID(t *testing.T) {
	_, _, err := parsePeers("=127.0.0.1:9002")
	if err == nil {
		t.Fatal("expected an error for an entry with an empty ID, got nil")
	}
}

func TestParsePeersRejectsEmptyAddress(t *testing.T) {
	_, _, err := parsePeers("node-2=")
	if err == nil {
		t.Fatal("expected an error for an entry with an empty address, got nil")
	}
}

func TestParsePeersAllowsEqualsSignInsideAddress(t *testing.T) {
	addrs, _, err := parsePeers("node-2=127.0.0.1:9002?token=abc")
	if err != nil {
		t.Fatalf("parsePeers returned error: %v", err)
	}
	if addrs["node-2"] != "127.0.0.1:9002?token=abc" {
		t.Fatalf("addrs[node-2] = %q, want %q", addrs["node-2"], "127.0.0.1:9002?token=abc")
	}
}
