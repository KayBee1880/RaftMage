package main

import (
	"os"
	"path/filepath"
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

func TestResolveConfigUsesFlagsWhenNoConfigPathGiven(t *testing.T) {
	cfg, err := resolveConfig("", "node-1", "127.0.0.1:9001", "127.0.0.1:9101", "node-2=127.0.0.1:9002", "/tmp/node-1.json", "127.0.0.1:9201")
	if err != nil {
		t.Fatalf("resolveConfig returned error: %v", err)
	}
	if cfg.ID != "node-1" || cfg.RaftAddr != "127.0.0.1:9001" || cfg.ClientAddr != "127.0.0.1:9101" || cfg.DataFile != "/tmp/node-1.json" || cfg.MetricsAddr != "127.0.0.1:9201" {
		t.Fatalf("cfg = %+v, want flag-derived values", cfg)
	}
	if cfg.PeerAddrs["node-2"] != "127.0.0.1:9002" {
		t.Fatalf("cfg.PeerAddrs = %v, want node-2 -> 127.0.0.1:9002", cfg.PeerAddrs)
	}
}

func TestResolveConfigRejectsMissingRequiredFlagsWhenNoConfigPathGiven(t *testing.T) {
	if _, err := resolveConfig("", "", "127.0.0.1:9001", "127.0.0.1:9101", "", "", ""); err == nil {
		t.Fatal("expected an error for a missing -id with no -config, got nil")
	}
}

func TestResolveConfigMetricsAddrDefaultsToEmptyMeaningDisabled(t *testing.T) {
	cfg, err := resolveConfig("", "node-1", "127.0.0.1:9001", "127.0.0.1:9101", "", "", "")
	if err != nil {
		t.Fatalf("resolveConfig returned error: %v", err)
	}
	if cfg.MetricsAddr != "" {
		t.Fatalf("cfg.MetricsAddr = %q, want empty when -metrics-addr is not given", cfg.MetricsAddr)
	}
}

func writeConfigFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("failed to write test config file: %v", err)
	}
	return path
}

func TestResolveConfigLoadsFromFileAndIgnoresFlagsWhenConfigPathGiven(t *testing.T) {
	path := writeConfigFile(t, `{
		"id": "node-1",
		"raft_addr": "127.0.0.1:9001",
		"client_addr": "127.0.0.1:9101",
		"peers": {"node-2": "127.0.0.1:9002", "node-3": "127.0.0.1:9003"},
		"data_file": "/data/node-1.json",
		"metrics_addr": "127.0.0.1:9201"
	}`)

	cfg, err := resolveConfig(path, "flag-id-should-be-ignored", "", "", "", "", "")
	if err != nil {
		t.Fatalf("resolveConfig returned error: %v", err)
	}
	if cfg.ID != "node-1" {
		t.Fatalf("cfg.ID = %q, want %q (flags must be ignored once -config is set)", cfg.ID, "node-1")
	}
	if cfg.RaftAddr != "127.0.0.1:9001" || cfg.ClientAddr != "127.0.0.1:9101" || cfg.DataFile != "/data/node-1.json" || cfg.MetricsAddr != "127.0.0.1:9201" {
		t.Fatalf("cfg = %+v, want file-derived values", cfg)
	}
	wantPeerIDs := []string{"node-2", "node-3"}
	if !reflect.DeepEqual(cfg.PeerIDs, wantPeerIDs) {
		t.Fatalf("cfg.PeerIDs = %v, want %v (sorted for determinism)", cfg.PeerIDs, wantPeerIDs)
	}
}

func TestLoadConfigFileRejectsMissingRequiredFields(t *testing.T) {
	path := writeConfigFile(t, `{"raft_addr": "127.0.0.1:9001", "client_addr": "127.0.0.1:9101"}`)
	if _, err := loadConfigFile(path); err == nil {
		t.Fatal("expected an error for a config file missing id, got nil")
	}
}

func TestLoadConfigFileRejectsMalformedJSON(t *testing.T) {
	path := writeConfigFile(t, `{not valid json`)
	if _, err := loadConfigFile(path); err == nil {
		t.Fatal("expected an error for malformed JSON, got nil")
	}
}

func TestLoadConfigFileReturnsErrorForMissingFile(t *testing.T) {
	if _, err := loadConfigFile(filepath.Join(t.TempDir(), "does-not-exist.json")); err == nil {
		t.Fatal("expected an error for a nonexistent config file, got nil")
	}
}

func TestLoadConfigFileOnAbsentPeersYieldsEmptyNonNilMap(t *testing.T) {
	path := writeConfigFile(t, `{"id": "node-1", "raft_addr": "127.0.0.1:9001", "client_addr": "127.0.0.1:9101"}`)
	cfg, err := loadConfigFile(path)
	if err != nil {
		t.Fatalf("loadConfigFile returned error: %v", err)
	}
	if cfg.PeerAddrs == nil || len(cfg.PeerAddrs) != 0 {
		t.Fatalf("cfg.PeerAddrs = %v, want an empty, non-nil map", cfg.PeerAddrs)
	}
	if cfg.PeerIDs != nil {
		t.Fatalf("cfg.PeerIDs = %v, want nil for a single-node cluster", cfg.PeerIDs)
	}
}
