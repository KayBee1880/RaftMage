package sharding

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func writeShardMapFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "shards.json")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("failed to write test shard map file: %v", err)
	}
	return path
}

func TestLoadShardMapParsesValidFile(t *testing.T) {
	path := writeShardMapFile(t, `{
		"shards": [
			{"id": 0, "replicas": ["127.0.0.1:9101"]},
			{"id": 1, "replicas": ["127.0.0.1:9111", "127.0.0.1:9112"]}
		]
	}`)

	sm, err := LoadShardMap(path)
	if err != nil {
		t.Fatalf("LoadShardMap returned error: %v", err)
	}
	if sm.NumShards() != 2 {
		t.Fatalf("NumShards() = %d, want 2", sm.NumShards())
	}
	if !reflect.DeepEqual(sm.Shards[0].Replicas, []string{"127.0.0.1:9101"}) {
		t.Fatalf("shard 0 replicas = %v, want [127.0.0.1:9101]", sm.Shards[0].Replicas)
	}
	if !reflect.DeepEqual(sm.Shards[1].Replicas, []string{"127.0.0.1:9111", "127.0.0.1:9112"}) {
		t.Fatalf("shard 1 replicas = %v, want [127.0.0.1:9111 127.0.0.1:9112]", sm.Shards[1].Replicas)
	}
}

func TestLoadShardMapAcceptsShardsListedOutOfOrder(t *testing.T) {
	path := writeShardMapFile(t, `{
		"shards": [
			{"id": 2, "replicas": ["127.0.0.1:9121"]},
			{"id": 0, "replicas": ["127.0.0.1:9101"]},
			{"id": 1, "replicas": ["127.0.0.1:9111"]}
		]
	}`)

	sm, err := LoadShardMap(path)
	if err != nil {
		t.Fatalf("LoadShardMap returned error: %v", err)
	}
	if sm.Shards[0].Replicas[0] != "127.0.0.1:9101" || sm.Shards[1].Replicas[0] != "127.0.0.1:9111" || sm.Shards[2].Replicas[0] != "127.0.0.1:9121" {
		t.Fatalf("shards not reordered by id: %+v", sm.Shards)
	}
}

func TestLoadShardMapRejectsEmptyShardsList(t *testing.T) {
	path := writeShardMapFile(t, `{"shards": []}`)
	if _, err := LoadShardMap(path); err == nil {
		t.Fatal("expected an error for an empty shards list, got nil")
	}
}

func TestLoadShardMapRejectsShardWithNoReplicas(t *testing.T) {
	path := writeShardMapFile(t, `{"shards": [{"id": 0, "replicas": []}]}`)
	if _, err := LoadShardMap(path); err == nil {
		t.Fatal("expected an error for a shard with no replicas, got nil")
	}
}

func TestLoadShardMapRejectsDuplicateShardID(t *testing.T) {
	path := writeShardMapFile(t, `{
		"shards": [
			{"id": 0, "replicas": ["127.0.0.1:9101"]},
			{"id": 0, "replicas": ["127.0.0.1:9111"]}
		]
	}`)
	if _, err := LoadShardMap(path); err == nil {
		t.Fatal("expected an error for a duplicate shard id, got nil")
	}
}

func TestLoadShardMapRejectsOutOfRangeShardID(t *testing.T) {
	path := writeShardMapFile(t, `{
		"shards": [
			{"id": 0, "replicas": ["127.0.0.1:9101"]},
			{"id": 5, "replicas": ["127.0.0.1:9111"]}
		]
	}`)
	if _, err := LoadShardMap(path); err == nil {
		t.Fatal("expected an error for a shard id outside 0..N-1, got nil")
	}
}

func TestLoadShardMapReturnsErrorForMissingFile(t *testing.T) {
	if _, err := LoadShardMap(filepath.Join(t.TempDir(), "does-not-exist.json")); err == nil {
		t.Fatal("expected an error for a nonexistent shard map file, got nil")
	}
}

func TestLoadShardMapRejectsMalformedJSON(t *testing.T) {
	path := writeShardMapFile(t, `{not valid json`)
	if _, err := LoadShardMap(path); err == nil {
		t.Fatal("expected an error for malformed JSON, got nil")
	}
}

func TestReplicasForKeyMatchesShardForKey(t *testing.T) {
	path := writeShardMapFile(t, `{
		"shards": [
			{"id": 0, "replicas": ["127.0.0.1:9101"]},
			{"id": 1, "replicas": ["127.0.0.1:9111"]},
			{"id": 2, "replicas": ["127.0.0.1:9121"]}
		]
	}`)
	sm, err := LoadShardMap(path)
	if err != nil {
		t.Fatalf("LoadShardMap returned error: %v", err)
	}

	key := "some-test-key"
	want := sm.Shards[ShardForKey(key, 3)].Replicas
	got := sm.ReplicasForKey(key)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReplicasForKey(%q) = %v, want %v", key, got, want)
	}
}
