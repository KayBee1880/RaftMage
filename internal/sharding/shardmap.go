package sharding

import (
	"encoding/json"
	"fmt"
	"os"
)

type Shard struct {
	ID       int      `json:"id"`
	Replicas []string `json:"replicas"`
}

type ShardMap struct {
	Shards []Shard
}

func LoadShardMap(path string) (ShardMap, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ShardMap{}, fmt.Errorf("failed to read shard map %q: %w", path, err)
	}

	var raw struct {
		Shards []Shard `json:"shards"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return ShardMap{}, fmt.Errorf("failed to parse shard map %q: %w", path, err)
	}
	if len(raw.Shards) == 0 {
		return ShardMap{}, fmt.Errorf("shard map %q: at least one shard is required", path)
	}

	ordered := make([]Shard, len(raw.Shards))
	seen := make([]bool, len(raw.Shards))
	for _, s := range raw.Shards {
		if len(s.Replicas) == 0 {
			return ShardMap{}, fmt.Errorf("shard map %q: shard %d has no replicas", path, s.ID)
		}
		if s.ID < 0 || s.ID >= len(raw.Shards) {
			return ShardMap{}, fmt.Errorf("shard map %q: shard ids must be exactly 0..%d, got %d", path, len(raw.Shards)-1, s.ID)
		}
		if seen[s.ID] {
			return ShardMap{}, fmt.Errorf("shard map %q: duplicate shard id %d", path, s.ID)
		}
		seen[s.ID] = true
		ordered[s.ID] = s
	}

	return ShardMap{Shards: ordered}, nil
}

func (sm ShardMap) NumShards() int {
	return len(sm.Shards)
}

func (sm ShardMap) ReplicasForKey(key string) []string {
	return sm.Shards[ShardForKey(key, len(sm.Shards))].Replicas
}
