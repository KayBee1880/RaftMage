package sharding

import (
	"fmt"
	"testing"
)

func TestShardForKeyStaysInRange(t *testing.T) {
	for _, key := range []string{"a", "b", "foo", "bar", "a-very-long-key-name-1234567890"} {
		for numShards := 1; numShards <= 8; numShards++ {
			shard := ShardForKey(key, numShards)
			if shard < 0 || shard >= numShards {
				t.Fatalf("ShardForKey(%q, %d) = %d, want a value in [0, %d)", key, numShards, shard, numShards)
			}
		}
	}
}

func TestShardForKeyIsDeterministic(t *testing.T) {
	first := ShardForKey("foo", 5)
	for i := 0; i < 100; i++ {
		if got := ShardForKey("foo", 5); got != first {
			t.Fatalf("ShardForKey(\"foo\", 5) = %d on call %d, want %d (same as the first call)", got, i, first)
		}
	}
}

func TestShardForKeyDistributesAcrossShards(t *testing.T) {
	seen := make(map[int]bool)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		seen[ShardForKey(key, 4)] = true
	}
	if len(seen) < 2 {
		t.Fatalf("100 distinct keys landed on only %d distinct shard(s) out of 4, want more spread", len(seen))
	}
}

func TestShardForKeySingleShardAlwaysReturnsZero(t *testing.T) {
	for _, key := range []string{"a", "b", "c"} {
		if got := ShardForKey(key, 1); got != 0 {
			t.Fatalf("ShardForKey(%q, 1) = %d, want 0", key, got)
		}
	}
}
