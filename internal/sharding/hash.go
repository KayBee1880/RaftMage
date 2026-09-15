package sharding

import "hash/fnv"

func ShardForKey(key string, numShards int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(numShards))
}
