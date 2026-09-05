package raft

type PersistentState struct {
	CurrentTerm       uint64
	VotedFor          string
	Log               []LogEntry
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
}

type Storage interface {
	Save(state PersistentState) error
	Load() (PersistentState, error)
}
