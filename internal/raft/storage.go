package raft

type PersistentState struct {
	CurrentTerm uint64
	VotedFor    string
	Log         []LogEntry
}

type Storage interface {
	Save(state PersistentState) error
	Load() (PersistentState, error)
}
