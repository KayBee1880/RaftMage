package kvstore

import (
	"encoding/json"
	"sync"
)

type Op int

const (
	OpPut Op = iota
	OpDelete
)

type Command struct {
	Op    Op
	Key   string
	Value []byte
}

func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}

type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewStore() *Store {
	return &Store{data: make(map[string][]byte)}
}

func (s *Store) Apply(command []byte) error {
	cmd, err := DecodeCommand(command)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = cmd.Value
	case OpDelete:
		delete(s.data, cmd.Key)
	}
	return nil
}

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), value...), true
}
