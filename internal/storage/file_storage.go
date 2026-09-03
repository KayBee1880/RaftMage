package storage

import (
	"encoding/json"
	"os"

	"raftmage/internal/raft"
)

type FileStorage struct {
	path string
}

func NewFileStorage(path string) *FileStorage {
	return &FileStorage{path: path}
}

func (fs *FileStorage) Save(state raft.PersistentState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	tmpPath := fs.path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, fs.path)
}

func (fs *FileStorage) Load() (raft.PersistentState, error) {
	data, err := os.ReadFile(fs.path)
	if os.IsNotExist(err) {
		return raft.PersistentState{}, nil
	}
	if err != nil {
		return raft.PersistentState{}, err
	}

	var state raft.PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return raft.PersistentState{}, err
	}
	return state, nil
}
