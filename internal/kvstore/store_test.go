package kvstore

import "testing"

func TestEncodeDecodeCommandRoundTrips(t *testing.T) {
	want := Command{Op: OpPut, Key: "x", Value: []byte("1")}

	encoded, err := EncodeCommand(want)
	if err != nil {
		t.Fatalf("EncodeCommand failed: %v", err)
	}
	got, err := DecodeCommand(encoded)
	if err != nil {
		t.Fatalf("DecodeCommand failed: %v", err)
	}

	if got.Op != want.Op || got.Key != want.Key || string(got.Value) != string(want.Value) {
		t.Fatalf("DecodeCommand() = %+v, want %+v", got, want)
	}
}

func TestDecodeCommandRejectsMalformedInput(t *testing.T) {
	if _, err := DecodeCommand([]byte("not json")); err == nil {
		t.Fatal("expected an error decoding malformed input, got nil")
	}
}

func TestStoreApplyPutThenGet(t *testing.T) {
	s := NewStore()
	encoded, _ := EncodeCommand(Command{Op: OpPut, Key: "x", Value: []byte("1")})

	if err := s.Apply(encoded); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	got, ok := s.Get("x")
	if !ok {
		t.Fatal("expected key \"x\" to be present after Apply")
	}
	if string(got) != "1" {
		t.Fatalf("Get(\"x\") = %q, want \"1\"", got)
	}
}

func TestStoreApplyDeleteRemovesKey(t *testing.T) {
	s := NewStore()
	put, _ := EncodeCommand(Command{Op: OpPut, Key: "x", Value: []byte("1")})
	del, _ := EncodeCommand(Command{Op: OpDelete, Key: "x"})

	if err := s.Apply(put); err != nil {
		t.Fatalf("Apply(put) failed: %v", err)
	}
	if err := s.Apply(del); err != nil {
		t.Fatalf("Apply(delete) failed: %v", err)
	}

	if _, ok := s.Get("x"); ok {
		t.Fatal("expected key \"x\" to be gone after Apply(delete)")
	}
}

func TestStoreGetOnMissingKeyReturnsFalse(t *testing.T) {
	s := NewStore()

	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected ok = false for a key that was never put")
	}
}

func TestStoreApplyRejectsMalformedCommand(t *testing.T) {
	s := NewStore()

	if err := s.Apply([]byte("not json")); err == nil {
		t.Fatal("expected Apply to return an error for a malformed command")
	}
}

func TestStoreGetReturnsACopyNotSharedMemory(t *testing.T) {
	s := NewStore()
	put, _ := EncodeCommand(Command{Op: OpPut, Key: "x", Value: []byte("1")})
	if err := s.Apply(put); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	got, _ := s.Get("x")
	got[0] = 'Z'

	again, _ := s.Get("x")
	if string(again) != "1" {
		t.Fatalf("stored value was mutated via a previously returned Get() slice, got %q", again)
	}
}

func TestStoreSnapshotRestoreRoundTrips(t *testing.T) {
	s := NewStore()
	put, _ := EncodeCommand(Command{Op: OpPut, Key: "x", Value: []byte("1")})
	if err := s.Apply(put); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	snapshot, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	restored := NewStore()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	got, ok := restored.Get("x")
	if !ok || string(got) != "1" {
		t.Fatalf("restored Get(\"x\") = (%q, %v), want (\"1\", true)", got, ok)
	}
}

func TestStoreRestoreOnEmptyDataYieldsEmptyStore(t *testing.T) {
	s := NewStore()
	put, _ := EncodeCommand(Command{Op: OpPut, Key: "x", Value: []byte("1")})
	if err := s.Apply(put); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if err := s.Restore(nil); err != nil {
		t.Fatalf("Restore(nil) failed: %v", err)
	}

	if _, ok := s.Get("x"); ok {
		t.Fatal("expected Restore(nil) to clear any pre-existing state")
	}
}

func TestStoreRestoreRejectsMalformedData(t *testing.T) {
	s := NewStore()

	if err := s.Restore([]byte("not json")); err == nil {
		t.Fatal("expected Restore to return an error for malformed data")
	}
}
