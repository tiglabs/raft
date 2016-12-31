package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ipdcode/raft/proto"
)

// MemoryStorage implements the Storage interface backed by an in-memory array.
type MemoryStorage struct {
	sync.Mutex
	hardState proto.HardState
	ents      []*proto.Entry
}

func NewMemoryStorage() *MemoryStorage {
	ms := &MemoryStorage{ents: make([]*proto.Entry, 0, 128)}
	ms.ents = append(ms.ents, new(proto.Entry))
	return ms
}

func (ms *MemoryStorage) InitialState() (*proto.HardState, error) {
	return &ms.hardState, nil
}

// SetHardState saves the current HardState.
func (ms *MemoryStorage) StoreHardState(st *proto.HardState) error {
	ms.hardState = *st
	return nil
}

func (ms *MemoryStorage) Entries(lo, hi uint64, maxSize uint64) (entries []*proto.Entry, isCompact bool, err error) {
	ms.Lock()
	defer ms.Unlock()

	offset := ms.ents[0].Index
	if lo <= offset {
		return nil, true, nil
	}
	if hi > ms.lastIndex()+1 {
		return nil, false, fmt.Errorf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}
	// only contains dummy entries.
	if len(ms.ents) == 1 {
		return nil, false, errors.New("requested entry at index is unavailable")
	}

	ents := ms.ents[lo-offset : hi-offset]
	return limitSize(ents, maxSize), false, nil
}

func (ms *MemoryStorage) Term(i uint64) (term uint64, isCompact bool, err error) {
	ms.Lock()
	defer ms.Unlock()

	offset := ms.ents[0].Index
	if i < offset {
		return 0, true, nil
	}
	return ms.ents[i-offset].Term, false, nil
}

func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()

	return ms.lastIndex(), nil
}

func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()

	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

func (ms *MemoryStorage) ApplySnapshot(meta proto.SnapshotMeta) error {
	ms.Lock()
	defer ms.Unlock()

	ms.ents = []*proto.Entry{{Term: meta.Term, Index: meta.Index}}
	return nil
}

func (ms *MemoryStorage) Truncate(index uint64) error {
	ms.Lock()
	defer ms.Unlock()

	offset := ms.ents[0].Index
	if index <= offset {
		return errors.New("requested index is unavailable due to compaction")
	}

	if index > ms.lastIndex() {
		return fmt.Errorf("compact %d is out of bound lastindex(%d)", index, ms.lastIndex())
	}

	i := index - offset
	ents := make([]*proto.Entry, 0, 1+uint64(len(ms.ents))-i)
	ents = append(ents, new(proto.Entry))
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

func (ms *MemoryStorage) StoreEntries(entries []*proto.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	first := ms.firstIndex()
	last := entries[0].Index + uint64(len(entries)) - 1

	// shortcut if there is no new entry.
	if last < first {
		return nil
	}
	// truncate compacted entries
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		ms.ents = append([]*proto.Entry{}, ms.ents[:offset]...)
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset:
		ms.ents = append(ms.ents, entries...)
	default:
		return fmt.Errorf("missing log entry [last: %d, append at: %d]", ms.lastIndex(), entries[0].Index)
	}

	return nil
}

func (ms *MemoryStorage) Clear() error {
	ms.Lock()
	defer ms.Unlock()

	ms.ents = append([]*proto.Entry{}, new(proto.Entry))
	return nil
}

func (ms *MemoryStorage) Close() {}

func limitSize(ents []*proto.Entry, maxSize uint64) []*proto.Entry {
	if len(ents) == 0 {
		return ents
	}

	size := ents[0].Size()
	limit := 1
	for l := len(ents); limit < l; limit++ {
		size += ents[limit].Size()
		if size > maxSize {
			break
		}
	}
	return ents[:limit]
}
