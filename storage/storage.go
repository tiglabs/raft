package storage

import (
	"github.com/tigcode/raft/proto"
)

// Storage is an interface that may be implemented by the application to retrieve log entries from storage.
// If any Storage method returns an error, the raft instance will become inoperable and refuse to participate in elections;
// the application is responsible for cleanup and recovery in this case.
type Storage interface {
	// InitialState returns the saved HardState information to init the repl state.
	InitialState() (proto.HardState, error)
	// Entries returns a slice of log entries in the range [lo,hi), the hi is not inclusive.
	// MaxSize limits the total size of the log entries returned, but Entries returns at least one entry if any.
	// If lo <= CompactIndex,then return isCompact true.
	// If no entries,then return entries nil.
	// Note: math.MaxUint32 is no limit.
	Entries(lo, hi uint64, maxSize uint64) (entries []*proto.Entry, isCompact bool, err error)
	// Term returns the term of entry i, which must be in the range [FirstIndex()-1, LastIndex()].
	// The term of the entry before FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
	// If lo <= CompactIndex,then return isCompact true.
	Term(i uint64) (term uint64, isCompact bool, err error)
	// FirstIndex returns the index of the first log entry that is possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the first log entry is not available).
	FirstIndex() (uint64, error)
	// LastIndex returns the index of the last entry in the log.
	LastIndex() (uint64, error)
	// StoreEntries store the log entries to the repository.
	// If first index of entries > LastIndex,then append all entries,
	// Else write entries at first index and truncate the redundant log entries.
	StoreEntries(entries []*proto.Entry) error
	// StoreHardState store the raft state to the repository.
	StoreHardState(st proto.HardState) error
	// Truncate the log to index,  The index is inclusive.
	Truncate(index uint64) error
	// Sync snapshot status.
	ApplySnapshot(meta proto.SnapshotMeta) error
	// Close the storage.
	Close()
}
