package raft

import (
	"errors"
)

var (
	ErrCompacted     = errors.New("requested index is unavailable due to compaction.")
	ErrRaftExists    = errors.New("raft already exists.")
	ErrRaftNotExists = errors.New("raft not exists.")
	ErrNotLeader     = errors.New("raft is not the leader.")
	ErrStopped       = errors.New("raft is already shutdown.")
	ErrSnapping      = errors.New("raft is doing snapshot.")
)

type FatalError struct {
	ID  uint64
	Err error
}

// AppPanicError is panic error when repl occurred fatal error.
// The server will recover this panic and stop the shard repl.
type AppPanicError string

func (pe *AppPanicError) Error() string {
	return "Occurred application logic panic error: " + string(*pe)
}
