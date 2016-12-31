package raft

import (
	"net"

	"github.com/ipdcode/raft/proto"
)

// The StateMachine interface is supplied by the application to persist/snapshot data of application.
type StateMachine interface {
	Apply(command []byte, index uint64) (interface{}, error)
	ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error)
	Snapshot() (Snapshot, error)
	ApplySnapshot(iter SnapIterator) error
	HandleFatalEvent(err *FatalError)
}

type SocketType byte

const (
	HeartBeat SocketType = 0
	Replicate SocketType = 1
)

// The SocketResolver interface is supplied by the application to resolve NodeID to net.Addr addresses.
type SocketResolver interface {
	AllNodes() []uint64
	NodeAddress(nodeID uint64, stype SocketType) (net.Addr, error)
}
