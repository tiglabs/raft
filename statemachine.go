package raft

import (
	"github.com/ipdcode/raft/proto"
)

// The StateMachine interface is supplied by the application to persist/snapshot data of application.
type StateMachine interface {
	Apply(command []byte, index uint64) (interface{}, error)
	ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error)
	Snapshot() (proto.Snapshot, error)
	ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error
	HandleFatalEvent(err *FatalError)
	HandleLeaderChange(leader uint64)
}

type SocketType byte

const (
	HeartBeat SocketType = 0
	Replicate SocketType = 1
)

func (t SocketType) String() string {
	switch t {
	case 0:
		return "HeartBeat"
	case 1:
		return "Replicate"
	}
	return "unkown"
}

// The SocketResolver interface is supplied by the application to resolve NodeID to net.Addr addresses.
type SocketResolver interface {
	NodeAddress(nodeID uint64, stype SocketType) (addr string, err error)
}
