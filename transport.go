package raft

import (
	"github.com/ipdcode/raft/proto"
)

type Transport interface {
	Send(m *proto.Message)
	SendSnapshot(m *proto.Message, rs *snapshotResult)
	Close()
}

func NewMultiTransport(config *TransportConfig, raft *RaftServer) (t Transport, err error) {
	return nil, nil
}
