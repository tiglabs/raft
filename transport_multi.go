package raft

import (
	"github.com/ipdcode/raft/proto"
	"github.com/ipdcode/raft/util"
)

type MultiTransport struct {
	heartbeat *heartbeatTransport
	replicate *replicateTransport
}

func NewMultiTransport(raft *RaftServer, config *TransportConfig) (Transport, error) {
	mt := new(MultiTransport)

	if ht, err := newHeartbeatTransport(raft, config); err != nil {
		return nil, err
	} else {
		mt.heartbeat = ht
	}
	if rt, err := newReplicateTransport(raft, config); err != nil {
		return nil, err
	} else {
		mt.replicate = rt
	}

	mt.heartbeat.start()
	mt.replicate.start()
	return mt, nil
}

func (t *MultiTransport) Stop() {
	t.heartbeat.stop()
	t.replicate.stop()
}

func (t *MultiTransport) Send(m *proto.Message) {
	if m.IsElectionMsg() {
		t.heartbeat.send(m)
	} else {
		t.replicate.send(m)
	}
}

func (t *MultiTransport) SendSnapshot(m *proto.Message, rs *snapshotStatus) {
	t.replicate.sendSnapshot(m, rs)
}

func reciveMessage(r *util.BufferReader) (msg *proto.Message, err error) {
	msg = proto.GetMessage()
	if err = msg.Decode(r); err != nil {
		proto.ReturnMessage(msg)
	}
	return
}
