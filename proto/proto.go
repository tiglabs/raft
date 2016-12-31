package proto

import (
	"fmt"
)

type (
	MsgType        byte
	EntryType      byte
	ConfChangeType byte
	PeerType       byte
)

const (
	ReqMsgAppend MsgType = iota
	ReqMsgVote
	ReqMsgHeartBeat
	ReqMsgSnapShot
	ReqMsgElectAck
	RespMsgAppend
	RespMsgVote
	RespMsgHeartBeat
	RespMsgSnapShot
	RespMsgElectAck
	LocalMsgHup
	LocalMsgProp
	LeaseMsgOffline
	LeaseMsgTimeout
)

const (
	ConfAddNode    ConfChangeType = 0
	ConfRemoveNode ConfChangeType = 1
	ConfUpdateNode ConfChangeType = 2

	EntryNormal     EntryType = 0
	EntryConfChange EntryType = 1

	PeerNormal  PeerType = 0
	PeerArbiter PeerType = 1
)

// The Snapshot interface is supplied by the application to access the snapshot data of application.
type Snapshot interface {
	// if error=io.EOF represent snapshot terminated.
	Next() ([]byte, error)
	ApplyIndex() uint64
	Close()
}

type SnapshotMeta struct {
	Index uint64
	Term  uint64
	Peers []Peer
}

type Peer struct {
	ID       uint64
	Priority uint16
	Type     PeerType
}

// HardState is the repl state,must persist to the storage.
type HardState struct {
	Term   uint64
	Commit uint64
	Vote   uint64
}

// Entry is the repl log entry.
type Entry struct {
	Type  EntryType
	Term  uint64
	Index uint64
	Data  []byte
}

// Message is the transport message.
type Message struct {
	Type         MsgType
	ForceVote    bool
	Reject       bool
	RejectIndex  uint64
	ID           uint64
	From         uint64
	To           uint64
	Term         uint64
	LogTerm      uint64
	Index        uint64
	Commit       uint64
	SnapshotMeta SnapshotMeta
	Entries      []*Entry
	Snapshot     Snapshot // No need for codec
}

type ConfChange struct {
	Type    ConfChangeType
	Peer    Peer
	Context []byte
}

func (t MsgType) String() string {
	switch t {
	case 0:
		return "ReqMsgAppend"
	case 1:
		return "ReqMsgVote"
	case 2:
		return "ReqMsgHeartBeat"
	case 3:
		return "ReqMsgSnapShot"
	case 4:
		return "ReqMsgElectAck"
	case 5:
		return "RespMsgAppend"
	case 6:
		return "RespMsgVote"
	case 7:
		return "RespMsgHeartBeat"
	case 8:
		return "RespMsgSnapShot"
	case 9:
		return "RespMsgElectAck"
	case 10:
		return "LocalMsgHup"
	case 11:
		return "LocalMsgProp"
	case 12:
		return "LeaseMsgOffline"
	case 13:
		return "LeaseMsgTimeout"
	}
	return "unkown"
}

func (t EntryType) String() string {
	switch t {
	case 0:
		return "EntryNormal"
	case 1:
		return "EntryConfChange"
	}
	return "unkown"
}

func (t ConfChangeType) String() string {
	switch t {
	case 0:
		return "ConfAddNode"
	case 1:
		return "ConfRemoveNode"
	case 2:
		return "ConfUpdateNode"
	}
	return "unkown"
}

func (t PeerType) String() string {
	switch t {
	case 0:
		return "PeerNormal"
	case 1:
		return "PeerArbiter"
	}
	return "unkown"
}

func (p Peer) String() string {
	return fmt.Sprintf(`"nodeID":"%v","priority":"%v","type":"%v"`, p.ID, p.Priority, p.Type.String())
}

func (cc *ConfChange) String() string {
	return fmt.Sprintf(`{"type":"%v",%v}`, cc.Type, cc.Peer.String())
}

func (m *Message) IsResponseMsg() bool {
	return m.Type == RespMsgAppend || m.Type == RespMsgHeartBeat || m.Type == RespMsgVote || m.Type == RespMsgElectAck || m.Type == RespMsgSnapShot
}

func (m *Message) IsElectionMsg() bool {
	return m.Type == ReqMsgHeartBeat || m.Type == RespMsgHeartBeat || m.Type == ReqMsgVote || m.Type == RespMsgVote ||
		m.Type == ReqMsgElectAck || m.Type == RespMsgElectAck || m.Type == LeaseMsgOffline || m.Type == LeaseMsgTimeout
}

func (s *HardState) IsEmpty() bool {
	return s.Term == 0 && s.Vote == 0 && s.Commit == 0
}
