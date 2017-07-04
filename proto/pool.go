package proto

import (
	"sync"
)

var (
	msgPool = &sync.Pool{
		New: func() interface{} {
			return &Message{
				Entries: make([]*Entry, 0, 128),
			}
		},
	}

	bytePool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, 128)
		},
	}
)

func GetMessage() *Message {
	msg := msgPool.Get().(*Message)
	msg.Reject = false
	msg.RejectIndex = 0
	msg.ID = 0
	msg.From = 0
	msg.To = 0
	msg.Term = 0
	msg.LogTerm = 0
	msg.Index = 0
	msg.Commit = 0
	msg.SnapshotMeta.Index = 0
	msg.SnapshotMeta.Term = 0
	msg.SnapshotMeta.Peers = nil
	msg.Snapshot = nil
	msg.Context = nil
	msg.Entries = msg.Entries[0:0]
	return msg
}

func ReturnMessage(msg *Message) {
	if msg != nil {
		msgPool.Put(msg)
	}
}

func getByteSlice() []byte {
	return bytePool.Get().([]byte)
}

func returnByteSlice(b []byte) {
	bytePool.Put(b)
}
