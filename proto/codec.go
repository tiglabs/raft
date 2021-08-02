// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package proto

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/tiglabs/raft/util"
)

const (
	version1        byte   = 1
	peer_size       uint64 = 11
	entry_header    uint64 = 17
	snapmeta_header uint64 = 20
	message_header  uint64 = 68 + 1 + 4 // 1 is the len(MsgMagic), 4 is the len(entries + context)
)

// Peer codec
func (p *Peer) Encode(datas []byte) {
	datas[0] = byte(p.Type)
	binary.BigEndian.PutUint16(datas[1:], p.Priority)
	binary.BigEndian.PutUint64(datas[3:], p.ID)
}

func (p *Peer) Decode(datas []byte) {
	p.Type = PeerType(datas[0])
	p.Priority = binary.BigEndian.Uint16(datas[1:])
	p.ID = binary.BigEndian.Uint64(datas[3:])
}

// HardState codec
func (c *HardState) Encode(datas []byte) {
	binary.BigEndian.PutUint64(datas[0:], c.Term)
	binary.BigEndian.PutUint64(datas[8:], c.Commit)
	binary.BigEndian.PutUint64(datas[16:], c.Vote)
}

func (c *HardState) Decode(datas []byte) {
	c.Term = binary.BigEndian.Uint64(datas[0:])
	c.Commit = binary.BigEndian.Uint64(datas[8:])
	c.Vote = binary.BigEndian.Uint64(datas[16:])
}

func (c *HardState) Size() uint64 {
	return 24
}

// ConfChange codec
func (c *ConfChange) Encode() []byte {
	datas := make([]byte, 1+peer_size+uint64(len(c.Context)))
	datas[0] = byte(c.Type)
	c.Peer.Encode(datas[1:])
	if len(c.Context) > 0 {
		copy(datas[peer_size+1:], c.Context)
	}
	return datas
}

func (c *ConfChange) Decode(datas []byte) {
	c.Type = ConfChangeType(datas[0])
	c.Peer.Decode(datas[1:])
	if uint64(len(datas)) > peer_size+1 {
		c.Context = append([]byte{}, datas[peer_size+1:]...)
	}
}

// SnapshotMeta codec
func (m *SnapshotMeta) Size() uint64 {
	return snapmeta_header + peer_size*uint64(len(m.Peers))
}

func (m *SnapshotMeta) Encode(w io.Writer) error {
	buf := getByteSlice()
	defer returnByteSlice(buf)

	binary.BigEndian.PutUint64(buf, m.Index)
	binary.BigEndian.PutUint64(buf[8:], m.Term)
	binary.BigEndian.PutUint32(buf[16:], uint32(len(m.Peers)))
	if _, err := w.Write(buf[0:snapmeta_header]); err != nil {
		return err
	}

	for _, p := range m.Peers {
		p.Encode(buf)
		if _, err := w.Write(buf[0:peer_size]); err != nil {
			return err
		}
	}
	return nil
}

func (m *SnapshotMeta) Decode(datas []byte) {
	m.Index = binary.BigEndian.Uint64(datas)
	m.Term = binary.BigEndian.Uint64(datas[8:])
	size := binary.BigEndian.Uint32(datas[16:])
	m.Peers = make([]Peer, size)
	start := snapmeta_header
	for i := uint32(0); i < size; i++ {
		m.Peers[i].Decode(datas[start:])
		start = start + peer_size
	}
}

// Entry codec
func (e *Entry) Size() uint64 {
	return entry_header + uint64(len(e.Data))
}

func (e *Entry) Encode(w io.Writer) error {
	buf := getByteSlice()
	defer returnByteSlice(buf)

	buf[0] = byte(e.Type)
	binary.BigEndian.PutUint64(buf[1:], e.Term)
	binary.BigEndian.PutUint64(buf[9:], e.Index)
	if _, err := w.Write(buf[0:entry_header]); err != nil {
		return err
	}

	if len(e.Data) > 0 {
		if _, err := w.Write(e.Data); err != nil {
			return err
		}
	}
	return nil
}

func (e *Entry) Decode(datas []byte) {
	e.Type = EntryType(datas[0])
	e.Term = binary.BigEndian.Uint64(datas[1:])
	e.Index = binary.BigEndian.Uint64(datas[9:])
	if uint64(len(datas)) > entry_header {
		e.Data = datas[entry_header:]
	}
}

// Message codec
func (m *Message) entryAndContextSize() uint64 {
	size := uint64(4)
	if len(m.Entries) > 0 {
		for _, e := range m.Entries {
			size = size + e.Size() + 4
		}
	}
	if len(m.Context) > 0 {
		size = size + uint64(len(m.Context))
	}
	return size
}

func (m *Message) Encode(w io.Writer) error {
	buf := getByteSlice()
	defer returnByteSlice(buf)

	buf[0] = MsgMagic
	buf[1] = version1
	buf[2] = byte(m.Type)
	if m.ForceVote {
		buf[3] = 1
	} else {
		buf[3] = 0
	}
	if m.Reject {
		buf[4] = 1
	} else {
		buf[4] = 0
	}
	binary.BigEndian.PutUint64(buf[5:], m.RejectIndex)
	binary.BigEndian.PutUint64(buf[13:], m.ID)
	binary.BigEndian.PutUint64(buf[21:], m.From)
	binary.BigEndian.PutUint64(buf[29:], m.To)
	binary.BigEndian.PutUint64(buf[37:], m.Term)
	binary.BigEndian.PutUint64(buf[45:], m.LogTerm)
	binary.BigEndian.PutUint64(buf[53:], m.Index)
	binary.BigEndian.PutUint64(buf[61:], m.Commit)

	if m.Type == ReqMsgSnapShot {
		binary.BigEndian.PutUint32(buf[69:], uint32(m.SnapshotMeta.Size()))
	} else {
		binary.BigEndian.PutUint32(buf[69:], uint32(m.entryAndContextSize()))
	}

	if _, err := w.Write(buf[0:message_header]); err != nil {
		return err
	}

	if m.Type == ReqMsgSnapShot {
		return m.SnapshotMeta.Encode(w)
	}

	binary.BigEndian.PutUint32(buf, uint32(len(m.Entries)))
	if _, err := w.Write(buf[0:4]); err != nil {
		return err
	}
	if len(m.Entries) > 0 {
		for _, e := range m.Entries {
			binary.BigEndian.PutUint32(buf, uint32(e.Size()))
			if _, err := w.Write(buf[0:4]); err != nil {
				return err
			}
			if err := e.Encode(w); err != nil {
				return err
			}
		}
	}
	if len(m.Context) > 0 {
		if _, err := w.Write(m.Context); err != nil {
			return err
		}
	}
	return nil
}

func (m *Message) Decode(r *util.BufferReader) error {
	var (
		datas []byte
		err   error
	)
	if datas, err = r.ReadFull(int(message_header)); err != nil {
		return err
	}

	if MsgType(datas[0]) != MsgMagic {
		return fmt.Errorf("Invalid message magic")
	}

	ver := datas[1]
	if ver == version1 {
		m.Type = MsgType(datas[2])
		if m.Type >= MsgTypeEnd {
			return fmt.Errorf("Unknow message type %v", m.Type)

		}
		m.ForceVote = (datas[3] == 1)
		m.Reject = (datas[4] == 1)
		m.RejectIndex = binary.BigEndian.Uint64(datas[5:])
		m.ID = binary.BigEndian.Uint64(datas[13:])
		m.From = binary.BigEndian.Uint64(datas[21:])
		m.To = binary.BigEndian.Uint64(datas[29:])
		m.Term = binary.BigEndian.Uint64(datas[37:])
		m.LogTerm = binary.BigEndian.Uint64(datas[45:])
		m.Index = binary.BigEndian.Uint64(datas[53:])
		m.Commit = binary.BigEndian.Uint64(datas[61:])

		dataSize := binary.BigEndian.Uint32(datas[69:])
		if dataSize <= 0 {
			return nil
		}

		if datas, err = r.ReadFull(int(dataSize)); err != nil {
			return err
		}

		if m.Type == ReqMsgSnapShot {
			m.SnapshotMeta.Decode(datas[0:])
		} else {
			size := binary.BigEndian.Uint32(datas[0:])
			start := uint64(4)
			if size > 0 {
				for i := uint32(0); i < size; i++ {
					esize := binary.BigEndian.Uint32(datas[start:])
					start = start + 4
					end := start + uint64(esize)
					entry := new(Entry)
					entry.Decode(datas[start:end])
					m.Entries = append(m.Entries, entry)
					start = end
				}
			}
			if start < uint64(len(datas)) {
				m.Context = datas[start:]
			}
		}
	}
	return nil
}

func EncodeHBConext(ctx HeartbeatContext) (buf []byte) {
	sort.Slice(ctx, func(i, j int) bool {
		return ctx[i] < ctx[j]
	})

	scratch := make([]byte, binary.MaxVarintLen64)
	prev := uint64(0)
	for _, id := range ctx {
		n := binary.PutUvarint(scratch, id-prev)
		buf = append(buf, scratch[:n]...)
		prev = id
	}
	return
}

func DecodeHBContext(buf []byte) (ctx HeartbeatContext) {
	prev := uint64(0)
	for len(buf) > 0 {
		id, n := binary.Uvarint(buf)
		ctx = append(ctx, id+prev)
		prev = id + prev
		buf = buf[n:]
	}
	return
}
