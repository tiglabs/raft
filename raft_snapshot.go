package raft

import (
	"encoding/binary"
	"io"

	"github.com/ipdcode/raft/proto"
	"github.com/ipdcode/raft/util"
)

type Snapshot interface {
	SnapIterator
	ApplyIndex() uint64
	Close()
}

type SnapIterator interface {
	// if error=io.EOF represent snapshot terminated.
	Next() ([]byte, error)
}

type snapshotResult struct {
	deferError
	stopCh chan struct{}
}

func newSnapshotResult() *snapshotResult {
	f := &snapshotResult{
		stopCh: make(chan struct{}),
	}
	f.init()
	return f
}

type snapshotRequest struct {
	deferError
	snapshotReader
	header *proto.Message
}

func newSnapshotRequest(m *proto.Message, r io.Reader) *snapshotRequest {
	f := &snapshotRequest{
		header:         m,
		snapshotReader: snapshotReader{reader: r, buffer: pool.getByteBuffer()},
	}
	f.init()
	return f
}

type snapshotReader struct {
	reader io.Reader
	buffer *util.ByteBuffer
	err    error
}

func (r *snapshotReader) Next() ([]byte, error) {
	if r.err != nil {
		return nil, r.err
	}

	// read size header
	r.buffer.Reset()
	buf := r.buffer.SliceBytes(4)
	if _, r.err = io.ReadFull(r.reader, buf); r.err != nil {
		return nil, r.err
	}
	size := uint64(binary.BigEndian.Uint32(buf))
	if size == 0 {
		r.err = io.EOF
		return nil, r.err
	}

	// read data
	r.buffer.Reset()
	buf = r.buffer.SliceBytes(size)
	if _, r.err = io.ReadFull(r.reader, buf); r.err != nil {
		return nil, r.err
	}

	return r.buffer.Bytes(), nil
}
