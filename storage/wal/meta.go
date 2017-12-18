package wal

import (
	"encoding/binary"
	"io"
	"os"
	"path"

	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/util/bufalloc"
)

type truncateMeta struct {
	truncIndex uint64
	truncTerm  uint64
}

func (m truncateMeta) Size() uint64 {
	return 16
}

func (m truncateMeta) Encode(b []byte) {
	binary.BigEndian.PutUint64(b, m.truncIndex)
	binary.BigEndian.PutUint64(b[8:], m.truncTerm)
}

func (m *truncateMeta) Decode(b []byte) {
	m.truncIndex = binary.BigEndian.Uint64(b)
	m.truncTerm = binary.BigEndian.Uint64(b[8:])
}

// 存储HardState和truncateMeta信息
type metaFile struct {
	f           *os.File
	truncOffset int64
}

func openMetaFile(dir string) (mf *metaFile, hs proto.HardState, meta truncateMeta, err error) {
	f, err := os.OpenFile(path.Join(dir, "META"), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return
	}

	mf = &metaFile{
		f:           f,
		truncOffset: int64(hs.Size()),
	}

	hs, meta, err = mf.load()
	return mf, hs, meta, err
}

func (mf *metaFile) load() (hs proto.HardState, meta truncateMeta, err error) {
	// load hardstate
	hs_size := int(hs.Size())
	buffer := bufalloc.AllocBuffer(hs_size)
	defer bufalloc.FreeBuffer(buffer)

	buf := buffer.Alloc(hs_size)
	n, err := mf.f.Read(buf)
	if err != nil {
		if err == io.EOF {
			err = nil
			return
		}
		return
	}
	if n != hs_size {
		err = NewCorruptError("META", 0, "wrong hardstate data size")
		return
	}
	hs.Decode(buf)

	// load trunc meta
	buffer.Reset()
	mt_size := int(meta.Size())
	buf = buffer.Alloc(mt_size)
	n, err = mf.f.Read(buf)
	if err != nil {
		if err == io.EOF {
			err = nil
			return
		}
		return
	}
	if n != mt_size {
		err = NewCorruptError("META", 0, "wrong truncmeta data size")
		return
	}
	meta.Decode(buf)
	return
}

func (mf *metaFile) Close() error {
	return mf.f.Close()
}

func (mf *metaFile) SaveTruncateMeta(meta truncateMeta) error {
	mt_size := int(meta.Size())
	buffer := bufalloc.AllocBuffer(mt_size)
	defer bufalloc.FreeBuffer(buffer)

	b := buffer.Alloc(mt_size)
	meta.Encode(b)
	_, err := mf.f.WriteAt(b, mf.truncOffset)
	return err
}

func (mf *metaFile) SaveHardState(hs proto.HardState) error {
	hs_size := int(hs.Size())
	buffer := bufalloc.AllocBuffer(hs_size)
	defer bufalloc.FreeBuffer(buffer)

	b := buffer.Alloc(hs_size)
	hs.Encode(b)
	_, err := mf.f.WriteAt(b, 0)
	return err
}

func (mf *metaFile) Sync() error {
	return mf.f.Sync()
}
