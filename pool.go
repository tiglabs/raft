package raft

import (
	"sync"

	"github.com/ipdcode/raft/util"
)

var pool = newPoolFactory()

type poolFactory struct {
	applyPool      *sync.Pool
	proposalPool   *sync.Pool
	byteBufferPool *sync.Pool
}

func newPoolFactory() *poolFactory {
	return &poolFactory{
		applyPool: &sync.Pool{
			New: func() interface{} {
				return new(apply)
			},
		},

		proposalPool: &sync.Pool{
			New: func() interface{} {
				return new(proposal)
			},
		},

		byteBufferPool: &sync.Pool{
			New: func() interface{} {
				return util.NewByteBuffer(16 * KB)
			},
		},
	}
}

func (f *poolFactory) getApply() *apply {
	return f.applyPool.Get().(*apply)
}

func (f *poolFactory) returnApply(a *apply) {
	if a == nil {
		return
	}

	a.command = nil
	a.future = nil
	f.applyPool.Put(a)
}

func (f *poolFactory) getProposal() *proposal {
	return f.proposalPool.Get().(*proposal)
}

func (f *poolFactory) returnProposal(p *proposal) {
	if p == nil {
		return
	}

	p.data = nil
	p.future = nil
	f.proposalPool.Put(p)
}

func (f *poolFactory) getByteBuffer() *util.ByteBuffer {
	buf := f.byteBufferPool.Get().(*util.ByteBuffer)
	buf.Reset()
	return buf
}

func (f *poolFactory) returnByteBuffer(b *util.ByteBuffer) {
	if b == nil {
		return
	}

	f.byteBufferPool.Put(b)
}
