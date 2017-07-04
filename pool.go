package raft

import (
	"sync"
)

var pool = newPoolFactory()

type poolFactory struct {
	applyPool    *sync.Pool
	proposalPool *sync.Pool
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
	}
}

func (f *poolFactory) getApply() *apply {
	a := f.applyPool.Get().(*apply)
	a.command = nil
	a.future = nil
	return a
}

func (f *poolFactory) returnApply(a *apply) {
	if a != nil {
		f.applyPool.Put(a)
	}
}

func (f *poolFactory) getProposal() *proposal {
	p := f.proposalPool.Get().(*proposal)
	p.data = nil
	p.future = nil
	return p
}

func (f *poolFactory) returnProposal(p *proposal) {
	if p != nil {
		f.proposalPool.Put(p)
	}
}
