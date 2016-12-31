package util

import "sync/atomic"

type AtomicUInt64 struct {
	v uint64
}

func (a *AtomicUInt64) Get() uint64 {
	return atomic.LoadUint64(&a.v)
}

func (a *AtomicUInt64) Set(v uint64) {
	atomic.StoreUint64(&a.v, v)
}

func (a *AtomicUInt64) Add(v uint64) uint64 {
	return atomic.AddUint64(&a.v, v)
}

func (a *AtomicUInt64) Incr() uint64 {
	return atomic.AddUint64(&a.v, 1)
}

func (a *AtomicUInt64) CompareAndSwap(o, n uint64) bool {
	return atomic.CompareAndSwapUint64(&a.v, o, n)
}
