package bufalloc

import (
	"sync"

	"github.com/tigcode/raft/util"
)

const (
	baseSize = 15
	bigSize  = 64 * util.KB
)

var buffPool *bufferPool

func init() {
	buffPool = &bufferPool{
		baseline: [...]int{64, 128, 256, 512, util.KB, 2 * util.KB, 4 * util.KB, 8 * util.KB, 16 * util.KB, 32 * util.KB, 64 * util.KB, 128 * util.KB, 256 * util.KB, 512 * util.KB, util.MB},
	}
	for i, n := range buffPool.baseline {
		buffPool.pool[i] = createPool(n)
	}
	buffPool.pool[baseSize] = createPool(0)
}

func createPool(n int) *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			if n == 0 || n > bigSize {
				return &ibuffer{}
			}
			return &ibuffer{buf: makeSlice(n)}
		},
	}
}

type bufferPool struct {
	baseline [baseSize]int
	pool     [baseSize + 1]*sync.Pool
}

func (p *bufferPool) getPoolNum(n int) int {
	for i, x := range p.baseline {
		if n <= x {
			return i
		}
	}
	return baseSize
}

func (p *bufferPool) getBuffer(n int) Buffer {
	num := p.getPoolNum(n)
	pool := p.pool[num]
	buf := pool.Get().(Buffer)
	if buf.Cap() < n {
		// return old buffer to pool
		buffPool.putBuffer(buf)
		buf = &ibuffer{buf: makeSlice(n)}
	}
	buf.Reset()
	return buf
}

func (p *bufferPool) putBuffer(buf Buffer) {
	num := p.getPoolNum(buf.Cap())
	pool := p.pool[num]
	pool.Put(buf)
}
