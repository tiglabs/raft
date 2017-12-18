package bufalloc

import (
	"testing"

	"github.com/tiglabs/raft/util"
)

func TestGetPoolNum(t *testing.T) {
	for i, n := range buffPool.baseline {
		num := buffPool.getPoolNum(n)
		if num != i {
			t.Errorf("Got %v expected %v", num, i)
		}
		num = buffPool.getPoolNum(n - 1)
		if num != i {
			t.Errorf("Got %v expected %v", num, i)
		}
	}
	num := buffPool.getPoolNum(2 * util.MB)
	if num != baseSize {
		t.Errorf("Got %v expected %v", num, baseSize)
	}
}

func TestGetBuffer(t *testing.T) {
	for _, n := range buffPool.baseline {
		buf := buffPool.getBuffer(n)
		if buf.Len() != 0 || buf.Cap() != n {
			t.Errorf("Got %v expected %v", buf.Cap(), n)
		}
		buffPool.putBuffer(buf)
	}
	buf := buffPool.getBuffer(2 * util.MB)
	if buf.Len() != 0 || buf.Cap() != 2*util.MB {
		t.Errorf("Got %v expected %v", buf.Cap(), 2*util.MB)
	}
	buffPool.putBuffer(buf)
}
