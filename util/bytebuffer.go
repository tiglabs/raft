package util

import (
	"errors"
)

var ErrTooLarge = errors.New("ByteBuffer too large.")

// A ByteBuffer is a variable-sized byte buffer for marshaling data.
// The zero value for ByteBuffer is an empty buffer ready to use.
type ByteBuffer struct {
	buf []byte // contents are the bytes buf[0 : len(buf)]
}

func NewByteBuffer(size int) *ByteBuffer {
	b := &ByteBuffer{buf: make([]byte, size)}
	b.Reset()
	return b
}

func (b *ByteBuffer) Len() int { return len(b.buf) }

func (b *ByteBuffer) Cap() int { return cap(b.buf) }

func (b *ByteBuffer) Reset() { b.buf = b.buf[0:0] }

func (b *ByteBuffer) Bytes() []byte { return b.buf[0:] }

// SliceBytes return n bytes can be written to the buffer.
// If n is negative, Grow will panic.
// If the buffer can't grow it will panic with ErrTooLarge.
func (b *ByteBuffer) SliceBytes(n uint64) []byte {
	if n < 0 {
		panic("ByteBuffer.SliceBytes: negative count")
	}
	m := b.Grow(n)
	return b.buf[m : m+n]
}

// makeSlice allocates a slice of size n. If the allocation fails, it panics with ErrTooLarge.
func (b *ByteBuffer) makeSlice(n uint64) []byte {
	// If the make fails, give a known error.
	defer func() {
		if recover() != nil {
			panic(ErrTooLarge)
		}
	}()
	return make([]byte, n)
}

// grow grows the buffer to guarantee space for n more bytes.
// It returns the index where bytes should be written.
func (b *ByteBuffer) Grow(n uint64) uint64 {
	m := uint64(len(b.buf))
	if m+n > uint64(cap(b.buf)) {
		var buf []byte
		if b.buf == nil {
			buf = b.makeSlice(n)
		} else {
			buf = b.makeSlice(uint64(cap(b.buf)<<1) + n)
			copy(buf, b.buf[0:])
		}
		b.buf = buf
	}
	b.buf = b.buf[0 : m+n]
	return m
}
