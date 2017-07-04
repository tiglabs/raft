package util

import (
	"testing"
)

type byteReader struct {
	buf []byte
}

func newByteReader(b byte, n int) *byteReader {
	br := new(byteReader)
	br.buf = make([]byte, n)
	for i := 0; i < n; i++ {
		br.buf[i] = b
	}
	return br
}

func (r *byteReader) Read(p []byte) (n int, err error) {
	if len(r.buf) == 0 {
		return 0, nil
	}

	n = copy(p, r.buf)
	return n, nil
}

func (r *byteReader) Close() error {
	return nil
}

func TestFill(t *testing.T) {
	br := NewBufferReader(newByteReader('a', 100), 8*1024)
	if br.r != 0 || br.w != 0 || cap(br.buf) != 8*1024 {
		t.Fatalf("BufferReader status is wrong: [%d], [%d], [%d]", cap(br.buf), br.r, br.w)
	}

	br.fill()
	if br.r != 0 || br.w != 100 || cap(br.buf) != 8*1024 {
		t.Fatalf("BufferReader status is wrong: [%d], [%d], [%d]", cap(br.buf), br.r, br.w)
	}

	br.fill()
	if br.r != 0 || br.w != 200 || cap(br.buf) != 8*1024 {
		t.Fatalf("BufferReader status is wrong: [%d], [%d], [%d]", cap(br.buf), br.r, br.w)
	}

	br.reader = newByteReader('a', 8*1024)
	br.fill()
	if br.r != 0 || br.w != 8*1024 || cap(br.buf) != 8*1024 {
		t.Fatalf("BufferReader status is wrong: [%d], [%d], [%d]", cap(br.buf), br.r, br.w)
	}
	for i := 0; i < 8*1024; i++ {
		if br.buf[i] != 'a' {
			t.Fatal("BufferReader value is wrong!")
		}
	}

	br.r = 8*1024 - 10
	br.reader = newByteReader('b', 5)
	br.fill()
	if br.r != 0 || br.w != 15 || cap(br.buf) != 8*1024 {
		t.Fatalf("BufferReader status is wrong: [%d], [%d], [%d]", cap(br.buf), br.r, br.w)
	}
	for i := 0; i < 10; i++ {
		if br.buf[i] != 'a' {
			t.Fatal("BufferReader value is wrong!")
		}
	}
	for i := 10; i < 15; i++ {
		if br.buf[i] != 'b' {
			t.Fatal("BufferReader value is wrong!")
		}
	}
	br.reader = newByteReader('a', 8*1024)
	br.fill()

	br.r = 50
	br.reader = newByteReader('c', 10)
	br.fill()
	if br.r != 0 || br.w != 8*1024-40 || cap(br.buf) != 8*1024 {
		t.Fatalf("BufferReader status is wrong: [%d], [%d], [%d]", cap(br.buf), br.r, br.w)
	}
	for i := 0; i < 8*1024-50; i++ {
		if br.buf[i] != 'a' {
			t.Fatal("BufferReader value is wrong!")
		}
	}
	for i := 8*1024 - 50; i < 8*1024-40; i++ {
		if br.buf[i] != 'c' {
			t.Fatal("BufferReader value is wrong!")
		}
	}
}

func TestReadFull(t *testing.T) {
	br := NewBufferReader(newByteReader('e', 100), 8*1024)
	ret, _ := br.ReadFull(8 * 1024)
	if br.r != 8*1024 || br.w != 8*1024 || cap(br.buf) != 8*1024 {
		t.Fatalf("BufferReader status is wrong: [%d], [%d], [%d]", cap(br.buf), br.r, br.w)
	}
	for i := 0; i < 8*1024; i++ {
		if ret[i] != 'e' {
			t.Fatal("BufferReader readfull value is wrong!")
		}
	}

	br.reader = newByteReader('z', 100)
	ret, _ = br.ReadFull(8*1024 - 111)
	if br.r != 8*1024-111 || br.w != ((8*1024-111)/100+1)*100 || cap(br.buf) != 8*1024 {
		t.Fatalf("BufferReader status is wrong: [%d], [%d], [%d]", cap(br.buf), br.r, br.w)
	}
	for i := 0; i < 8*1024-111; i++ {
		if ret[i] != 'z' {
			t.Fatal("BufferReader readfull value is wrong!")
		}
	}
}

func TestReadFullWithReset(t *testing.T) {
	br := NewBufferReader(newByteReader('e', 100), 8*1024)
	ret, _ := br.ReadFull(8)
	if br.r != 8 || br.w != 100 || cap(br.buf) != 8*1024 {
		t.Fatalf("BufferReader status is wrong: [%d], [%d], [%d]", cap(br.buf), br.r, br.w)
	}
	for i := 0; i < 8; i++ {
		if ret[i] != 'e' {
			t.Fatal("BufferReader readfull value is wrong!")
		}
	}

	br.Reset()
	if br.r != 0 || br.w != 100-8 || cap(br.buf) != 8*1024 {
		t.Fatalf("BufferReader status is wrong: [%d], [%d], [%d]", cap(br.buf), br.r, br.w)
	}

	br.reader = newByteReader('z', 100)
	ret, _ = br.ReadFull(8 * 1024)
	if br.r != 8*1024 || br.w != 8*1024 || cap(br.buf) != 8*1024 {
		t.Fatalf("BufferReader status is wrong: [%d], [%d], [%d]", cap(br.buf), br.r, br.w)
	}
	for i := 0; i < 92; i++ {
		if ret[i] != 'e' {
			t.Fatal("BufferReader readfull value is wrong!")
		}
	}
	for i := 92; i < 8*1024; i++ {
		if ret[i] != 'z' {
			t.Fatal("BufferReader readfull value is wrong!")
		}
	}

	br.Reset()
	ret, _ = br.ReadFull(8)
	if br.r != 8 || br.w != 100 || cap(br.buf) != 8*1024 {
		t.Fatalf("BufferReader status is wrong: [%d], [%d], [%d]", cap(br.buf), br.r, br.w)
	}
	for i := 0; i < 8; i++ {
		if ret[i] != 'z' {
			t.Fatal("BufferReader readfull value is wrong!")
		}
	}
	br.Reset()
	if br.r != 0 || br.w != 100-8 || cap(br.buf) != 8*1024 {
		t.Fatalf("BufferReader status is wrong: [%d], [%d], [%d]", cap(br.buf), br.r, br.w)
	}
	br.reader = newByteReader('g', 100)
	ret, _ = br.ReadFull(2 * 8 * 1024)
	if br.r != 2*8*1024 || br.w != 2*8*1024 || cap(br.buf) != 2*8*1024 {
		t.Fatalf("BufferReader status is wrong: [%d], [%d], [%d]", cap(br.buf), br.r, br.w)
	}
	for i := 0; i < 92; i++ {
		if ret[i] != 'z' {
			t.Fatal("BufferReader readfull value is wrong!")
		}
	}
	for i := 92; i < 2*8*1024; i++ {
		if ret[i] != 'g' {
			t.Fatal("BufferReader readfull value is wrong!")
		}
	}
}
