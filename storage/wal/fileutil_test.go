package wal

import (
	"io/ioutil"
	"os"
	"path"
	"syscall"
	"testing"
)

var datas = []byte("abcjklds;lsdgkldsjgkdlsjglsdjqroeioewrotl;sgkdjkgsjkdgjsk129428309583908593952abcjklds;lsdgkldsjgkdlsjglsdjqroeioewrotl;sgkdjkgsjkdgjsk129428309583908593952\n")

func BenchmarkFSync(b *testing.B) {
	dir, err := ioutil.TempDir(os.TempDir(), "fbase_test_rafts_sync_")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	f, err := os.Create(path.Join(dir, "test_fsync.data"))
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := f.Write(datas)
		if err != nil {
			b.Error(err)
		}
		err = f.Sync()
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkFDataSync(b *testing.B) {
	dir, err := ioutil.TempDir(os.TempDir(), "fbase_test_rafts_sync_")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	f, err := os.Create(path.Join(dir, "test_fdatasync.data"))
	err = syscall.Fallocate(int(f.Fd()), 0, 0, 1024*1024*10)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := f.Write(datas)
		if err != nil {
			b.Error(err)
		}
		err = syscall.Fdatasync(int(f.Fd()))
		if err != nil {
			b.Error(err)
		}
	}
}
