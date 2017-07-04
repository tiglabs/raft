// +build darwin

package wal

import (
	"os"
	"syscall"
)

func fdatasync(f *os.File) error {
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), uintptr(syscall.F_FULLFSYNC), uintptr(0))
	if errno == 0 {
		return nil
	}
	return errno
}

func fallocate(f *os.File, sizeInBytes int64) error {
	return fallocDegraded(f, sizeInBytes)
}
