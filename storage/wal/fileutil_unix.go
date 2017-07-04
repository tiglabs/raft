// +build linux

package wal

import (
	"os"
	"syscall"
)

const (
	fallocateModeDefault  uint32 = 0 // 默认模式下预分配的空间全部补0
	fallocateModeKeepSize uint32 = 1 // 预分配后保持原来的文件大小，不补0
)

func fdatasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

// 预分配然后补零
func fallocate(f *os.File, sizeInBytes int64) error {
	err := syscall.Fallocate(int(f.Fd()), fallocateModeDefault, 0, sizeInBytes)
	if err != nil {
		errno, ok := err.(syscall.Errno)
		if ok && (errno == syscall.ENOTSUP || errno == syscall.EINTR) {
			return fallocDegraded(f, sizeInBytes)
		}
	}
	return err
}
