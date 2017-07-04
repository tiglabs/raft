// +build freebsd openbsd netbsd dragonfly darwin linux

package log

import (
	"os"
	"syscall"
)

func logCrash(f *os.File) error {
	return syscall.Dup2(int(f.Fd()), 2)
}
