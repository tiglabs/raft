package wal

import "github.com/tiglabs/raft/util"

const (
	DefaultFileCacheCapacity = 2
	DefaultFileSize          = 32 * util.MB
	DefaultSync              = false
)

// Config wal config
type Config struct {
	// FileCacheCapacity  缓存多少个打开的日志文件（包括index等）
	FileCacheCapacity int

	// FileSize 日志文件的大小
	FileSize int

	Sync bool

	// TruncateFirstDummy  初始化时添加一条日志然后截断
	TruncateFirstDummy bool
}

func (c *Config) GetFileCacheCapacity() int {
	if c == nil || c.FileCacheCapacity <= 0 {
		return DefaultFileCacheCapacity
	}
	return c.FileCacheCapacity
}

func (c *Config) GetFileSize() int {
	if c == nil || c.FileSize <= 0 {
		return DefaultFileSize
	}

	return c.FileSize
}

func (c *Config) GetSync() bool {
	if c == nil {
		return DefaultSync
	}
	return c.Sync
}

func (c *Config) GetTruncateFirstDummy() bool {
	if c == nil {
		return false
	}
	return c.TruncateFirstDummy
}

func (c *Config) dup() *Config {
	if c != nil {
		dc := *c
		return &dc
	} else {
		return nil
	}
}
