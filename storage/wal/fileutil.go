package wal

import (
	"errors"
	"fmt"
	"os"
	"sort"
)

// 目录初始化 不存在则创建；存在检查路径是否是目录
func initDir(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		if pathErr, ok := err.(*os.PathError); ok {
			if os.IsNotExist(pathErr) {
				return os.MkdirAll(dir, 0755)
			}
		}
		return err
	}

	if !info.IsDir() {
		return errors.New("fbase/raftstore: path is not directory")
	}

	return nil
}

// 日志文件名的组成 seq-index.log
type logFileName struct {
	seq   uint64 // 文件序号
	index uint64 // 起始index（log entry)
}

func (l *logFileName) String() string {
	return fmt.Sprintf("%016x-%016x.log", l.seq, l.index)
}

func (l *logFileName) ParseFrom(s string) bool {
	_, err := fmt.Sscanf(s, "%016x-%016x.log", &l.seq, &l.index)
	return err == nil
}

type nameSlice []logFileName

func (s nameSlice) Len() int           { return len(s) }
func (s nameSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s nameSlice) Less(i, j int) bool { return s[i].seq < s[j].seq }

// 枚举目录下的所有日志文件并按序号排序
func listLogEntryFiles(path string) (fnames []logFileName, err error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	names, err := dir.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	for _, name := range names {
		var n logFileName
		if n.ParseFrom(name) {
			fnames = append(fnames, n)
		}
	}
	sort.Sort(nameSlice(fnames))
	return
}

// 退化版本的预分配空间
func fallocDegraded(f *os.File, sizeInBytes int64) error {
	curOff, err := f.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	size, err := f.Seek(sizeInBytes, os.SEEK_END)
	if err != nil {
		return err
	}
	if _, err = f.Seek(curOff, os.SEEK_SET); err != nil {
		return err
	}
	if sizeInBytes > size {
		return nil
	}
	return f.Truncate(sizeInBytes)
}
