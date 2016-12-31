package util

import (
	"time"
)

const time_format = "2006-01-02 15:04:05.000"

type Uint64Slice []uint64

func (p Uint64Slice) Len() int           { return len(p) }
func (p Uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func Min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func Max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func FormatDate(t time.Time) string {
	return t.Format(time_format)
}

func FormatTimestamp(t int64) string {
	if t <= 0 {
		return ""
	}
	return time.Unix(0, t).Format(time_format)
}
