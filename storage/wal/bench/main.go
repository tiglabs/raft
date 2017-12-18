package main

import (
	"flag"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	"fmt"

	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
)

var n = flag.Int("n", 100000, "requests")
var l = flag.Int("l", 1024, "entry data length")

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomEntry(rnd *rand.Rand, index uint64) *proto.Entry {
	data := make([]byte, *l)
	for i := 0; i < *l; i++ {
		data[i] = letters[rnd.Int()%len(letters)]
	}

	ent := &proto.Entry{
		Index: index,
		Type:  proto.EntryNormal,
		Term:  uint64(rnd.Uint32()),
		Data:  data,
	}

	return ent
}

func main() {
	flag.Parse()

	dir, err := ioutil.TempDir(os.TempDir(), "db_bench_")
	if err != nil {
		panic(err)
	}

	s, err := wal.NewStorage(dir, nil)
	if err != nil {
		panic(err)
	}

	start := time.Now()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	reqs := *n
	for i := 0; i < reqs; i++ {
		ent := randomEntry(rnd, uint64(i))
		if err := s.StoreEntries([]*proto.Entry{ent}); err != nil {
			panic(err)
		}
	}
	spend := time.Since(start)
	ops := (int64(reqs) * 1000) / (spend.Nanoseconds() / 1000000)
	fmt.Printf("write %d entries spend: %v, ops: %v\n", reqs, spend, ops)
}
