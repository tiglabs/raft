package wal

import (
	"github.com/google/btree"
	"github.com/ipdcode/raft/proto"
)

type cacheItem proto.Entry

func (c *cacheItem) Less(than btree.Item) bool {
	return c.Index < than.(*cacheItem).Index
}

// cache中只保持最新的(index较大的)若干条日志
type entryCache struct {
	capacity int
	ents     *btree.BTree
	key      *cacheItem
}

func newEntryCache(capacity int) *entryCache {
	return &entryCache{
		capacity: capacity,
		ents:     btree.New(4),
		key:      new(cacheItem),
	}
}

func (c *entryCache) Get(index uint64) *proto.Entry {
	c.key.Index = index
	ent := c.ents.Get(c.key)
	if ent != nil {
		return (*proto.Entry)(ent.(*cacheItem))
	} else {
		return nil
	}
}

func (c *entryCache) Append(ent *proto.Entry) {
	// 截断冲突的
	for c.ents.Len() > 0 && c.ents.Max().(*cacheItem).Index >= ent.Index {
		c.ents.DeleteMax()
	}

	c.ents.ReplaceOrInsert((*cacheItem)(ent))

	// keep capacity
	for c.ents.Len() > c.capacity {
		c.ents.DeleteMin()
	}
}
