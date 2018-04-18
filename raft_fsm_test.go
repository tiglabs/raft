package raft

import (
	"math/rand"
	"testing"
	"time"

	"github.com/tiglabs/raft/proto"
)

func TestRemovePeer(t *testing.T) {
	peer := proto.Peer{
		ID:     1,
		PeerID: 10,
	}

	r := &raftFsm{
		config: &Config{
			NodeID:       1,
			ElectionTick: 10,
		},
		rand:     rand.New(rand.NewSource(1)),
		replicas: map[uint64]*replica{peer.ID: newReplica(peer, 100)},
	}

	removedPeer := proto.Peer{
		ID:     1,
		PeerID: 2,
	}

	r.removePeer(removedPeer)
	if len(r.replicas) != 1 {
		t.Errorf("expected replicas size = 1")
	}

	removedPeer.PeerID = peer.PeerID
	r.removePeer(removedPeer)
	if len(r.replicas) != 0 {
		t.Error("expected replicas size = 0")
	}

	time.Sleep(time.Second)
}
