package raft

import (
	"fmt"
)

type Status struct {
	id                uint64
	nodeID            uint64
	leader            uint64
	term              uint64
	index             uint64
	commit            uint64
	applied           uint64
	vote              uint64
	pendQueue         int
	recvQueue         int
	appQueue          int
	stopped           bool
	restoringSnapshot bool
	state             fsmState
	replicas          map[uint64]replica
}

func (s *Status) String() string {
	st := "running"
	if s.stopped {
		st = "stopped"
	} else if s.restoringSnapshot {
		st = "snapshot"
	}
	j := fmt.Sprintf(`{"id":"%v","nodeID":"%v","state":"%v","leader":"%v","term":"%v","index":"%v","commit":"%v","applied":"%v","vote":"%v","pendingQueue":"%v",
					"recvQueue":"%v","applyQueue":"%v","status":"%v","replication":{`, s.id, s.nodeID, s.state.String(), s.leader, s.term, s.index, s.commit, s.applied, s.vote, s.pendQueue, s.recvQueue, s.appQueue, st)
	if len(s.replicas) == 0 {
		j += "}}"
	} else {
		for k, v := range s.replicas {
			p := "false"
			if v.paused {
				p = "true"
			}
			subj := fmt.Sprintf(`"%v":{"match":"%v","next":"%v","state":"%v","paused":"%v","inflight":"%v","active":"%v"},`, k, v.match, v.next, v.state.String(), p, v.count, v.active)
			j += subj
		}
		j = j[:len(j)-1] + "}}"
	}
	return j
}
