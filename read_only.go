// Copyright 2015 The etcd Authors
// Modified work copyright 2018 The tiglabs Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

// ReadOnlyOption read only option
type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyLeaseBased
)

type readIndexStatus struct {
	index   uint64
	futures []*Future
	acks    map[uint64]struct{}
}

type readOnly struct {
	option           ReadOnlyOption
	pendingReadIndex map[uint64]*readIndexStatus
	readIndexQueue   []uint64
}

func newReadOnly(option ReadOnlyOption) *readOnly {
	return &readOnly{
		option:           option,
		pendingReadIndex: make(map[uint64]*readIndexStatus),
	}
}

func (r *readOnly) add(index uint64, futures []*Future) {
	if status, ok := r.pendingReadIndex[index]; ok {
		status.futures = append(status.futures, futures...)
		return
	}
	r.readIndexQueue = append(r.readIndexQueue, index)
	r.pendingReadIndex[index] = &readIndexStatus{
		index:   index,
		futures: futures,
		acks:    make(map[uint64]struct{}),
	}
}

func (r *readOnly) recvAck(index uint64, from uint64, qurom int) {
	status, ok := r.pendingReadIndex[index]
	if !ok {
		return
	}
	status.acks[from] = struct{}{}
	// add one to include an ack from local node
	if len(status.acks)+1 < qurom {
		return
	}

	// recv quorum ack
	var i int
	for _, idx := range r.readIndexQueue {
		i++
		if rs, ok := r.pendingReadIndex[idx]; ok {
			respondReadIndex(rs.futures, idx, nil)
			delete(r.pendingReadIndex, idx)
		}
		if idx == index {
			break
		}
	}
	r.readIndexQueue = r.readIndexQueue[i:]
}

func (r *readOnly) reset(err error) {
	if len(r.pendingReadIndex) > 0 {
		for _, status := range r.pendingReadIndex {
			respondReadIndex(status.futures, 0, err)
		}
	}
	r.pendingReadIndex = make(map[uint64]*readIndexStatus)
	r.readIndexQueue = nil
}
