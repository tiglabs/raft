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

type readIndexStatus struct {
	index   uint64
	futures []*Future
	acks    map[uint64]struct{}
}

type readIndexPendings struct {
	status []readIndexStatus
}

func (p *readIndexPendings) add(index uint64, futures []*Future) {
	p.status = append(p.status, readIndexStatus{
		index:   index,
		futures: futures,
		acks:    make(map[uint64]struct{}),
	})
}

func (p *readIndexPendings) recvAck(index uint64, fromNodeID uint64) {
}

func (p *readIndexPendings) clearAndResp(err error) {
	for _, s := range p.status {
		for _, f := range s.futures {
			f.respond(nil, err)
		}
	}
	p.status = nil
}
