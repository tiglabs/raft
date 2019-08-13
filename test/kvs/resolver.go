// Copyright 2018 The tiglabs raft Authors.
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
// limitations under the License.package wal

package main

import (
	"fmt"

	"github.com/tiglabs/raft"
)

// ClusterResolver implement raft Resolver
type ClusterResolver struct {
	cfg *Config
}

func newClusterResolver(cfg *Config) *ClusterResolver {
	return &ClusterResolver{
		cfg: cfg,
	}
}

// NodeAddress get node address
func (r *ClusterResolver) NodeAddress(nodeID uint64, stype raft.SocketType) (addr string, err error) {
	node := r.cfg.FindClusterNode(nodeID)
	if node == nil {
		return "", fmt.Errorf("could not find node(%v) in cluster config:\n: %v", nodeID, r.cfg.String())
	}
	switch stype {
	case raft.HeartBeat:
		return fmt.Sprintf("%s:%d", node.Host, node.HeartbeatPort), nil
	case raft.Replicate:
		return fmt.Sprintf("%s:%d", node.Host, node.ReplicatePort), nil
	}
	return "", fmt.Errorf("unknown socket type: %v", stype)
}
