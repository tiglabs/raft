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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/BurntSushi/toml"
)

const defaultConfigStr = `
# KVS Configuration.

[server]
data-path = "/export/Data/raft-kvs"
log-path = "/export/Logs/raft-kvs"
log-level = "debug"

[cluster]
[[cluster.nodes]]
node-id=1
host="n1"
http-port=9991
heartbeat-port=9992
replicate-port=9993

[[cluster.nodes]]
node-id=2
host="n2"
http-port=9991
heartbeat-port=9992
replicate-port=9993

[[cluster.nodes]]
node-id=3
host="n3"
http-port=9991
heartbeat-port=9992
replicate-port=9993
`

// ServerConfig server config
type ServerConfig struct {
	LogPath  string `toml:"log-path,omitempty" json:"log-path"`
	LogLevel string `toml:"log-level,omitempty" json:"log-level"`
	DataPath string `toml:"data-path,omitempty" json:"data-path"`
}

// ClusterNode  cluster node
type ClusterNode struct {
	NodeID        uint64 `toml:"node-id,omitempty" json:"node-id"`
	Host          string `toml:"host,omitempty" json:"host"`
	HTTPPort      uint32 `toml:"http-port,omitempty" json:"http-port"`
	HeartbeatPort uint32 `toml:"heartbeat-port,omitempty" json:"heartbeat-port"`
	ReplicatePort uint32 `toml:"replicate-port,omitempty" json:"replicate-port"`
}

// ClusterConfig  cluster configs
type ClusterConfig struct {
	Nodes []*ClusterNode `toml:"nodes,omitempty" json:"nodes"`
}

// Config kvs config
type Config struct {
	ServerCfg  ServerConfig  `toml:"server,omitempty" json:"server"`
	ClusterCfg ClusterConfig `toml:"cluster,omitempty" json:"cluster"`
}

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
		return errors.New("path is not directory")
	}
	return nil
}

// Validate check config validate, add node id as dir prefix
func (c *Config) Validate(nodeID uint64) {
	if len(c.ServerCfg.LogPath) == 0 {
		panic("invalid log path")
	}
	c.ServerCfg.LogPath = path.Join(c.ServerCfg.LogPath, fmt.Sprintf("node%d", nodeID))
	if err := initDir(c.ServerCfg.LogPath); err != nil {
		panic(fmt.Sprintf("init log dir(%s) failed: %v", c.ServerCfg.LogPath, err))
	}

	if len(c.ServerCfg.DataPath) == 0 {
		panic("invalid data path")
	}
	c.ServerCfg.DataPath = path.Join(c.ServerCfg.DataPath, fmt.Sprintf("node%d", nodeID))
	if err := initDir(c.ServerCfg.DataPath); err != nil {
		panic(fmt.Sprintf("init data dir(%s) failed: %v", c.ServerCfg.DataPath, err))
	}

	if len(c.ClusterCfg.Nodes) == 0 {
		panic("cluster nodes is empty")
	}
}

// FindClusterNode find cluster node by NodeID
func (c *Config) FindClusterNode(NodeID uint64) *ClusterNode {
	if c == nil {
		return nil
	}
	for _, n := range c.ClusterCfg.Nodes {
		if n.NodeID == NodeID {
			return n
		}
	}
	return nil
}

func (c *Config) String() string {
	data, _ := json.Marshal(c)
	return string(data)
}

// LoadConfig load config from file
func LoadConfig(filePath string, nodeID uint64) *Config {
	c := new(Config)
	if _, err := toml.Decode(defaultConfigStr, c); err != nil {
		panic(fmt.Sprintf("fail to decode default config, err: %v", err))
	}
	if len(filePath) != 0 {
		_, err := toml.DecodeFile(filePath, c)
		if err != nil {
			panic(fmt.Sprintf("fail to decode config file(%v): %v", filePath, err))
		}
	}
	c.Validate(nodeID)
	return c
}
