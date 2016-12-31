// Copyright 2018 The TigLabs raft Authors.
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
	"flag"
	"fmt"

	"github.com/tiglabs/raft/util/log"
)

var nodeID = flag.Uint64("node", 0, "current node id")
var confFile = flag.String("conf", "", "config file path")

func main() {
	flag.Parse()

	// load config
	cfg := LoadConfig(*confFile, *nodeID)

	// init log
	log.InitFileLog(cfg.ServerCfg.LogPath, fmt.Sprintf("node%d", *nodeID), cfg.ServerCfg.LogLevel)

	// start server
	server := NewServer(*nodeID, cfg)
	server.Run()
}
