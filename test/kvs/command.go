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
	"fmt"
)

// CmdType command type
type CmdType int

const (
	// CmdQuorumGet quorum get a key
	CmdQuorumGet CmdType = 1
	// CmdPut put key value
	CmdPut CmdType = 2
	// CmdDelete delete a key
	CmdDelete CmdType = 3
)

// Command a raft op command
type Command struct {
	OP    CmdType `json:"op"`
	Key   []byte  `json:"k"`
	Value []byte  `json:"v"`
}

func (c *Command) String() string {
	switch c.OP {
	case CmdQuorumGet:
		return fmt.Sprintf("QuorumGet %v", string(c.Key))
	case CmdPut:
		return fmt.Sprintf("Put %s %s", string(c.Key), string(c.Value))
	case CmdDelete:
		return fmt.Sprintf("Delete %s", string(c.Key))
	default:
		return "<Invalid>"
	}
}
