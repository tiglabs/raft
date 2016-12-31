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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"time"

	"github.com/gorilla/mux"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/logger"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
	"github.com/tiglabs/raft/util/log"
)

// DefaultClusterID the default cluster id, we have only one raft cluster
const DefaultClusterID = 1

// DefaultRequestTimeout default request timeout
const DefaultRequestTimeout = time.Second * 3

// Server the kv server
type Server struct {
	cfg    *Config
	nodeID uint64       // self node id
	node   *ClusterNode // self node

	hs *http.Server
	rs *raft.RaftServer
	db *leveldb.DB // we use leveldb to store key-value data

	leader uint64
}

// NewServer create kvs
func NewServer(nodeID uint64, cfg *Config) *Server {
	s := &Server{
		nodeID: nodeID,
		cfg:    cfg,
	}
	node := cfg.FindClusterNode(nodeID)
	if node == nil {
		log.Panic("could not find self node(%v) in cluster config: \n(%v)", nodeID, cfg.String())
	}
	s.node = node
	return s
}

// Run run server
func (s *Server) Run() {
	// init store
	s.initLeveldb()
	// start raft
	s.startRaft()
	// start http server
	s.startHTTPServer()
}

// Stop stop server
func (s *Server) Stop() {
	// stop http server
	if s.hs != nil {
		if err := s.hs.Shutdown(nil); err != nil {
			log.Error("shutdown http failed: %v", err)
		}
	}

	// stop raft server
	if s.rs != nil {
		s.rs.Stop()
	}

	// close leveldb
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			log.Error("close leveldb failed: %v", err)
		}
	}
}

func (s *Server) initLeveldb() {
	opt := &opt.Options{}
	dbPath := path.Join(s.cfg.ServerCfg.DataPath, "db")
	db, err := leveldb.OpenFile(dbPath, opt)
	if err != nil {
		log.Panic("init leveldb failed: %v, path: %v", err, dbPath)
	}
	s.db = db
	log.Info("init leveldb sucessfully. path: %v", dbPath)
}

func (s *Server) startRaft() {
	logger.SetLogger(log.GetFileLogger())

	// start raft server
	sc := raft.DefaultConfig()
	sc.NodeID = s.nodeID
	sc.Resolver = newClusterResolver(s.cfg)
	sc.TickInterval = time.Millisecond * 500
	sc.ReplicateAddr = fmt.Sprintf(":%d", s.node.ReplicatePort)
	sc.HeartbeatAddr = fmt.Sprintf(":%d", s.node.HeartbeatPort)
	rs, err := raft.NewRaftServer(sc)
	if err != nil {
		log.Panic("start raft server failed: %v", err)
	}
	s.rs = rs
	log.Info("raft server started.")

	// create raft
	walPath := path.Join(s.cfg.ServerCfg.DataPath, "wal")
	raftStore, err := wal.NewStorage(walPath, &wal.Config{})
	if err != nil {
		log.Panic("init raft log storage failed: %v", err)
	}
	rc := &raft.RaftConfig{
		ID:           DefaultClusterID,
		Storage:      raftStore,
		StateMachine: s,
	}
	for _, n := range s.cfg.ClusterCfg.Nodes {
		rc.Peers = append(rc.Peers, proto.Peer{
			Type:   proto.PeerNormal,
			ID:     n.NodeID,
			PeerID: n.NodeID,
		})
	}
	err = s.rs.CreateRaft(rc)
	if err != nil {
		log.Panic("create raft failed: %v", err)
	}
	log.Info("raft created.")
}

func (s *Server) startHTTPServer() {
	router := mux.NewRouter()
	router.HandleFunc("/kvs/{key}", s.Get).Methods("GET")
	router.HandleFunc("/kvs/{key}", s.Put).Methods("PUT")
	router.HandleFunc("/kvs/{key}", s.Delete).Methods("DELETE")

	addr := fmt.Sprintf(":%d", s.node.HTTPPort)
	s.hs = &http.Server{
		Addr:    addr,
		Handler: router,
	}
	err := s.hs.ListenAndServe()
	if err != nil {
		log.Panic("listen http on %v failed: %v", addr, err)
	}
	log.Info("http start listen on %v", addr)
}

// Apply implement raft StateMachine Apply method
func (s *Server) Apply(command []byte, index uint64) (interface{}, error) {
	log.Debug("apply command at index(%v): %v", index, string(command))

	cmd := new(Command)
	err := json.Unmarshal(command, cmd)
	if err != nil {
		return nil, fmt.Errorf("unmarshal command failed: %v", command)
	}
	switch cmd.OP {
	case CmdQuorumGet:
		return s.db.Get(cmd.Key, nil)
	case CmdPut:
		return nil, s.db.Put(cmd.Key, cmd.Value, nil)
	case CmdDelete:
		return nil, s.db.Delete(cmd.Key, nil)
	default:
		return nil, fmt.Errorf("invalid cmd type: %v", cmd.OP)
	}
}

// ApplyMemberChange implement raft.StateMachine
func (s *Server) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	return nil, errors.New("not supported")
}

// Snapshot implement raft.StateMachine
func (s *Server) Snapshot() (proto.Snapshot, error) {
	return nil, errors.New("not supported")
}

// ApplySnapshot implement raft.StateMachine
func (s *Server) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	return errors.New("not supported")
}

// HandleFatalEvent implement raft.StateMachine
func (s *Server) HandleFatalEvent(err *raft.FatalError) {
	log.Panic("raft fatal error: %v", err)
}

// HandleLeaderChange implement raft.StateMachine
func (s *Server) HandleLeaderChange(leader uint64) {
	log.Info("raft leader change to %v", leader)
	s.leader = leader
}

func (s *Server) replyNotLeader(w http.ResponseWriter) {
	leader, term := s.rs.LeaderTerm(DefaultClusterID)
	w.Header().Add("leader", fmt.Sprintf("%d", leader))
	w.Header().Add("term", fmt.Sprintf("%d", term))
	node := s.cfg.FindClusterNode(leader)
	if node != nil {
		w.Header().Add("leader-host", node.Host)
		w.Header().Add("leader-addr", fmt.Sprintf("%s:%d", node.Host, node.HTTPPort))
	} else {
		w.Header().Add("leader-host", "")
		w.Header().Add("leader-addr", "")
	}
	w.WriteHeader(http.StatusMovedPermanently)
}

func (s *Server) process(w http.ResponseWriter, op CmdType, key, value []byte) {
	cmd := &Command{
		OP:    op,
		Key:   key,
		Value: value,
	}
	log.Debug("start process command: %v", cmd)

	if s.leader != s.nodeID {
		s.replyNotLeader(w)
		return
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		log.Error("marshal raft command failed: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	f := s.rs.Submit(DefaultClusterID, data)
	respCh, errCh := f.AsyncResponse()
	select {
	case resp := <-respCh:
		switch r := resp.(type) {
		case []byte:
			w.Write(r)
		case nil:
		default:
			log.Error("unknown resp type: %T", r)
			w.WriteHeader(http.StatusInternalServerError)
		}
	case err := <-errCh:
		log.Error("process %v failed: %v", cmd.String(), err)
		if err == leveldb.ErrNotFound {
			log.Info("process %v not found", cmd.String())
			w.WriteHeader(http.StatusNotFound)
		} else if err == raft.ErrNotLeader {
			s.replyNotLeader(w)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	case <-time.After(DefaultRequestTimeout):
		log.Error("process %v timeout", cmd.String())
		w.WriteHeader(http.StatusRequestTimeout)
	}
}

func (s *Server) getByReadIndex(w http.ResponseWriter, key string) {
	if log.GetFileLogger().IsEnableDebug() {
		log.Debug("get %s by ReadIndex", key)
	}

	f := s.rs.ReadIndex(DefaultClusterID)
	respCh, errCh := f.AsyncResponse()
	select {
	case resp := <-respCh:
		if resp != nil {
			log.Error("process get %s failed: unexpected resp type: %T", key, resp)
			return
		}
		value, err := s.db.Get([]byte(key), nil)
		if err == leveldb.ErrNotFound {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.Write(value)
		}

	case err := <-errCh:
		log.Error("process get %s failed: %v", key, err)
		if err == raft.ErrNotLeader {
			s.replyNotLeader(w)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	case <-time.After(DefaultRequestTimeout):
		log.Error("process get %s timeout", key)
		w.WriteHeader(http.StatusRequestTimeout)
	}
}

// Get get a key
func (s *Server) Get(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]

	if err := r.ParseForm(); err != nil {
		log.Error("call ParseForm failed: %v", err)
		return
	}

	level := r.Form.Get("level")
	switch level {
	case "log":
		s.process(w, CmdQuorumGet, []byte(key), nil)
		return
	case "index":
		s.getByReadIndex(w, key)
		return
	default:
		value, err := s.db.Get([]byte(key), nil)
		if err == leveldb.ErrNotFound {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.Write(value)
		}
	}
}

// Put put
func (s *Server) Put(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]
	buf, _ := ioutil.ReadAll(r.Body)
	s.process(w, CmdPut, []byte(key), buf)
}

// Delete delete a key
func (s *Server) Delete(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]
	s.process(w, CmdDelete, []byte(key), nil)
}
