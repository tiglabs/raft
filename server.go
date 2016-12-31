package raft

import (
	"errors"
	"sync"
	"time"

	"github.com/ipdcode/raft/logger"
	"github.com/ipdcode/raft/proto"
	"github.com/ipdcode/raft/util"
)

type RaftServer struct {
	config *Config
	ticker *time.Ticker
	heartc chan *proto.Message
	stopc  chan struct{}
	mu     sync.RWMutex
	rafts  map[uint64]*raft
}

func NewRaftServer(config *Config) (*RaftServer, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	rs := &RaftServer{
		config: config,
		ticker: time.NewTicker(config.TickInterval),
		rafts:  make(map[uint64]*raft),
		heartc: make(chan *proto.Message, 512),
		stopc:  make(chan struct{}),
	}
	if transport, err := NewMultiTransport(&config.TransportConfig, rs); err != nil {
		return nil, err
	} else {
		rs.config.transport = transport
	}

	util.RunWorkerUtilStop(rs.run, rs.stopc)
	return rs, nil
}

func (rs *RaftServer) run() {
	ticks := 0
	for {
		select {
		case <-rs.stopc:
			return

		case id := <-fatalStopc:
			rs.mu.Lock()
			delete(rs.rafts, id)
			rs.mu.Unlock()

		case m := <-rs.heartc:
			switch m.Type {
			case proto.ReqMsgHeartBeat:
				rs.handleHeartbeat(m)
			case proto.RespMsgHeartBeat:
				rs.handleHeartbeatResp(m)
			}

		case <-rs.ticker.C:
			rs.mu.RLock()
			for _, raft := range rs.rafts {
				raft.tick()
			}
			rs.mu.RUnlock()

			ticks++
			if ticks >= rs.config.HeartbeatTick {
				ticks = 0
				rs.sendHeartbeat()
			}
		}
	}
}

func (rs *RaftServer) Stop() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	select {
	case <-rs.stopc:
		return

	default:
		close(rs.stopc)
		rs.ticker.Stop()
		wg := new(sync.WaitGroup)
		for id, s := range rs.rafts {
			delete(rs.rafts, id)
			wg.Add(1)
			go func(r *raft) {
				defer wg.Done()
				r.stop()
			}(s)
		}
		wg.Wait()
		rs.config.transport.Close()
	}
}

func (rs *RaftServer) CreateRaft(raftConfig *RaftConfig) error {
	var (
		raft *raft
		err  error
	)

	defer func() {
		if err != nil {
			logger.Error("CreateRaft [%v] failed, error is:\r\n %s", raftConfig.ID, err.Error())
		}
	}()

	if raft, err = newRaft(rs.config, raftConfig); err != nil {
		return err
	}
	if raft == nil {
		err = errors.New("CreateRaft return nil, maybe occur panic.")
		return err
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()
	if _, ok := rs.rafts[raftConfig.ID]; ok {
		raft.stop()
		err = ErrRaftExists
		return err
	}
	rs.rafts[raftConfig.ID] = raft
	return nil
}

func (rs *RaftServer) RemoveRaft(id uint64) error {
	rs.mu.Lock()
	raft, ok := rs.rafts[id]
	delete(rs.rafts, id)
	rs.mu.Unlock()

	if ok {
		raft.stop()
	}
	return nil
}

func (rs *RaftServer) Submit(id uint64, cmd []byte) (future *Future) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	future = newFuture()
	if !ok {
		future.respond(nil, ErrRaftNotExists)
		return
	}
	raft.propose(cmd, future)
	return
}

func (rs *RaftServer) ChangeMember(id uint64, changeType proto.ConfChangeType, peer proto.Peer, context []byte) (future *Future) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	future = newFuture()
	if !ok {
		future.respond(nil, ErrRaftNotExists)
		return
	}
	raft.proposeMemberChange(&proto.ConfChange{Type: changeType, Peer: peer, Context: context}, future)
	return
}

func (rs *RaftServer) Status(id uint64) (status *Status) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if ok {
		status = raft.status()
	}
	if status == nil {
		status = &Status{
			id:      id,
			nodeID:  rs.config.NodeID,
			stopped: true,
		}
	}
	return
}

func (rs *RaftServer) Term(id uint64) uint64 {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if ok {
		return raft.term()
	}
	return 0
}

func (rs *RaftServer) Leader(id uint64) uint64 {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if ok {
		return raft.leader()
	}
	return NoLeader
}

func (rs *RaftServer) AppliedIndex(id uint64) uint64 {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if ok {
		return raft.applied()
	}
	return 0
}

func (rs *RaftServer) TryToLeader(id uint64) (future *Future) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	future = newFuture()
	if !ok {
		future.respond(nil, ErrRaftNotExists)
		return
	}
	raft.tryToLeader(future)
	return
}

func (rs *RaftServer) Truncate(id uint64, index uint64) {
	rs.mu.RLock()
	raft, ok := rs.rafts[id]
	rs.mu.RUnlock()

	if !ok {
		return
	}
	if err := raft.truncate(index); err != nil {
		logger.Error("raft[%v] truncate failed, error is:\r\n %s", id, err)
	}
}

func (rs *RaftServer) sendHeartbeat() {
	nodes := rs.config.Resolver.AllNodes()
	for _, nodeID := range nodes {
		if nodeID == rs.config.NodeID {
			continue
		}

		msg := proto.GetMessage()
		msg.Type = proto.ReqMsgHeartBeat
		msg.From = rs.config.NodeID
		msg.To = nodeID
		rs.config.transport.Send(msg)
	}
}

func (rs *RaftServer) handleHeartbeat(m *proto.Message) {
	rs.mu.RLock()
	for _, raft := range rs.rafts {
		raft.reciveMessage(m)
	}
	rs.mu.RUnlock()

	msg := proto.GetMessage()
	msg.Type = proto.RespMsgHeartBeat
	msg.From = rs.config.NodeID
	msg.To = m.From
	rs.config.transport.Send(msg)
}

func (rs *RaftServer) handleHeartbeatResp(m *proto.Message) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	for _, raft := range rs.rafts {
		raft.reciveMessage(m)
	}
}

func (rs *RaftServer) reciveMessage(m *proto.Message) {
	if m.Type == proto.ReqMsgHeartBeat || m.Type == proto.RespMsgHeartBeat {
		rs.heartc <- m
		return
	}

	rs.mu.RLock()
	raft, ok := rs.rafts[m.ID]
	rs.mu.RUnlock()
	if ok {
		raft.reciveMessage(m)
	}
}

func (rs *RaftServer) reciveSnapshot(req *snapshotRequest) {
	rs.mu.RLock()
	raft, ok := rs.rafts[req.header.ID]
	rs.mu.RUnlock()

	if !ok {
		req.respond(ErrRaftNotExists)
		return
	}
	raft.reciveSnapshot(req)
}
