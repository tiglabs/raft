package raft

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/ipdcode/raft/logger"
	"github.com/ipdcode/raft/proto"
	"github.com/ipdcode/raft/util"
)

type proposal struct {
	cmdType proto.EntryType
	future  *Future
	data    []byte
}

type apply struct {
	term    uint64
	index   uint64
	future  *Future
	command interface{}
}

type softState struct {
	leader uint64
	term   uint64
}

type peerState struct {
	peers map[uint64]proto.Peer
	mu    sync.RWMutex
}

func (s *peerState) change(c *proto.ConfChange) {
	s.mu.Lock()
	switch c.Type {
	case proto.ConfAddNode:
		s.peers[c.Peer.ID] = c.Peer
	case proto.ConfRemoveNode:
		delete(s.peers, c.Peer.ID)
	case proto.ConfUpdateNode:
		s.peers[c.Peer.ID] = c.Peer
	}
	s.mu.Unlock()
}

func (s *peerState) replace(peers []proto.Peer) {
	s.mu.Lock()
	s.peers = nil
	s.peers = make(map[uint64]proto.Peer)
	for _, p := range peers {
		s.peers[p.ID] = p
	}
	s.mu.Unlock()
}

func (s *peerState) get() (nodes []uint64) {
	s.mu.RLock()
	for n := range s.peers {
		nodes = append(nodes, n)
	}
	s.mu.RUnlock()
	return
}

type raft struct {
	raftFsm           *raftFsm
	config            *Config
	raftConfig        *RaftConfig
	restoringSnapshot util.AtomicBool
	curApplied        util.AtomicUInt64
	curSoftSt         unsafe.Pointer
	prevSoftSt        softState
	prevHardSt        proto.HardState
	peerState         peerState
	pending           map[uint64]*Future
	snapping          map[uint64]*snapshotStatus
	propc             chan *proposal
	applyc            chan *apply
	recvc             chan *proto.Message
	snapRecvc         chan *snapshotRequest
	truncatec         chan uint64
	statusc           chan chan *Status
	readyc            chan struct{}
	tickc             chan struct{}
	electc            chan struct{}
	stopc             chan struct{}
	done              chan struct{}
	mu                sync.Mutex
}

func newRaft(config *Config, raftConfig *RaftConfig) (*raft, error) {
	defer util.HandleCrash()

	if err := raftConfig.validate(); err != nil {
		return nil, err
	}

	r, err := newRaftFsm(config, raftConfig)
	if err != nil {
		return nil, err
	}

	raft := &raft{
		raftFsm:    r,
		config:     config,
		raftConfig: raftConfig,
		pending:    make(map[uint64]*Future),
		snapping:   make(map[uint64]*snapshotStatus),
		recvc:      make(chan *proto.Message, config.ReqBufferSize),
		applyc:     make(chan *apply, config.AppBufferSize),
		propc:      make(chan *proposal, 256),
		snapRecvc:  make(chan *snapshotRequest, 1),
		truncatec:  make(chan uint64, 1),
		statusc:    make(chan chan *Status, 1),
		tickc:      make(chan struct{}, 64),
		readyc:     make(chan struct{}, 1),
		electc:     make(chan struct{}, 1),
		stopc:      make(chan struct{}),
		done:       make(chan struct{}),
	}
	raft.curApplied.Set(r.raftLog.applied)
	raft.peerState.replace(raftConfig.Peers)

	util.RunWorker(raft.runApply, raft.handlePanic)
	util.RunWorker(raft.run, raft.handlePanic)
	return raft, nil
}

func (s *raft) stop() {
	select {
	case <-s.done:
		return
	default:
		s.doStop()
	}
	<-s.done
}

func (s *raft) doStop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.stopc:
		return
	default:
		close(s.stopc)
		s.restoringSnapshot.Set(false)
	}
}

func (s *raft) runApply() {
	defer func() {
		s.doStop()
		s.resetApply()
	}()

	loopCount := 0
	for {
		loopCount = loopCount + 1
		if loopCount > 16 {
			loopCount = 0
			runtime.Gosched()
		}

		select {
		case <-s.stopc:
			return

		case apply := <-s.applyc:
			if apply.index <= s.curApplied.Get() {
				continue
			}

			var (
				err  error
				resp interface{}
			)
			switch cmd := apply.command.(type) {
			case *proto.ConfChange:
				resp, err = s.raftConfig.StateMachine.ApplyMemberChange(cmd, apply.index)
			case []byte:
				resp, err = s.raftConfig.StateMachine.Apply(cmd, apply.index)
			}

			if apply.future != nil {
				apply.future.respond(resp, err)
			}
			s.curApplied.Set(apply.index)
			pool.returnApply(apply)
		}
	}
}

func (s *raft) run() {
	defer func() {
		s.doStop()
		s.resetPending(ErrStopped)
		s.stopSnapping()
		s.raftConfig.Storage.Close()
		close(s.done)
	}()

	s.prevHardSt.Term = s.raftFsm.term
	s.prevHardSt.Vote = s.raftFsm.vote
	s.prevHardSt.Commit = s.raftFsm.raftLog.committed
	s.maybeChange()

	loopCount := 0
	var readyc chan struct{}
	for {
		if readyc == nil && s.containsUpdate() {
			readyc = s.readyc
			readyc <- struct{}{}
		}

		select {
		case <-s.stopc:
			return

		case <-s.tickc:
			s.raftFsm.tick()
			s.maybeChange()

		case pr := <-s.propc:
			if s.raftFsm.leader != s.config.NodeID {
				pr.future.respond(nil, ErrNotLeader)
				pool.returnProposal(pr)
				break
			}

			msg := proto.GetMessage()
			msg.Type = proto.LocalMsgProp
			msg.From = s.config.NodeID
			starti := s.raftFsm.raftLog.lastIndex() + 1
			s.pending[starti] = pr.future
			msg.Entries = append(msg.Entries, &proto.Entry{Term: s.raftFsm.term, Index: starti, Type: pr.cmdType, Data: pr.data})
			pool.returnProposal(pr)

			flag := false
			for i := 1; i < 64; i++ {
				starti = starti + 1
				select {
				case pr := <-s.propc:
					s.pending[starti] = pr.future
					msg.Entries = append(msg.Entries, &proto.Entry{Term: s.raftFsm.term, Index: starti, Type: pr.cmdType, Data: pr.data})
					pool.returnProposal(pr)
				default:
					flag = true
				}
				if flag {
					break
				}
			}
			s.raftFsm.Step(msg)

		case m := <-s.recvc:
			if _, ok := s.raftFsm.replicas[m.From]; ok || (!m.IsResponseMsg() && m.Type != proto.ReqMsgVote) ||
				(m.Type == proto.ReqMsgVote && s.raftFsm.raftLog.isUpToDate(m.Index, m.LogTerm, 0, 0)) {
				switch m.Type {
				case proto.ReqMsgHeartBeat:
					if s.raftFsm.leader == m.From && m.From != s.config.NodeID {
						s.raftFsm.Step(m)
					}
				case proto.RespMsgHeartBeat:
					if s.raftFsm.leader == s.config.NodeID && m.From != s.config.NodeID {
						s.raftFsm.Step(m)
					}
				default:
					s.raftFsm.Step(m)
				}
				s.maybeChange()
			} else if logger.IsEnableWarn() && m.Type != proto.RespMsgHeartBeat {
				logger.Warn("[raft][%v term: %d] ignored a %s message without the replica from [%v term: %d].", s.raftFsm.id, s.raftFsm.term, m.Type, m.From, m.Term)
			}

		case snapReq := <-s.snapRecvc:
			s.handleSnapshot(snapReq)

		case <-readyc:
			s.persist()
			s.apply()
			s.advance()
			// Send all messages.
			for _, msg := range s.raftFsm.msgs {
				if msg.Type == proto.ReqMsgSnapShot {
					s.sendSnapshot(msg)
					continue
				}
				s.sendMessage(msg)
			}
			s.raftFsm.msgs = nil
			readyc = nil
			loopCount = loopCount + 1
			if loopCount >= 2 {
				loopCount = 0
				runtime.Gosched()
			}

		case <-s.electc:
			msg := proto.GetMessage()
			msg.Type = proto.LocalMsgHup
			msg.From = s.config.NodeID
			msg.ForceVote = true
			s.raftFsm.Step(msg)
			s.maybeChange()

		case c := <-s.statusc:
			c <- s.getStatus()

		case truncIndex := <-s.truncatec:
			func() {
				defer util.HandleCrash()

				if lasti, err := s.raftConfig.Storage.LastIndex(); err != nil {
					logger.Error("raft[%v] truncate failed to get last index from storage: %v", s.raftFsm.id, err)
				} else if lasti > s.config.RetainLogs {
					maxIndex := util.Min(truncIndex, lasti-s.config.RetainLogs)
					if err = s.raftConfig.Storage.Truncate(maxIndex); err != nil {
						logger.Error("raft[%v] truncate failed,error is: %v", s.raftFsm.id, err)
					}
				}
			}()
		}
	}
}

func (s *raft) tick() {
	if s.restoringSnapshot.Get() {
		return
	}

	select {
	case <-s.stopc:
	case s.tickc <- struct{}{}:
	default:
		return
	}
}

func (s *raft) propose(cmd []byte, future *Future) {
	if !s.isLeader() {
		future.respond(nil, ErrNotLeader)
		return
	}

	pr := pool.getProposal()
	pr.cmdType = proto.EntryNormal
	pr.data = cmd
	pr.future = future

	select {
	case <-s.stopc:
		future.respond(nil, ErrStopped)
	case s.propc <- pr:
	}
}

func (s *raft) proposeMemberChange(cc *proto.ConfChange, future *Future) {
	if !s.isLeader() {
		future.respond(nil, ErrNotLeader)
		return
	}

	pr := pool.getProposal()
	pr.cmdType = proto.EntryConfChange
	pr.future = future
	pr.data = cc.Encode()

	select {
	case <-s.stopc:
		future.respond(nil, ErrStopped)
	case s.propc <- pr:
	}
}

func (s *raft) reciveMessage(m *proto.Message) {
	if s.restoringSnapshot.Get() {
		return
	}

	select {
	case <-s.stopc:
	case s.recvc <- m:
	default:
		return
	}
}

func (s *raft) reciveSnapshot(m *snapshotRequest) {
	if s.restoringSnapshot.Get() {
		m.respond(ErrSnapping)
		return
	}

	select {
	case <-s.stopc:
		m.respond(ErrStopped)
		return
	case s.snapRecvc <- m:
	}
}

func (s *raft) status() *Status {
	if s.restoringSnapshot.Get() {
		return &Status{
			id:                s.raftFsm.id,
			nodeID:            s.config.NodeID,
			restoringSnapshot: true,
			state:             stateFollower,
		}
	}

	c := make(chan *Status, 1)
	select {
	case <-s.stopc:
		return nil
	case s.statusc <- c:
		return <-c
	}
}

func (s *raft) truncate(index uint64) {
	if s.restoringSnapshot.Get() {
		return
	}

	select {
	case <-s.stopc:
	case s.truncatec <- index:
	default:
		return
	}
}

func (s *raft) tryToLeader(future *Future) {
	if s.restoringSnapshot.Get() {
		future.respond(nil, nil)
		return
	}

	select {
	case <-s.stopc:
		future.respond(nil, ErrStopped)
	case s.electc <- struct{}{}:
		future.respond(nil, nil)
	}
}

func (s *raft) leaderTerm() (leader, term uint64) {
	st := (*softState)(atomic.LoadPointer(&s.curSoftSt))
	if st == nil {
		return NoLeader, 0
	}
	return st.leader, st.term
}

func (s *raft) isLeader() bool {
	leader, _ := s.leaderTerm()
	return leader == s.config.NodeID
}

func (s *raft) applied() uint64 {
	return s.curApplied.Get()
}

func (s *raft) sendMessage(m *proto.Message) {
	s.config.transport.Send(m)
}

func (s *raft) maybeChange() {
	updated := false
	if s.prevSoftSt.term != s.raftFsm.term {
		updated = true
		s.prevSoftSt.term = s.raftFsm.term
		s.resetTick()
	}
	preLeader := s.prevSoftSt.leader
	if preLeader != s.raftFsm.leader {
		updated = true
		s.prevSoftSt.leader = s.raftFsm.leader
		if s.raftFsm.leader != s.config.NodeID {
			s.resetPending(ErrNotLeader)
			s.stopSnapping()
		}
		if logger.IsEnableWarn() {
			if s.raftFsm.leader != NoLeader {
				if preLeader == NoLeader {
					logger.Warn("raft:[%v] elected leader %v at term %d.", s.raftFsm.id, s.raftFsm.leader, s.raftFsm.term)
				} else {
					logger.Warn("raft:[%v] changed leader from %v to %v at term %d.", s.raftFsm.id, preLeader, s.raftFsm.leader, s.raftFsm.term)
				}
			} else {
				logger.Warn("raft:[%v] lost leader %v at term %d.", s.raftFsm.id, preLeader, s.raftFsm.term)
			}
		}

		s.raftConfig.StateMachine.HandleLeaderChange(s.raftFsm.leader)
	}
	if updated {
		atomic.StorePointer(&s.curSoftSt, unsafe.Pointer(&softState{leader: s.raftFsm.leader, term: s.raftFsm.term}))
	}
}

func (s *raft) persist() {
	unstableEntries := s.raftFsm.raftLog.unstableEntries()
	if len(unstableEntries) > 0 {
		if err := s.raftConfig.Storage.StoreEntries(unstableEntries); err != nil {
			panic(AppPanicError(fmt.Sprintf("[raft->persist][%v] storage storeEntries err: [%v].", s.raftFsm.id, err)))
		}
	}
	if s.raftFsm.raftLog.committed != s.prevHardSt.Commit || s.raftFsm.term != s.prevHardSt.Term || s.raftFsm.vote != s.prevHardSt.Vote {
		hs := proto.HardState{Term: s.raftFsm.term, Vote: s.raftFsm.vote, Commit: s.raftFsm.raftLog.committed}
		if err := s.raftConfig.Storage.StoreHardState(hs); err != nil {
			panic(AppPanicError(fmt.Sprintf("[raft->persist][%v] storage storeHardState err: [%v].", s.raftFsm.id, err)))
		}
		s.prevHardSt = hs
	}
}

func (s *raft) apply() {
	committedEntries := s.raftFsm.raftLog.nextEnts(noLimit)
	for _, entry := range committedEntries {
		apply := pool.getApply()
		apply.term = entry.Term
		apply.index = entry.Index
		if future, ok := s.pending[entry.Index]; ok {
			apply.future = future
			delete(s.pending, entry.Index)
		}

		switch entry.Type {
		case proto.EntryNormal:
			if entry.Data == nil || len(entry.Data) == 0 {
				continue
			}
			apply.command = entry.Data

		case proto.EntryConfChange:
			cc := new(proto.ConfChange)
			cc.Decode(entry.Data)
			apply.command = cc
			// repl apply
			s.raftFsm.applyConfChange(cc)
			s.peerState.change(cc)
			if logger.IsEnableWarn() {
				logger.Warn("raft[%v] applying configuration change %v.", s.raftFsm.id, cc)
			}
		}
		select {
		case <-s.stopc:
			if apply.future != nil {
				apply.future.respond(nil, ErrStopped)
			}
		case s.applyc <- apply:
		}
	}
}

func (s *raft) advance() {
	s.raftFsm.raftLog.appliedTo(s.raftFsm.raftLog.committed)
	entries := s.raftFsm.raftLog.unstableEntries()
	if len(entries) > 0 {
		s.raftFsm.raftLog.stableTo(entries[len(entries)-1].Index, entries[len(entries)-1].Term)
	}
}

func (s *raft) containsUpdate() bool {
	return len(s.raftFsm.raftLog.unstableEntries()) > 0 || s.raftFsm.raftLog.committed > s.raftFsm.raftLog.applied || len(s.raftFsm.msgs) > 0 ||
		s.raftFsm.raftLog.committed != s.prevHardSt.Commit || s.raftFsm.term != s.prevHardSt.Term || s.raftFsm.vote != s.prevHardSt.Vote
}

func (s *raft) resetPending(err error) {
	if len(s.pending) > 0 {
		for k, v := range s.pending {
			v.respond(nil, err)
			delete(s.pending, k)
		}
	}
}

func (s *raft) resetTick() {
	for {
		select {
		case <-s.tickc:
		default:
			return
		}
	}
}

func (s *raft) resetApply() {
	for {
		select {
		case apply := <-s.applyc:
			if apply.future != nil {
				apply.future.respond(nil, ErrStopped)
			}
			pool.returnApply(apply)
		default:
			return
		}
	}
}

func (s *raft) getStatus() *Status {
	stopped := false
	select {
	case <-s.stopc:
		stopped = true
	default:
	}

	st := &Status{
		id:                s.raftFsm.id,
		nodeID:            s.config.NodeID,
		leader:            s.raftFsm.leader,
		term:              s.raftFsm.term,
		index:             s.raftFsm.raftLog.lastIndex(),
		commit:            s.raftFsm.raftLog.committed,
		applied:           s.curApplied.Get(),
		vote:              s.raftFsm.vote,
		state:             s.raftFsm.state,
		restoringSnapshot: s.restoringSnapshot.Get(),
		pendQueue:         len(s.pending),
		recvQueue:         len(s.recvc),
		appQueue:          len(s.applyc),
		stopped:           stopped,
	}
	if st.state == stateLeader {
		st.replicas = make(map[uint64]replica)
		for id, p := range s.raftFsm.replicas {
			st.replicas[id] = *p
		}
	}
	return st
}

func (s *raft) handlePanic(err interface{}) {
	fatalStopc <- s.raftFsm.id

	fatal := &FatalError{
		ID:  s.raftFsm.id,
		Err: fmt.Errorf("raft[%v] occur panic error: [%v].", s.raftFsm.id, err),
	}
	s.raftConfig.StateMachine.HandleFatalEvent(fatal)
}

func (s *raft) getPeers() (peers []uint64) {
	return s.peerState.get()
}
