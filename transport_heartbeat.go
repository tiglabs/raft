package raft

import (
	"net"
	"sync"

	"github.com/ipdcode/raft/proto"
	"github.com/ipdcode/raft/util"
)

type heartbeatTransport struct {
	config     *TransportConfig
	raftServer *RaftServer
	listener   net.Listener
	mu         sync.RWMutex
	senders    map[uint64]*transportSender
	stopc      chan struct{}
}

func newHeartbeatTransport(raftServer *RaftServer, config *TransportConfig) (*heartbeatTransport, error) {
	var (
		listener net.Listener
		err      error
	)

	if listener, err = net.Listen("tcp", config.HeartbeatAddr); err != nil {
		return nil, err
	}
	t := &heartbeatTransport{
		config:     config,
		raftServer: raftServer,
		listener:   listener,
		senders:    make(map[uint64]*transportSender),
		stopc:      make(chan struct{}),
	}
	return t, nil
}

func (t *heartbeatTransport) stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	select {
	case <-t.stopc:
		return
	default:
		close(t.stopc)
		t.listener.Close()
		for _, s := range t.senders {
			s.stop()
		}
	}
}

func (t *heartbeatTransport) start() {
	util.RunWorkerUtilStop(func() {
		for {
			select {
			case <-t.stopc:
				return
			default:
				conn, err := t.listener.Accept()
				if err != nil {
					continue
				}
				t.handleConn(util.NewConnTimeout(conn))
			}
		}
	}, t.stopc)
}

func (t *heartbeatTransport) handleConn(conn *util.ConnTimeout) {
	util.RunWorker(func() {
		defer conn.Close()

		bufRd := util.NewBufferReader(conn, 16*KB)
		for {
			select {
			case <-t.stopc:
				return
			default:
				if msg, err := reciveMessage(bufRd); err != nil {
					return
				} else {
					t.raftServer.reciveMessage(msg)
				}
			}
		}
	})
}

func (t *heartbeatTransport) send(msg *proto.Message) {
	s := t.getSender(msg.To)
	s.send(msg)
}

func (t *heartbeatTransport) getSender(nodeId uint64) *transportSender {
	t.mu.RLock()
	sender, ok := t.senders[nodeId]
	t.mu.RUnlock()
	if ok {
		return sender
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if sender, ok = t.senders[nodeId]; !ok {
		sender = newTransportSender(nodeId, 1, 64, HeartBeat, t.config.Resolver)
		t.senders[nodeId] = sender
	}
	return sender
}
