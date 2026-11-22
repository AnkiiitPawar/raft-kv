// Package kv is a linearizable key-value store replicated with Raft.
//
// Every operation, reads included, goes through the Raft log, so all replicas apply the same
// operations in the same order. Clients tag each request with (ClientID, Seq); a replica applies
// a given request at most once, so a client may safely retry after a timeout or a leader change.
package kv

import (
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AnkiiitPawar/raft-kv/labnet"
	"github.com/AnkiiitPawar/raft-kv/raft"
)

type OpKind int

const (
	Get OpKind = iota
	Put
	Append
)

// Op is the command stored in the Raft log.
type Op struct {
	Kind     OpKind
	Key      string
	Value    string
	ClientID int64
	Seq      int64
}

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
)

type Args struct{ Op Op }

type Reply struct {
	Err   Err
	Value string
}

func init() { gob.Register(Op{}) }

type result struct {
	op    Op
	value string
	err   Err
}

// Server is one replica. The Raft peer is at the same index on the same network.
type Server struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	data    map[string]string
	lastSeq map[int64]int64  // highest Seq applied per client
	lastRes map[int64]result // its result, for answering retries of it
	waiting map[int]chan result
	applied int

	// UnsafeLocalReads answers Gets from this replica's own state without going through the
	// log. It is wrong (a lagging follower returns stale data) and exists only so the tests can
	// show the linearizability checker catching it.
	UnsafeLocalReads bool
}

// StartServer creates a replica on top of a new Raft peer.
func StartServer(net *labnet.Network, me, n int, persister *raft.Persister) *Server {
	s := &Server{me: me, applyCh: make(chan raft.ApplyMsg), data: map[string]string{},
		lastSeq: map[int64]int64{}, lastRes: map[int64]result{}, waiting: map[int]chan result{}}
	s.rf = raft.Make(net, me, n, persister, s.applyCh)
	go s.applier()
	return s
}

func (s *Server) Raft() *raft.Raft { return s.rf }

func (s *Server) Kill() {
	atomic.StoreInt32(&s.dead, 1)
	s.rf.Kill()
}

func (s *Server) killed() bool { return atomic.LoadInt32(&s.dead) == 1 }

// Command submits an operation and waits for it to be applied at the index Raft assigned.
func (s *Server) Command(args Args) Reply {
	s.mu.Lock()
	if s.UnsafeLocalReads && args.Op.Kind == Get {
		v, ok := s.data[args.Op.Key]
		s.mu.Unlock()
		if !ok {
			return Reply{Err: ErrNoKey}
		}
		return Reply{Err: OK, Value: v}
	}
	if r, ok := s.lastRes[args.Op.ClientID]; ok && s.lastSeq[args.Op.ClientID] == args.Op.Seq {
		s.mu.Unlock()
		return Reply{Err: r.err, Value: r.value} // a retry of something already applied
	}
	s.mu.Unlock()

	// Start and register the waiter under one lock. Registering after Start would race with
	// the applier: on a fast network the entry can commit and be applied in between, and the
	// result would go to nobody, leaving the client to sit out its full timeout.
	s.mu.Lock()
	index, term, isLeader := s.rf.Start(args.Op)
	if !isLeader {
		s.mu.Unlock()
		return Reply{Err: ErrWrongLeader}
	}
	ch := make(chan result, 1)
	s.waiting[index] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.waiting, index)
		s.mu.Unlock()
	}()

	select {
	case r := <-ch:
		// Another leader's entry may have won this index; then ours was lost.
		if r.op.ClientID != args.Op.ClientID || r.op.Seq != args.Op.Seq {
			return Reply{Err: ErrWrongLeader}
		}
		return Reply{Err: r.err, Value: r.value}
	case <-time.After(800 * time.Millisecond):
		if t, _ := s.rf.GetState(); t != term {
			return Reply{Err: ErrWrongLeader}
		}
		return Reply{Err: ErrTimeout}
	}
}

func (s *Server) applier() {
	for msg := range s.applyCh {
		if s.killed() {
			continue // keep draining so Raft's applier never blocks
		}
		if !msg.CommandValid {
			continue
		}
		op := msg.Command.(Op)
		s.mu.Lock()
		var r result
		if seq, ok := s.lastSeq[op.ClientID]; ok && op.Seq <= seq {
			r = s.lastRes[op.ClientID] // duplicate: don't apply twice
			if op.Seq < seq {
				r = result{err: OK}
			}
			r.op = op
		} else {
			r = s.apply(op)
			s.lastSeq[op.ClientID] = op.Seq
			s.lastRes[op.ClientID] = r
		}
		s.applied = msg.CommandIndex
		if ch, ok := s.waiting[msg.CommandIndex]; ok {
			ch <- r
		}
		s.mu.Unlock()
	}
}

func (s *Server) apply(op Op) result {
	r := result{op: op, err: OK}
	switch op.Kind {
	case Get:
		v, ok := s.data[op.Key]
		if !ok {
			r.err = ErrNoKey
		}
		r.value = v
	case Put:
		s.data[op.Key] = op.Value
	case Append:
		s.data[op.Key] += op.Value
	}
	return r
}

// Dispatch routes both Raft and KV calls arriving for this server.
func (s *Server) Dispatch(method string, args any) any {
	if method == "KV.Command" {
		return s.Command(args.(Args))
	}
	return s.rf.Dispatch(method, args)
}
