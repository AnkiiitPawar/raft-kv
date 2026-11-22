package raft

import "sync"

// Persister stands in for stable storage. Tests "crash" a peer by killing it and starting a
// new one with a copy of its Persister, which is exactly the state a disk would have kept.
type Persister struct {
	mu        sync.Mutex
	raftState []byte
}

func NewPersister() *Persister { return &Persister{} }

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftState = append([]byte(nil), state...)
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return append([]byte(nil), ps.raftState...)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftState)
}

// Copy returns an independent persister with the same contents.
func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return &Persister{raftState: append([]byte(nil), ps.raftState...)}
}
