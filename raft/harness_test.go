package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/AnkiiitPawar/raft-kv/labnet"
)

// cluster drives n peers over a labnet and checks, on every apply, that no two peers ever
// commit different commands at the same index (the State Machine Safety property).
type cluster struct {
	t          *testing.T
	mu         sync.Mutex
	n          int
	net        *labnet.Network
	rafts      []*Raft
	persisters []*Persister
	logs       []map[int]any // per peer: index -> command applied
	failure    string
}

func newCluster(t *testing.T, n int, reliable bool) *cluster {
	c := &cluster{t: t, n: n, net: labnet.New(n, 1), rafts: make([]*Raft, n), persisters: make([]*Persister, n),
		logs: make([]map[int]any, n)}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.persisters[i] = NewPersister()
		c.start(i)
	}
	t.Cleanup(c.cleanup)
	return c
}

// start boots peer i from its persister (a fresh start, or a restart after crash).
func (c *cluster) start(i int) {
	c.crash(i)
	c.mu.Lock()
	c.persisters[i] = c.persisters[i].Copy()
	c.logs[i] = map[int]any{}
	ch := make(chan ApplyMsg)
	rf := Make(c.net, i, c.n, c.persisters[i], ch)
	c.rafts[i] = rf
	c.mu.Unlock()
	c.net.Register(i, rf)
	go c.applier(i, rf, ch)
}

func (c *cluster) applier(i int, rf *Raft, ch chan ApplyMsg) {
	for m := range ch {
		c.mu.Lock()
		if c.rafts[i] != rf { // a stale goroutine of a crashed incarnation; keep draining
			c.mu.Unlock()
			continue
		}
		for j := 0; j < c.n; j++ {
			if old, ok := c.logs[j][m.CommandIndex]; ok && old != m.Command && c.failure == "" {
				c.failure = fmt.Sprintf("peer %d committed %v at index %d but peer %d committed %v",
					i, m.Command, m.CommandIndex, j, old)
			}
		}
		if _, ok := c.logs[i][m.CommandIndex-1]; m.CommandIndex > 1 && !ok && c.failure == "" {
			c.failure = fmt.Sprintf("peer %d applied index %d out of order", i, m.CommandIndex)
		}
		c.logs[i][m.CommandIndex] = m.Command
		c.mu.Unlock()
	}
}

// crash kills peer i and keeps a copy of its persisted state for a later restart.
func (c *cluster) crash(i int) {
	c.net.Register(i, nil)
	c.mu.Lock()
	rf := c.rafts[i]
	c.rafts[i] = nil
	if c.persisters[i] != nil {
		c.persisters[i] = c.persisters[i].Copy()
	}
	c.mu.Unlock()
	if rf != nil {
		rf.Kill()
	}
}

func (c *cluster) cleanup() {
	for i := 0; i < c.n; i++ {
		c.crash(i)
	}
}

func (c *cluster) disconnect(i int) { c.net.Connect(i, false) }
func (c *cluster) connect(i int)    { c.net.Connect(i, true) }

func (c *cluster) checkFailure() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != "" {
		c.t.Fatal(c.failure)
	}
}

// checkOneLeader waits for exactly one leader among connected peers and returns it.
func (c *cluster) checkOneLeader() int {
	for iter := 0; iter < 10; iter++ {
		time.Sleep(450 * time.Millisecond)
		leaders := map[int][]int{}
		for i := 0; i < c.n; i++ {
			c.mu.Lock()
			rf := c.rafts[i]
			c.mu.Unlock()
			if rf != nil && c.net.Connected(i) {
				if term, isLeader := rf.GetState(); isLeader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}
		last := -1
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders", term, len(ls))
			}
			if term > last {
				last = term
			}
		}
		if len(leaders) > 0 {
			return leaders[last][0]
		}
	}
	c.t.Fatal("expected one leader, got none")
	return -1
}

func (c *cluster) checkNoLeader() {
	for i := 0; i < c.n; i++ {
		c.mu.Lock()
		rf := c.rafts[i]
		c.mu.Unlock()
		if rf != nil && c.net.Connected(i) {
			if _, isLeader := rf.GetState(); isLeader {
				c.t.Fatalf("expected no leader among connected servers, but %d claims to be one", i)
			}
		}
	}
}

func (c *cluster) checkTerms() int {
	term := -1
	for i := 0; i < c.n; i++ {
		c.mu.Lock()
		rf := c.rafts[i]
		c.mu.Unlock()
		if rf != nil && c.net.Connected(i) {
			t, _ := rf.GetState()
			if term == -1 {
				term = t
			} else if t != term {
				c.t.Fatal("connected servers disagree on the term")
			}
		}
	}
	return term
}

// nCommitted reports how many peers have applied index, and the command there.
func (c *cluster) nCommitted(index int) (int, any) {
	c.checkFailure()
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	var cmd any
	for i := 0; i < c.n; i++ {
		if v, ok := c.logs[i][index]; ok {
			count++
			cmd = v
		}
	}
	return count, cmd
}

// one submits cmd to whichever peer is leader and waits until at least `expected` peers have
// committed it. With retry, it resubmits if a leader fails before committing.
func (c *cluster) one(cmd any, expected int, retry bool) int {
	deadline := time.Now().Add(10 * time.Second)
	starts := 0
	for time.Now().Before(deadline) {
		index := -1
		for k := 0; k < c.n; k++ {
			starts = (starts + 1) % c.n
			c.mu.Lock()
			rf := c.rafts[starts]
			c.mu.Unlock()
			if rf != nil && c.net.Connected(starts) {
				if i, _, ok := rf.Start(cmd); ok {
					index = i
					break
				}
			}
		}
		if index != -1 {
			t1 := time.Now()
			for time.Since(t1) < 2*time.Second {
				if nd, got := c.nCommitted(index); nd > 0 && nd >= expected && got == cmd {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				c.t.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Fatalf("one(%v) failed to reach agreement", cmd)
	return -1
}

// wait until at least n peers have committed index, giving up after a few seconds.
func (c *cluster) wait(index, n int, startTerm int) any {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		if nd, _ := c.nCommitted(index); nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for _, rf := range c.rafts {
				if rf != nil {
					if t, _ := rf.GetState(); t > startTerm {
						return -1 // someone moved on; can't expect this index to commit
					}
				}
			}
		}
	}
	nd, cmd := c.nCommitted(index)
	if nd < n {
		c.t.Fatalf("only %d peers committed index %d, wanted %d", nd, index, n)
	}
	return cmd
}
