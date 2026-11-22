package kv

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AnkiiitPawar/raft-kv/labnet"
	"github.com/AnkiiitPawar/raft-kv/linz"
	"github.com/AnkiiitPawar/raft-kv/raft"
)

// kvCluster runs n replicas on network slots 0..n-1; clients use slots n, n+1, ...
type kvCluster struct {
	t          *testing.T
	mu         sync.Mutex
	n          int
	net        *labnet.Network
	servers    []*Server
	persisters []*raft.Persister
	unsafe     bool
}

func newKV(t *testing.T, n, clients int, reliable bool) *kvCluster {
	c := &kvCluster{t: t, n: n, net: labnet.New(n+clients, 7), servers: make([]*Server, n),
		persisters: make([]*raft.Persister, n)}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.persisters[i] = raft.NewPersister()
		c.start(i)
	}
	t.Cleanup(func() {
		for i := 0; i < n; i++ {
			c.crash(i)
		}
	})
	return c
}

func (c *kvCluster) start(i int) {
	c.mu.Lock()
	c.persisters[i] = c.persisters[i].Copy()
	s := StartServer(c.net, i, c.n, c.persisters[i])
	s.UnsafeLocalReads = c.unsafe
	c.servers[i] = s
	c.mu.Unlock()
	c.net.Register(i, s)
}

func (c *kvCluster) crash(i int) {
	c.net.Register(i, nil)
	c.mu.Lock()
	s := c.servers[i]
	c.servers[i] = nil
	c.persisters[i] = c.persisters[i].Copy()
	c.mu.Unlock()
	if s != nil {
		s.Kill()
	}
}

func TestBasicPutGetAppend(t *testing.T) {
	c := newKV(t, 3, 1, true)
	ck := MakeClerk(c.net, 3, 3)
	if v := ck.Get("k"); v != "" {
		t.Fatalf("Get of a missing key = %q", v)
	}
	ck.Put("k", "a")
	ck.Append("k", "b")
	ck.Append("k", "c")
	if v := ck.Get("k"); v != "abc" {
		t.Fatalf("Get = %q, want abc", v)
	}
	ck.Put("k", "z")
	if v := ck.Get("k"); v != "z" {
		t.Fatalf("Get = %q, want z", v)
	}
}

func TestRetriedRequestIsAppliedOnce(t *testing.T) {
	c := newKV(t, 3, 1, true)
	ck := MakeClerk(c.net, 3, 3)
	ck.Put("k", "")
	ck.seq++
	op := Op{Kind: Append, Key: "k", Value: "x", ClientID: ck.clientID, Seq: ck.seq}
	for tries := 0; tries < 3; tries++ { // the same request, as a client retrying after a timeout
		for s := 0; s < 3; s++ {
			c.net.Call(3, s, "KV.Command", Args{Op: op})
		}
	}
	if v := ck.Get("k"); v != "x" {
		t.Fatalf("Get = %q: a retried Append was applied more than once", v)
	}
}

// run drives `clients` concurrent clerks over `keys` for `d`, recording a history, while the
// nemesis (if any) disturbs the cluster. It then heals everything and checks the history.
func run(t *testing.T, c *kvCluster, clients, keys int, d time.Duration, readAny bool, nemesis func(stop chan struct{})) ([]linz.Operation, []string) {
	start := time.Now()
	var mu sync.Mutex
	var history []linz.Operation
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var ops int64
	for cl := 0; cl < clients; cl++ {
		wg.Add(1)
		go func(cl int) {
			defer wg.Done()
			ck := MakeClerk(c.net, c.n+cl, c.n)
			ck.ReadFromAny = readAny
			rng := rand.New(rand.NewSource(int64(cl) + 100))
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("k%d", rng.Intn(keys))
				in := linz.KVInput{Key: key}
				var out linz.KVOutput
				call := time.Since(start).Nanoseconds()
				switch r := rng.Intn(10); {
				case r < 4:
					in.Kind, in.Value = linz.KVAppend, fmt.Sprintf("[%d.%d]", cl, i)
					ck.Append(key, in.Value)
				case r < 5:
					in.Kind, in.Value = linz.KVPut, fmt.Sprintf("<%d.%d>", cl, i)
					ck.Put(key, in.Value)
				default:
					in.Kind = linz.KVGet
					out.Value = ck.Get(key)
				}
				ret := time.Since(start).Nanoseconds()
				atomic.AddInt64(&ops, 1)
				mu.Lock()
				history = append(history, linz.Operation{ClientID: cl, Input: in, Output: out, Call: call, Return: ret})
				mu.Unlock()
			}
		}(cl)
	}
	nemesisDone := make(chan struct{})
	go func() {
		defer close(nemesisDone)
		if nemesis != nil {
			nemesis(stop)
		}
	}()
	time.Sleep(d)
	close(stop)
	<-nemesisDone
	for i := 0; i < c.n; i++ { // heal so that in-flight operations can finish
		c.net.Connect(i, true)
		c.mu.Lock()
		down := c.servers[i] == nil
		c.mu.Unlock()
		if down {
			c.start(i)
		}
	}
	c.net.SetReliable(true)
	wg.Wait()
	if atomic.LoadInt64(&ops) < int64(clients) {
		t.Fatalf("only %d operations completed: no progress", ops)
	}
	return history, linz.CheckKV(history)
}

func TestLinearizableReliable(t *testing.T) {
	c := newKV(t, 5, 5, true)
	h, bad := run(t, c, 5, 3, 3*time.Second, false, nil)
	t.Logf("%d operations", len(h))
	if bad != nil {
		t.Fatalf("history not linearizable for keys %v", bad)
	}
}

// Isolate a random minority of servers every 400 ms over an unreliable network.
func TestLinearizableWithPartitions(t *testing.T) {
	c := newKV(t, 5, 5, false)
	h, bad := run(t, c, 5, 3, 5*time.Second, false, func(stop chan struct{}) {
		rng := rand.New(rand.NewSource(11))
		for {
			select {
			case <-stop:
				return
			case <-time.After(400 * time.Millisecond):
			}
			for i := 0; i < c.n; i++ {
				c.net.Connect(i, true)
			}
			for _, i := range rng.Perm(c.n)[:rng.Intn(3)] {
				c.net.Connect(i, false)
			}
		}
	})
	t.Logf("%d operations", len(h))
	if bad != nil {
		t.Fatalf("history not linearizable for keys %v", bad)
	}
}

// Crash and restart random servers; they recover from their persisted Raft state.
func TestLinearizableWithCrashes(t *testing.T) {
	c := newKV(t, 5, 5, true)
	h, bad := run(t, c, 5, 3, 5*time.Second, false, func(stop chan struct{}) {
		rng := rand.New(rand.NewSource(12))
		for {
			select {
			case <-stop:
				return
			case <-time.After(500 * time.Millisecond):
			}
			for i := 0; i < c.n; i++ {
				c.mu.Lock()
				down := c.servers[i] == nil
				c.mu.Unlock()
				if down {
					c.start(i)
				}
			}
			for _, i := range rng.Perm(c.n)[:rng.Intn(3)] {
				c.crash(i)
			}
		}
	})
	t.Logf("%d operations", len(h))
	if bad != nil {
		t.Fatalf("history not linearizable for keys %v", bad)
	}
}

// The negative control: replicas answering Gets from their own, possibly lagging, state.
// The checker must notice. If this test ever passes vacuously, the other tests prove nothing.
func TestCheckerCatchesUnsafeLocalReads(t *testing.T) {
	c := newKV(t, 5, 5, true)
	c.unsafe = true
	for i := 0; i < c.n; i++ {
		c.servers[i].UnsafeLocalReads = true
	}
	_, bad := run(t, c, 5, 1, 3*time.Second, true, func(stop chan struct{}) {
		rng := rand.New(rand.NewSource(13))
		for { // keep followers lagging by isolating them for short spells
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
			}
			for i := 0; i < c.n; i++ {
				c.net.Connect(i, true)
			}
			c.net.Connect(rng.Intn(c.n), false)
		}
	})
	if bad == nil {
		t.Fatal("unsafe local reads produced a history the checker accepted")
	}
}
