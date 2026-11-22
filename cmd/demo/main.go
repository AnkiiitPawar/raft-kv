// Command demo runs a simulated cluster through scripted failures and records a timeline,
// or measures throughput.
//
//	go run ./cmd/demo -mode timeline > docs/timeline.csv
//	go run ./cmd/demo -mode bench
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AnkiiitPawar/raft-kv/kv"
	"github.com/AnkiiitPawar/raft-kv/labnet"
	"github.com/AnkiiitPawar/raft-kv/raft"
)

type cluster struct {
	mu         sync.Mutex
	n          int
	net        *labnet.Network
	servers    []*kv.Server
	persisters []*raft.Persister
}

func newCluster(n, clients int) *cluster {
	c := &cluster{n: n, net: labnet.New(n+clients, 1), servers: make([]*kv.Server, n), persisters: make([]*raft.Persister, n)}
	for i := 0; i < n; i++ {
		c.persisters[i] = raft.NewPersister()
		c.start(i)
	}
	return c
}

func (c *cluster) start(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.persisters[i] = c.persisters[i].Copy()
	c.servers[i] = kv.StartServer(c.net, i, c.n, c.persisters[i])
	c.net.Register(i, c.servers[i])
}

func (c *cluster) crash(i int) {
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

func (c *cluster) leader() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	best, bestTerm := -1, -1
	for i, s := range c.servers {
		if s != nil && c.net.Connected(i) {
			if t, ok := s.Raft().GetState(); ok && t > bestTerm {
				best, bestTerm = i, t
			}
		}
	}
	return best
}

func timeline() {
	c := newCluster(5, 1)
	ck := kv.MakeClerk(c.net, 5, 5)
	var committed int64
	stop := make(chan struct{})
	go func() { // a client appending as fast as the cluster allows
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			ck.Append("log", ".")
			atomic.AddInt64(&committed, 1)
		}
	}()
	t0 := time.Now()
	events := []struct {
		at   time.Duration
		desc string
		do   func()
	}{
		{2 * time.Second, "isolate the leader", func() { c.net.Connect(c.leader(), false) }},
		{4 * time.Second, "reconnect everyone", func() {
			for i := 0; i < 5; i++ {
				c.net.Connect(i, true)
			}
		}},
		{6 * time.Second, "isolate the leader and one follower", func() {
			l := c.leader()
			c.net.Connect(l, false)
			c.net.Connect((l+1)%5, false)
		}},
		{8 * time.Second, "isolate a third server: no majority left", func() {
			for i := 0; i < 5; i++ {
				if c.net.Connected(i) {
					c.net.Connect(i, false)
					return
				}
			}
		}},
		{10 * time.Second, "reconnect everyone", func() {
			for i := 0; i < 5; i++ {
				c.net.Connect(i, true)
			}
		}},
		{12 * time.Second, "crash the leader", func() { c.crash(c.leader()) }},
		{14 * time.Second, "restart it from disk", func() {
			for i := 0; i < 5; i++ {
				c.mu.Lock()
				down := c.servers[i] == nil
				c.mu.Unlock()
				if down {
					c.start(i)
				}
			}
		}},
	}
	fmt.Println("t,node,term,role,alive,connected,ops")
	next := 0
	for time.Since(t0) < 16*time.Second {
		el := time.Since(t0)
		for next < len(events) && el >= events[next].at {
			fmt.Fprintf(os.Stderr, "%5.1fs  %s\n", el.Seconds(), events[next].desc)
			fmt.Printf("# %.2f,%s\n", el.Seconds(), events[next].desc)
			events[next].do()
			next++
		}
		for i := 0; i < 5; i++ {
			c.mu.Lock()
			s := c.servers[i]
			c.mu.Unlock()
			term, role, alive := 0, "down", 0
			if s != nil {
				var leader bool
				term, leader = s.Raft().GetState()
				role, alive = "follower", 1
				if leader {
					role = "leader"
				}
			}
			conn := 0
			if c.net.Connected(i) {
				conn = 1
			}
			fmt.Printf("%.2f,%d,%d,%s,%d,%d,%d\n", el.Seconds(), i, term, role, alive, conn, atomic.LoadInt64(&committed))
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(stop)
}

func bench() {
	for _, n := range []int{3, 5} {
		for _, clients := range []int{1, 8} {
			c := newCluster(n, clients)
			time.Sleep(1500 * time.Millisecond) // let a leader emerge
			var wg sync.WaitGroup
			var mu sync.Mutex
			var lat []time.Duration
			dur := 3 * time.Second
			t0 := time.Now()
			for k := 0; k < clients; k++ {
				wg.Add(1)
				go func(k int) {
					defer wg.Done()
					ck := kv.MakeClerk(c.net, n+k, n)
					for time.Since(t0) < dur {
						s := time.Now()
						ck.Put(fmt.Sprintf("k%d", k), "v")
						d := time.Since(s)
						mu.Lock()
						lat = append(lat, d)
						mu.Unlock()
					}
				}(k)
			}
			wg.Wait()
			el := time.Since(t0)
			sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
			fmt.Printf("%d servers, %d clients: %6.0f ops/s, median %5.2f ms, p99 %5.2f ms\n", n, clients,
				float64(len(lat))/el.Seconds(), float64(lat[len(lat)/2].Microseconds())/1000,
				float64(lat[len(lat)*99/100].Microseconds())/1000)
			for i := 0; i < n; i++ {
				c.crash(i)
			}
		}
	}
}

func main() {
	mode := flag.String("mode", "timeline", "timeline or bench")
	flag.Parse()
	if *mode == "bench" {
		bench()
		return
	}
	timeline()
}
