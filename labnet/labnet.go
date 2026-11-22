// Package labnet is an in-process network for testing distributed protocols.
//
// Servers register a Service; any server can Call another by index. The network can be made
// unreliable (dropped requests and replies, random delays), and servers can be disconnected
// (partitions) or removed entirely (crashes). Arguments and replies are passed by value; callers
// must not share mutable memory through them.
package labnet

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Service handles a call. It must be safe for concurrent use.
type Service interface {
	Dispatch(method string, args any) any
}

// Network connects n servers.
type Network struct {
	mu        sync.Mutex
	services  []Service
	connected []bool
	reliable  bool
	rng       *rand.Rand
	calls     int64
}

// New returns a reliable network with n slots, all connected and empty.
func New(n int, seed int64) *Network {
	nw := &Network{services: make([]Service, n), connected: make([]bool, n), reliable: true,
		rng: rand.New(rand.NewSource(seed))}
	for i := range nw.connected {
		nw.connected[i] = true
	}
	return nw
}

// Register installs (or with nil, removes) the service for server i.
func (nw *Network) Register(i int, s Service) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.services[i] = s
}

// Connect or disconnect server i from every other server.
func (nw *Network) Connect(i int, up bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.connected[i] = up
}

// Connected reports whether server i is connected.
func (nw *Network) Connected(i int) bool {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	return nw.connected[i]
}

// SetReliable turns message loss and delay off (true) or on (false).
func (nw *Network) SetReliable(r bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.reliable = r
}

// Calls is the total number of calls attempted so far.
func (nw *Network) Calls() int64 { return atomic.LoadInt64(&nw.calls) }

func (nw *Network) roll(p float64) bool {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	return nw.rng.Float64() < p
}

func (nw *Network) delay(max time.Duration) {
	nw.mu.Lock()
	d := time.Duration(nw.rng.Int63n(int64(max) + 1))
	nw.mu.Unlock()
	time.Sleep(d)
}

// Call delivers method(args) from server `from` to server `to` and returns the reply.
// ok is false if the call or its reply was lost; the caller can't tell which.
func (nw *Network) Call(from, to int, method string, args any) (reply any, ok bool) {
	atomic.AddInt64(&nw.calls, 1)
	nw.mu.Lock()
	up := nw.connected[from] && nw.connected[to]
	svc := nw.services[to]
	reliable := nw.reliable
	nw.mu.Unlock()

	if !up || svc == nil {
		// A real timeout takes a while; don't return instantly, or callers spin.
		nw.delay(30 * time.Millisecond)
		return nil, false
	}
	if !reliable {
		nw.delay(25 * time.Millisecond)
		if nw.roll(0.1) {
			return nil, false // request lost
		}
	}
	reply = svc.Dispatch(method, args)

	// The server may have been disconnected or crashed while handling the call.
	nw.mu.Lock()
	up = nw.connected[from] && nw.connected[to] && nw.services[to] == svc
	nw.mu.Unlock()
	if !up {
		return nil, false
	}
	if !reliable && nw.roll(0.1) {
		return nil, false // reply lost
	}
	return reply, true
}
