package kv

import (
	"crypto/rand"
	"math/big"
	mrand "math/rand"
	"time"

	"github.com/AnkiiitPawar/raft-kv/labnet"
)

// Clerk is a client. It remembers the last leader it reached and retries other servers on
// ErrWrongLeader, timeouts and lost messages. Each operation gets the next sequence number,
// which is what lets servers discard duplicates.
type Clerk struct {
	net      *labnet.Network
	me       int // the network slot this client calls from
	n        int
	leader   int
	clientID int64
	seq      int64

	// ReadFromAny sends Gets to a random replica first. Only meaningful together with the
	// servers' UnsafeLocalReads, for the negative-control test.
	ReadFromAny bool
	rng         *mrand.Rand
}

func randID() int64 {
	v, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	return v.Int64()
}

// MakeClerk returns a client that calls servers 0..n-1 from network slot `me`.
func MakeClerk(net *labnet.Network, me, n int) *Clerk {
	id := randID()
	return &Clerk{net: net, me: me, n: n, clientID: id, rng: mrand.New(mrand.NewSource(id))}
}

func (ck *Clerk) do(kind OpKind, key, value string) (string, Err) {
	ck.seq++
	op := Op{Kind: kind, Key: key, Value: value, ClientID: ck.clientID, Seq: ck.seq}
	first := ck.leader
	if ck.ReadFromAny && kind == Get {
		first = ck.rng.Intn(ck.n)
	}
	for {
		for k := 0; k < ck.n; k++ {
			s := (first + k) % ck.n
			r, ok := ck.net.Call(ck.me, s, "KV.Command", Args{Op: op})
			if !ok {
				continue
			}
			reply := r.(Reply)
			if reply.Err == OK || reply.Err == ErrNoKey {
				if !(ck.ReadFromAny && kind == Get) {
					ck.leader = s
				}
				return reply.Value, reply.Err
			}
		}
		time.Sleep(50 * time.Millisecond) // nobody is leader right now; wait for an election
	}
}

// Get returns the value of key, or "" if it has never been written.
func (ck *Clerk) Get(key string) string {
	v, _ := ck.do(Get, key, "")
	return v
}

func (ck *Clerk) Put(key, value string)    { ck.do(Put, key, value) }
func (ck *Clerk) Append(key, value string) { ck.do(Append, key, value) }
