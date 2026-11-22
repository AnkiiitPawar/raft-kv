package raft

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, true)
	c.checkOneLeader()
	time.Sleep(50 * time.Millisecond)
	term1 := c.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d, want at least 1", term1)
	}
	time.Sleep(2 * ElectionTimeoutMax) // no failures: the leader keeps its term
	if term2 := c.checkTerms(); term1 != term2 {
		t.Logf("warning: term changed from %d to %d with no failures", term1, term2)
	}
	c.checkOneLeader()
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3, true)
	leader1 := c.checkOneLeader()
	c.disconnect(leader1) // a new leader must take over
	c.checkOneLeader()
	c.connect(leader1) // the old leader rejoins and must not disturb the new one
	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	c.disconnect((leader2 + 1) % 3) // no quorum: no leader
	time.Sleep(2 * ElectionTimeoutMax)
	c.checkNoLeader()
	c.connect((leader2 + 1) % 3) // quorum again
	c.checkOneLeader()
	c.connect(leader2)
	c.checkOneLeader()
}

func TestManyElections(t *testing.T) {
	c := newCluster(t, 7, true)
	c.checkOneLeader()
	rng := rand.New(rand.NewSource(3))
	for iter := 0; iter < 8; iter++ {
		a, b, d := rng.Intn(7), rng.Intn(7), rng.Intn(7)
		c.disconnect(a)
		c.disconnect(b)
		c.disconnect(d)
		c.checkOneLeader() // at least four of seven remain: still a quorum
		c.connect(a)
		c.connect(b)
		c.connect(d)
	}
	c.checkOneLeader()
}

func TestBasicAgree(t *testing.T) {
	c := newCluster(t, 3, true)
	for index := 1; index <= 3; index++ {
		if nd, _ := c.nCommitted(index); nd > 0 {
			t.Fatal("something committed before Start()")
		}
		if got := c.one(index*100, 3, false); got != index {
			t.Fatalf("got index %d, want %d", got, index)
		}
	}
}

func TestFollowerFailure(t *testing.T) {
	c := newCluster(t, 3, true)
	c.one(101, 3, false)
	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % 3)
	c.one(102, 2, false) // two of three is a majority
	time.Sleep(ElectionTimeoutMax)
	c.one(103, 2, false)
	c.disconnect(leader)
	c.disconnect((leader + 2) % 3) // nobody left with a quorum
	index, _, ok := c.rafts[leader].Start(104)
	if !ok {
		t.Fatal("the disconnected leader should still accept Start")
	}
	time.Sleep(2 * ElectionTimeoutMax)
	if nd, _ := c.nCommitted(index); nd > 0 {
		t.Fatalf("%d peers committed without a majority", nd)
	}
}

func TestNoAgreementWithoutMajority(t *testing.T) {
	c := newCluster(t, 5, true)
	c.one(10, 5, false)
	leader := c.checkOneLeader()
	for k := 1; k <= 3; k++ {
		c.disconnect((leader + k) % 5)
	}
	index, _, ok := c.rafts[leader].Start(20)
	if !ok || index != 2 {
		t.Fatalf("Start: ok=%v index=%d", ok, index)
	}
	time.Sleep(2 * ElectionTimeoutMax)
	if nd, _ := c.nCommitted(index); nd > 0 {
		t.Fatalf("%d peers committed without a majority", nd)
	}
	for k := 1; k <= 3; k++ {
		c.connect((leader + k) % 5)
	}
	// The uncommitted entry may be lost or kept; either way agreement resumes.
	c.one(30, 5, true)
}

func TestRejoinOfPartitionedLeader(t *testing.T) {
	c := newCluster(t, 3, true)
	c.one(101, 3, true)
	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	c.rafts[leader1].Start(102) // these can never commit: only leader1 has them
	c.rafts[leader1].Start(103)
	c.rafts[leader1].Start(104)
	c.one(103, 2, true) // the new leader commits index 2
	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	c.connect(leader1) // the old leader returns with conflicting entries
	c.one(104, 2, true)
	c.connect(leader2)
	c.one(105, 3, true)
}

// TestBackup forces followers to discard many conflicting entries at once: the fast-backup
// path in AppendEntries must get them in sync in a reasonable number of round trips.
func TestBackup(t *testing.T) {
	c := newCluster(t, 5, true)
	c.one(rand.Int(), 5, true)
	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 2) % 5)
	c.disconnect((leader1 + 3) % 5)
	c.disconnect((leader1 + 4) % 5)
	for i := 0; i < 50; i++ { // 50 entries that will never commit
		c.rafts[leader1].Start(rand.Int())
	}
	time.Sleep(ElectionTimeoutMax / 2)
	c.disconnect(leader1)
	c.disconnect((leader1 + 1) % 5)
	c.connect((leader1 + 2) % 5)
	c.connect((leader1 + 3) % 5)
	c.connect((leader1 + 4) % 5)
	for i := 0; i < 50; i++ { // the other partition commits 50 entries
		c.one(rand.Int(), 3, true)
	}
	leader2 := c.checkOneLeader()
	other := (leader1 + 2) % 5
	if leader2 == other {
		other = (leader2 + 1) % 5
	}
	c.disconnect(other)
	for i := 0; i < 50; i++ { // 50 more that won't commit
		c.rafts[leader2].Start(rand.Int())
	}
	time.Sleep(ElectionTimeoutMax / 2)
	for i := 0; i < 5; i++ {
		c.disconnect(i)
	}
	c.connect(leader1)
	c.connect((leader1 + 1) % 5)
	c.connect(other)
	for i := 0; i < 50; i++ {
		c.one(rand.Int(), 3, true)
	}
	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	c.one(rand.Int(), 5, true)
}

func TestPersistence(t *testing.T) {
	c := newCluster(t, 3, true)
	c.one(11, 3, true)
	for i := 0; i < 3; i++ { // crash and restart everyone
		c.start(i)
	}
	c.one(12, 3, true)
	leader1 := c.checkOneLeader()
	c.start(leader1) // the leader crashes and comes back
	c.one(13, 3, true)
	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	c.crash(leader2)
	c.one(14, 2, true)
	c.start(leader2)
	c.connect(leader2)
	c.wait(4, 3, -1) // the restarted peer catches up on what it missed
	i3 := (c.checkOneLeader() + 1) % 3
	c.crash(i3)
	c.one(15, 2, true)
	c.start(i3)
	c.one(16, 3, true)
}

// TestFigure8Unreliable is the scenario from Figure 8 of the paper, made harder: random
// leaders crash after accepting entries, over a network that drops and delays messages.
// Safety is checked on every apply by the cluster harness.
func TestFigure8Unreliable(t *testing.T) {
	c := newCluster(t, 5, false)
	rng := rand.New(rand.NewSource(8))
	c.one(rng.Int()%10000, 1, true)
	up := 5
	for iters := 0; iters < 300; iters++ {
		if iters == 200 {
			c.net.SetReliable(true)
		}
		leader := -1
		for i := 0; i < 5; i++ {
			c.mu.Lock()
			rf := c.rafts[i]
			c.mu.Unlock()
			if rf != nil && c.net.Connected(i) {
				if _, _, ok := rf.Start(rng.Int() % 10000); ok {
					leader = i
				}
			}
		}
		if rng.Intn(1000) < 100 {
			time.Sleep(time.Duration(rng.Int63n(int64(ElectionTimeoutMax / 2))))
		} else {
			time.Sleep(time.Duration(rng.Int63n(13)) * time.Millisecond)
		}
		if leader != -1 && rng.Intn(1000) < int(1000)/2 {
			c.disconnect(leader)
			up--
		}
		if up < 3 {
			s := rng.Intn(5)
			if !c.net.Connected(s) {
				c.connect(s)
				up++
			}
		}
	}
	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	c.one(rng.Int()%10000, 5, true)
}

func TestConcurrentStarts(t *testing.T) {
	c := newCluster(t, 3, true)
	var wg sync.WaitGroup
	leader := c.checkOneLeader()
	term, _ := c.rafts[leader].GetState()
	indices := make(chan int, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if idx, t2, ok := c.rafts[leader].Start(100 + i); ok && t2 == term {
				indices <- idx
			}
		}(i)
	}
	wg.Wait()
	close(indices)
	seen := map[int]bool{}
	for idx := range indices {
		if seen[idx] {
			t.Fatalf("index %d handed out twice", idx)
		}
		seen[idx] = true
		c.wait(idx, 3, term)
	}
}

func TestUnreliableAgreement(t *testing.T) {
	c := newCluster(t, 5, false)
	var wg sync.WaitGroup
	for iter := 1; iter < 30; iter++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iter, j int) {
				defer wg.Done()
				c.one(100*iter+j, 1, true)
			}(iter, j)
		}
		c.one(iter, 1, true)
	}
	c.net.SetReliable(true)
	wg.Wait()
	c.one(100, 5, true)
}
