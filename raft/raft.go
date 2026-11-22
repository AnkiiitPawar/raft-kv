// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout, 2014):
// leader election, log replication with the fast-backup optimisation, and persistence of the
// state that must survive a crash (currentTerm, votedFor, log).
//
// Names follow Figure 2 of the paper. Log indices start at 1; log[0] is a sentinel with term 0.
package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AnkiiitPawar/raft-kv/labnet"
)

const (
	HeartbeatInterval  = 50 * time.Millisecond
	ElectionTimeoutMin = 300 * time.Millisecond
	ElectionTimeoutMax = 600 * time.Millisecond
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// LogEntry is one replicated command.
type LogEntry struct {
	Term    int
	Command any
}

// ApplyMsg tells the service that a command is committed and should be applied.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int
	CommandTerm  int
}

type RequestVoteArgs struct {
	Term, CandidateID, LastLogIndex, LastLogTerm int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term, LeaderID, PrevLogIndex, PrevLogTerm, LeaderCommit int
	Entries                                                 []LogEntry
}

// AppendEntriesReply carries the conflict hint used to back nextIndex up a whole term at a time
// instead of one entry per round trip (§5.3, "the protocol can be optimized...").
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// Raft is one peer.
type Raft struct {
	mu        sync.Mutex
	me        int
	n         int
	net       *labnet.Network
	persister *Persister
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	dead      int32
	rng       *rand.Rand

	// Persistent state.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state.
	commitIndex, lastApplied int
	state                    State
	electionDeadline         time.Time

	// Leader state, reinitialised after each election.
	nextIndex, matchIndex []int
}

// Make creates a peer, restores any persisted state and starts its background goroutines.
// Committed commands are delivered on applyCh in log order.
func Make(net *labnet.Network, me, n int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{me: me, n: n, net: net, persister: persister, applyCh: applyCh, votedFor: -1,
		log: []LogEntry{{Term: 0}}, rng: rand.New(rand.NewSource(int64(me)*7919 + time.Now().UnixNano()))}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.readPersist(persister.ReadRaftState())
	rf.resetElectionTimer()
	go rf.ticker()
	go rf.applier()
	return rf
}

// GetState returns the current term and whether this peer believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// Start asks the peer to append a command to the replicated log. It returns immediately with
// the index the command will have if it is ever committed, the current term, and whether this
// peer is the leader. Commitment is not guaranteed: the leader may lose its term.
func (rf *Raft) Start(command any) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || rf.killed() {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist()
	index := rf.lastIndex()
	rf.matchIndex[rf.me] = index
	rf.broadcastAppendEntries()
	return index, rf.currentTerm, true
}

// Kill stops the peer's goroutines (a crash, as far as the protocol is concerned).
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool { return atomic.LoadInt32(&rf.dead) == 1 }

func (rf *Raft) lastIndex() int { return len(rf.log) - 1 }
func (rf *Raft) lastTerm() int  { return rf.log[len(rf.log)-1].Term }

// ---- persistence -------------------------------------------------------------------------

type persistent struct {
	CurrentTerm, VotedFor int
	Log                   []LogEntry
}

func (rf *Raft) persist() {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(persistent{rf.currentTerm, rf.votedFor, rf.log}); err != nil {
		panic(err)
	}
	rf.persister.SaveRaftState(buf.Bytes())
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	var p persistent
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&p); err != nil {
		panic(err)
	}
	rf.currentTerm, rf.votedFor, rf.log = p.CurrentTerm, p.VotedFor, p.Log
}

// ---- elections ---------------------------------------------------------------------------

func (rf *Raft) resetElectionTimer() {
	span := int64(ElectionTimeoutMax - ElectionTimeoutMin)
	rf.electionDeadline = time.Now().Add(ElectionTimeoutMin + time.Duration(rf.rng.Int63n(span)))
}

// becomeFollower must be called with the lock held.
func (rf *Raft) becomeFollower(term int) {
	if term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = term, -1
		rf.persist()
	}
	rf.state = Follower
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		switch {
		case rf.state == Leader:
			rf.broadcastAppendEntries()
			rf.mu.Unlock()
			time.Sleep(HeartbeatInterval)
			continue
		case time.Now().After(rf.electionDeadline):
			rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()
	term := rf.currentTerm
	args := RequestVoteArgs{Term: term, CandidateID: rf.me, LastLogIndex: rf.lastIndex(), LastLogTerm: rf.lastTerm()}
	votes := 1
	for p := 0; p < rf.n; p++ {
		if p == rf.me {
			continue
		}
		go func(p int) {
			r, ok := rf.net.Call(rf.me, p, "Raft.RequestVote", args)
			if !ok {
				return
			}
			reply := r.(RequestVoteReply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.state != Candidate || rf.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes > rf.n/2 {
				rf.becomeLeader()
			}
		}(p)
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, rf.n)
	rf.matchIndex = make([]int, rf.n)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastIndex() + 1
	}
	rf.matchIndex[rf.me] = rf.lastIndex()
	rf.broadcastAppendEntries()
}

// RequestVote is the vote handler (Figure 2, "RequestVote RPC: receiver implementation").
func (rf *Raft) RequestVote(args RequestVoteArgs) RequestVoteReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply := RequestVoteReply{Term: rf.currentTerm}
	if args.Term < rf.currentTerm {
		return reply
	}
	// §5.4.1: only vote for a candidate whose log is at least as up to date as ours.
	upToDate := args.LastLogTerm > rf.lastTerm() ||
		(args.LastLogTerm == rf.lastTerm() && args.LastLogIndex >= rf.lastIndex())
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		rf.persist()
		rf.resetElectionTimer()
		reply.VoteGranted = true
	}
	return reply
}

// ---- replication -------------------------------------------------------------------------

// broadcastAppendEntries sends each follower the entries it is missing (or a heartbeat).
// Must be called with the lock held.
func (rf *Raft) broadcastAppendEntries() {
	for p := 0; p < rf.n; p++ {
		if p == rf.me {
			continue
		}
		prev := rf.nextIndex[p] - 1
		entries := make([]LogEntry, len(rf.log)-(prev+1))
		copy(entries, rf.log[prev+1:])
		args := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me, PrevLogIndex: prev,
			PrevLogTerm: rf.log[prev].Term, LeaderCommit: rf.commitIndex, Entries: entries}
		go rf.sendAppendEntries(p, args)
	}
}

func (rf *Raft) sendAppendEntries(p int, args AppendEntriesArgs) {
	r, ok := rf.net.Call(rf.me, p, "Raft.AppendEntries", args)
	if !ok {
		return
	}
	reply := r.(AppendEntriesReply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.resetElectionTimer()
		return
	}
	if rf.state != Leader || rf.currentTerm != args.Term {
		return // a stale reply from an earlier term
	}
	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		if match > rf.matchIndex[p] {
			rf.matchIndex[p] = match
		}
		rf.nextIndex[p] = rf.matchIndex[p] + 1
		rf.advanceCommitIndex()
		return
	}
	// Fast backup: skip the follower's whole conflicting term, or to the end of its log.
	if reply.ConflictTerm == -1 {
		rf.nextIndex[p] = reply.ConflictIndex
	} else {
		next := reply.ConflictIndex
		for i := rf.lastIndex(); i > 0; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				next = i + 1
				break
			}
		}
		rf.nextIndex[p] = next
	}
	if rf.nextIndex[p] < 1 {
		rf.nextIndex[p] = 1
	}
}

// advanceCommitIndex commits the highest index replicated on a majority, but only if that
// entry is from the current term (§5.4.2: the "Figure 8" rule).
func (rf *Raft) advanceCommitIndex() {
	for N := rf.lastIndex(); N > rf.commitIndex; N-- {
		if rf.log[N].Term != rf.currentTerm {
			break
		}
		count := 0
		for _, m := range rf.matchIndex {
			if m >= N {
				count++
			}
		}
		if count > rf.n/2 {
			rf.commitIndex = N
			rf.applyCond.Broadcast()
			return
		}
	}
}

// AppendEntries is the replication and heartbeat handler (Figure 2).
func (rf *Raft) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := AppendEntriesReply{Term: rf.currentTerm}
	if args.Term < rf.currentTerm {
		return reply
	}
	rf.becomeFollower(args.Term)
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.lastIndex() {
		reply.ConflictIndex, reply.ConflictTerm = rf.lastIndex()+1, -1
		return reply
	}
	if t := rf.log[args.PrevLogIndex].Term; t != args.PrevLogTerm {
		first := args.PrevLogIndex
		for first > 1 && rf.log[first-1].Term == t {
			first--
		}
		reply.ConflictIndex, reply.ConflictTerm = first, t
		return reply
	}
	// Append new entries, truncating only on a real conflict: a delayed, shorter
	// AppendEntries must never cut off entries a newer one already added.
	changed := false
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= rf.lastIndex() {
			if rf.log[idx].Term == e.Term {
				continue
			}
			rf.log = rf.log[:idx]
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		changed = true
		break
	}
	if changed {
		rf.persist()
	}
	if args.LeaderCommit > rf.commitIndex {
		last := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, last)
		rf.applyCond.Broadcast()
	}
	reply.Success = true
	return reply
}

// ---- applying ----------------------------------------------------------------------------

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			continue
		}
		rf.lastApplied++
		e := rf.log[rf.lastApplied]
		msg := ApplyMsg{CommandValid: true, Command: e.Command, CommandIndex: rf.lastApplied, CommandTerm: e.Term}
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
	}
}

// Dispatch makes a Raft peer a labnet.Service.
func (rf *Raft) Dispatch(method string, args any) any {
	switch method {
	case "Raft.RequestVote":
		return rf.RequestVote(args.(RequestVoteArgs))
	case "Raft.AppendEntries":
		return rf.AppendEntries(args.(AppendEntriesArgs))
	}
	panic("raft: unknown method " + method)
}
