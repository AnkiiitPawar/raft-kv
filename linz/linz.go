// Package linz checks whether a concurrent history is linearizable (Herlihy & Wing, 1990):
// whether the operations can be put in one sequential order that respects real time (an
// operation that returned before another was called comes first) and in which every result
// is what a single copy of the data would have returned.
//
// The search is Wing & Gong's backtracking with Lowe's memoisation of (set of operations
// already linearized, model state), the algorithm also used by the Porcupine checker.
// For a key-value store, keys are independent, so each key's history is checked separately.
package linz

import (
	"hash/fnv"
	"sort"
)

// Operation is one completed call, with times from a monotonic clock.
type Operation struct {
	ClientID int
	Input    any
	Output   any
	Call     int64
	Return   int64
}

// Model is the sequential specification.
type Model struct {
	Init  func() any
	Step  func(state, input, output any) (ok bool, next any)
	Equal func(a, b any) bool
}

type node struct {
	value      any
	match      *node // for a call: its return; nil for a return
	id         int
	prev, next *node
}

type bitset []uint64

func newBitset(n int) bitset { return make(bitset, (n+63)/64) }
func (b bitset) set(i int)   { b[i/64] |= 1 << (uint(i) % 64) }
func (b bitset) clear(i int) { b[i/64] &^= 1 << (uint(i) % 64) }
func (b bitset) clone() bitset {
	c := make(bitset, len(b))
	copy(c, b)
	return c
}
func (b bitset) equals(c bitset) bool {
	for i := range b {
		if b[i] != c[i] {
			return false
		}
	}
	return true
}
func (b bitset) hash() uint64 {
	h := fnv.New64a()
	for _, w := range b {
		var buf [8]byte
		for k := 0; k < 8; k++ {
			buf[k] = byte(w >> (8 * k))
		}
		h.Write(buf[:])
	}
	return h.Sum64()
}

type cacheEntry struct {
	lin   bitset
	state any
}

// build turns a history into a time-ordered doubly linked list of call and return events.
// Calls sort before returns at equal times, which treats such operations as concurrent.
func build(history []Operation) *node {
	type ev struct {
		t      int64
		isCall bool
		id     int
		value  any
	}
	evs := make([]ev, 0, 2*len(history))
	for i, op := range history {
		evs = append(evs, ev{op.Call, true, i, op.Input}, ev{op.Return, false, i, op.Output})
	}
	sort.SliceStable(evs, func(a, b int) bool {
		if evs[a].t != evs[b].t {
			return evs[a].t < evs[b].t
		}
		return evs[a].isCall && !evs[b].isCall
	})
	head := &node{id: -1}
	tail := head
	calls := map[int]*node{}
	for _, e := range evs {
		n := &node{value: e.value, id: e.id, prev: tail}
		tail.next = n
		tail = n
		if e.isCall {
			calls[e.id] = n
		} else {
			calls[e.id].match = n
		}
	}
	return head
}

func lift(e *node) {
	e.prev.next = e.next
	e.next.prev = e.prev
	m := e.match
	m.prev.next = m.next
	if m.next != nil {
		m.next.prev = m.prev
	}
}

func unlift(e *node) {
	m := e.match
	m.prev.next = m
	if m.next != nil {
		m.next.prev = m
	}
	e.prev.next = e
	e.next.prev = e
}

// CheckSingle reports whether one history is linearizable with respect to the model.
func CheckSingle(model Model, history []Operation) bool {
	if len(history) == 0 {
		return true
	}
	head := build(history)
	lin := newBitset(len(history))
	cache := map[uint64][]cacheEntry{}
	type frame struct {
		entry *node
		state any
	}
	var calls []frame
	state := model.Init()
	entry := head.next
	for head.next != nil {
		if entry.match != nil { // a call: try to linearize it here
			ok, next := model.Step(state, entry.value, entry.match.value)
			if ok {
				nl := lin.clone()
				nl.set(entry.id)
				h := nl.hash()
				seen := false
				for _, c := range cache[h] {
					if c.lin.equals(nl) && model.Equal(c.state, next) {
						seen = true
						break
					}
				}
				if !seen {
					cache[h] = append(cache[h], cacheEntry{nl, next})
					calls = append(calls, frame{entry, state})
					state = next
					lin.set(entry.id)
					lift(entry)
					entry = head.next
					continue
				}
			}
			entry = entry.next
		} else { // reached a return whose call is not linearized yet: backtrack
			if len(calls) == 0 {
				return false
			}
			top := calls[len(calls)-1]
			calls = calls[:len(calls)-1]
			entry, state = top.entry, top.state
			lin.clear(entry.id)
			unlift(entry)
			entry = entry.next
		}
	}
	return true
}

// ---- the key-value model -------------------------------------------------------------------

type KVKind int

const (
	KVGet KVKind = iota
	KVPut
	KVAppend
)

type KVInput struct {
	Kind  KVKind
	Key   string
	Value string
}

type KVOutput struct{ Value string }

// KVRegister is the sequential specification of one key: Put sets it, Append extends it and
// Get must return its current value ("" if never written).
var KVRegister = Model{
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		s, in := state.(string), input.(KVInput)
		switch in.Kind {
		case KVGet:
			return output.(KVOutput).Value == s, s
		case KVPut:
			return true, in.Value
		default:
			return true, s + in.Value
		}
	},
	Equal: func(a, b any) bool { return a.(string) == b.(string) },
}

// CheckKV splits a key-value history by key and checks each part. It returns the keys whose
// histories are not linearizable (nil means the whole history is).
func CheckKV(history []Operation) []string {
	byKey := map[string][]Operation{}
	for _, op := range history {
		k := op.Input.(KVInput).Key
		byKey[k] = append(byKey[k], op)
	}
	var bad []string
	for k, ops := range byKey {
		if !CheckSingle(KVRegister, ops) {
			bad = append(bad, k)
		}
	}
	sort.Strings(bad)
	return bad
}
