package linz

import "testing"

func op(client int, kind KVKind, value, out string, call, ret int64) Operation {
	return Operation{ClientID: client, Input: KVInput{Kind: kind, Key: "x", Value: value},
		Output: KVOutput{Value: out}, Call: call, Return: ret}
}

func TestSequentialHistory(t *testing.T) {
	h := []Operation{
		op(0, KVPut, "a", "", 0, 10),
		op(0, KVAppend, "b", "", 20, 30),
		op(0, KVGet, "", "ab", 40, 50),
	}
	if !CheckSingle(KVRegister, h) {
		t.Fatal("a correct sequential history was rejected")
	}
}

func TestStaleReadIsCaught(t *testing.T) {
	// The Put finished before the Get started, so the Get must see it.
	h := []Operation{
		op(0, KVPut, "a", "", 0, 10),
		op(1, KVGet, "", "", 20, 30),
	}
	if CheckSingle(KVRegister, h) {
		t.Fatal("a stale read was accepted")
	}
}

func TestConcurrentOperationsMayBeOrderedEitherWay(t *testing.T) {
	// The Put and the first Get overlap: the Get may see either value.
	for _, seen := range []string{"", "a"} {
		h := []Operation{
			op(0, KVPut, "a", "", 0, 100),
			op(1, KVGet, "", seen, 50, 60),
		}
		if !CheckSingle(KVRegister, h) {
			t.Fatalf("an overlapping Get returning %q was rejected", seen)
		}
	}
}

func TestReadsMustNotGoBackInTime(t *testing.T) {
	// Two Gets during a long Put: once one Get has seen the new value, a later Get must too.
	h := []Operation{
		op(0, KVPut, "a", "", 0, 100),
		op(1, KVGet, "", "a", 10, 20),
		op(2, KVGet, "", "", 30, 40),
	}
	if CheckSingle(KVRegister, h) {
		t.Fatal("a read going back in time was accepted")
	}
}

func TestAppendsKeepOneOrder(t *testing.T) {
	// Two concurrent appends: readers may see "12" or "21", but not both.
	ok := []Operation{
		op(0, KVAppend, "1", "", 0, 50),
		op(1, KVAppend, "2", "", 0, 50),
		op(2, KVGet, "", "21", 60, 70),
		op(3, KVGet, "", "21", 80, 90),
	}
	if !CheckSingle(KVRegister, ok) {
		t.Fatal("a consistent order of concurrent appends was rejected")
	}
	bad := []Operation{
		op(0, KVAppend, "1", "", 0, 50),
		op(1, KVAppend, "2", "", 0, 50),
		op(2, KVGet, "", "21", 60, 70),
		op(3, KVGet, "", "12", 80, 90),
	}
	if CheckSingle(KVRegister, bad) {
		t.Fatal("two different orders of the same appends were accepted")
	}
}

func TestLostAppendIsCaught(t *testing.T) {
	h := []Operation{
		op(0, KVAppend, "1", "", 0, 10),
		op(1, KVAppend, "2", "", 20, 30),
		op(2, KVGet, "", "2", 40, 50),
	}
	if CheckSingle(KVRegister, h) {
		t.Fatal("a lost append was accepted")
	}
}

func TestKeysAreCheckedIndependently(t *testing.T) {
	h := []Operation{
		{Input: KVInput{Kind: KVPut, Key: "a", Value: "1"}, Output: KVOutput{}, Call: 0, Return: 10},
		{Input: KVInput{Kind: KVGet, Key: "b"}, Output: KVOutput{Value: ""}, Call: 20, Return: 30},
		{Input: KVInput{Kind: KVGet, Key: "a"}, Output: KVOutput{Value: "1"}, Call: 40, Return: 50},
		{Input: KVInput{Kind: KVGet, Key: "c"}, Output: KVOutput{Value: "zzz"}, Call: 60, Return: 70},
	}
	if bad := CheckKV(h); len(bad) != 1 || bad[0] != "c" {
		t.Fatalf("expected only key c to fail, got %v", bad)
	}
}

func TestLargeConcurrentHistoryIsFast(t *testing.T) {
	// 8 clients each appending 25 times with heavy overlap, then reads: a big search space
	// that the memoisation must keep tractable.
	var h []Operation
	var expect string
	for i := 0; i < 25; i++ {
		for c := 0; c < 8; c++ {
			v := string(rune('a' + c))
			h = append(h, op(c, KVAppend, v, "", int64(i*100), int64(i*100+90)))
		}
		order := "abcdefgh"
		expect += order
		h = append(h, op(9, KVGet, "", expect, int64(i*100+95), int64(i*100+99)))
	}
	if !CheckSingle(KVRegister, h) {
		t.Fatal("rejected a linearizable history")
	}
}
