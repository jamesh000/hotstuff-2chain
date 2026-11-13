package state

import (
	"path/filepath"
	"testing"
	"time"

	jmt "github.com/jamesh000/SimpleJMT"
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/execution"
	"github.com/jamesh000/hotstuff-2chain/mempool"
	"github.com/jamesh000/hotstuff-2chain/store"
)

type harness struct {
	t     *testing.T
	dir   string
	store *store.Store
	state *Client
}

func newHarness(t *testing.T) *harness {
	dir := t.TempDir()
	s, err := store.NewStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}

	h := &harness{t: t, dir: dir, store: s}
	h.open()
	t.Cleanup(func() {
		h.state.Close()
		s.Close()
	})
	return h
}

func (h *harness) open() {
	c, err := Spawn(filepath.Join(h.dir, "state"), *h.store, execution.TestExecutor{})
	if err != nil {
		h.t.Fatal(err)
	}
	h.state = c
}

// batch builds a batch of transactions and returns its digest. If write is
// false the batch is not stored yet.
func (h *harness) batch(write bool, txs ...execution.Transaction) (crypto.Digest, []byte) {
	raw := make(mempool.Batch, 0, len(txs))
	for _, tx := range txs {
		raw = append(raw, tx.Serialize())
	}
	data := mempool.SerializeBatch(raw)
	digest := crypto.NewDigest(data)
	if write {
		h.store.Write(digest[:], data)
	}
	return digest, data
}

func (h *harness) root(digest crypto.Digest, round uint64) RootResult {
	select {
	case r := <-h.state.Root(digest, round):
		return r
	case <-time.After(5 * time.Second):
		h.t.Fatalf("timed out waiting for root of %v", digest)
		return RootResult{}
	}
}

func (h *harness) mustRoot(digest crypto.Digest, round uint64) crypto.Digest {
	r := h.root(digest, round)
	if r.Err != nil {
		h.t.Fatalf("root of %v: %v", digest, r.Err)
	}
	return r.Root
}

func block(name string, parent crypto.Digest, round uint64, payload ...crypto.Digest) BlockInfo {
	return BlockInfo{
		Digest:  crypto.NewDigest([]byte(name)),
		Parent:  parent,
		Round:   round,
		Payload: payload,
	}
}

func account(i byte) crypto.PublicKey {
	var pk crypto.PublicKey
	pk[0] = i
	return pk
}

// reference computes expected roots with an independent in-memory tree.
type reference struct {
	store   *jmt.MemStore
	tree    *jmt.JellyfishMerkleTree
	version jmt.Version
}

func newReference() *reference {
	s := jmt.NewMemStore()
	return &reference{s, jmt.NewJellyfishMerkleTree(s, s), 0}
}

func (r *reference) apply(t *testing.T, version jmt.Version, values map[crypto.PublicKey]uint64) crypto.Digest {
	updates := make(map[jmt.Hash]jmt.Hash)
	for pk, v := range values {
		updates[execution.AccountKey(pk)] = jmt.NewHash(execution.EncodeValue(v))
	}
	root, err := r.tree.BatchUpdate(r.version, version, updates)
	if err != nil {
		t.Fatal(err)
	}
	r.version = version
	return crypto.Digest(*root)
}

func TestLinearChain(t *testing.T) {
	h := newHarness(t)
	ref := newReference()
	a, b := account(1), account(2)

	if got := h.mustRoot(crypto.Digest{}, 0); got != crypto.Digest(jmt.EmptyRootHash) {
		t.Fatalf("genesis root %v, want empty root", got)
	}

	p1, _ := h.batch(true, execution.NewSetTransaction(a, 10), execution.NewIncrementTransaction(b))
	b1 := block("b1", crypto.Digest{}, 1, p1)
	h.state.Execute(b1)
	if got, want := h.mustRoot(b1.Digest, b1.Round), ref.apply(t, 1, map[crypto.PublicKey]uint64{a: 10, b: 1}); got != want {
		t.Fatalf("b1 root %v, want %v", got, want)
	}

	// increments read the parent state and earlier writes in the same block
	p2, _ := h.batch(true, execution.NewIncrementTransaction(a), execution.NewIncrementTransaction(a), execution.NewIncrementTransaction(b))
	b2 := block("b2", b1.Digest, 3, p2)
	h.state.Execute(b2)
	if got, want := h.mustRoot(b2.Digest, b2.Round), ref.apply(t, 3, map[crypto.PublicKey]uint64{a: 12, b: 2}); got != want {
		t.Fatalf("b2 root %v, want %v", got, want)
	}

	// empty block carries the root forward
	b3 := block("b3", b2.Digest, 4)
	h.state.Execute(b3)
	if got, want := h.mustRoot(b3.Digest, b3.Round), ref.apply(t, 4, nil); got != want {
		t.Fatalf("b3 root %v, want %v", got, want)
	}

	// commit part of the chain, then keep executing on top of disk state
	h.state.Commit(b1.Digest)
	h.state.Commit(b2.Digest)
	p4, _ := h.batch(true, execution.NewIncrementTransaction(a))
	b4 := block("b4", b3.Digest, 5, p4)
	h.state.Execute(b4)
	if got, want := h.mustRoot(b4.Digest, b4.Round), ref.apply(t, 5, map[crypto.PublicKey]uint64{a: 13}); got != want {
		t.Fatalf("b4 root %v, want %v", got, want)
	}

	// committed roots are still answered
	if got, want := h.mustRoot(b1.Digest, b1.Round), h.mustRoot(b1.Digest, b1.Round); got != want {
		t.Fatal("unstable root")
	}

	h.state.Commit(b3.Digest)
	h.state.Commit(b4.Digest)
	want := h.mustRoot(b4.Digest, b4.Round)

	// the committed state survives a restart
	h.state.Close()
	h.open()
	if got := h.mustRoot(b4.Digest, b4.Round); got != want {
		t.Fatalf("root after reopen %v, want %v", got, want)
	}

	p5, _ := h.batch(true, execution.NewIncrementTransaction(a))
	b5 := block("b5", b4.Digest, 6, p5)
	h.state.Execute(b5)
	if got, want := h.mustRoot(b5.Digest, b5.Round), ref.apply(t, 6, map[crypto.PublicKey]uint64{a: 14}); got != want {
		t.Fatalf("b5 root %v, want %v", got, want)
	}
}

func TestForks(t *testing.T) {
	h := newHarness(t)
	a := account(1)

	p1, _ := h.batch(true, execution.NewSetTransaction(a, 1))
	b1 := block("b1", crypto.Digest{}, 1, p1)
	h.state.Execute(b1)

	// two conflicting blocks in the same round (equivocation) and one in a
	// later round, all extending b1
	pa, _ := h.batch(true, execution.NewIncrementTransaction(a))
	pb, _ := h.batch(true, execution.NewSetTransaction(a, 100))
	forkA := block("forkA", b1.Digest, 2, pa)
	forkB := block("forkB", b1.Digest, 2, pb)
	forkC := block("forkC", b1.Digest, 3, pa)
	h.state.Execute(forkA)
	h.state.Execute(forkB)
	h.state.Execute(forkC)

	// children of each fork see their own fork's state
	pInc, _ := h.batch(true, execution.NewIncrementTransaction(a))
	childA := block("childA", forkA.Digest, 4, pInc)
	childB := block("childB", forkB.Digest, 4, pInc)
	h.state.Execute(childA)
	h.state.Execute(childB)

	refA := newReference()
	refA.apply(t, 1, map[crypto.PublicKey]uint64{a: 1})
	refB := newReference()
	refB.apply(t, 1, map[crypto.PublicKey]uint64{a: 1})

	if got, want := h.mustRoot(forkA.Digest, 2), refA.apply(t, 2, map[crypto.PublicKey]uint64{a: 2}); got != want {
		t.Fatalf("forkA root %v, want %v", got, want)
	}
	if got, want := h.mustRoot(forkB.Digest, 2), refB.apply(t, 2, map[crypto.PublicKey]uint64{a: 100}); got != want {
		t.Fatalf("forkB root %v, want %v", got, want)
	}
	if got, want := h.mustRoot(childA.Digest, 4), refA.apply(t, 4, map[crypto.PublicKey]uint64{a: 3}); got != want {
		t.Fatalf("childA root %v, want %v", got, want)
	}
	if got, want := h.mustRoot(childB.Digest, 4), refB.apply(t, 4, map[crypto.PublicKey]uint64{a: 101}); got != want {
		t.Fatalf("childB root %v, want %v", got, want)
	}

	// committing fork B prunes fork A and C
	h.state.Commit(b1.Digest)
	h.state.Commit(forkB.Digest)
	h.mustRoot(childB.Digest, 4)
	for _, b := range []BlockInfo{forkA, forkC, childA} {
		if r := h.root(b.Digest, b.Round); r.Err == nil {
			t.Fatalf("pruned block %v still has a root", b.Digest)
		}
	}

	// a late block extending a pruned fork is ignored
	late := block("late", forkA.Digest, 5)
	h.state.Execute(late)
	h.state.Commit(childB.Digest)
	if r := h.root(late.Digest, late.Round); r.Err == nil {
		t.Fatal("block extending a pruned fork has a root")
	}
}

func TestWaiting(t *testing.T) {
	h := newHarness(t)
	a := account(1)

	// payload not available yet
	p1, data := h.batch(false, execution.NewSetTransaction(a, 5))
	b1 := block("b1", crypto.Digest{}, 1, p1)
	b2 := block("b2", b1.Digest, 2)

	rootCh := h.state.Root(b2.Digest, b2.Round)
	h.state.Execute(b1)
	h.state.Execute(b2)
	h.state.Commit(b1.Digest)

	select {
	case <-rootCh:
		t.Fatal("got a root before the payload was available")
	case <-time.After(200 * time.Millisecond):
	}

	h.store.Write(p1[:], data)

	ref := newReference()
	ref.apply(t, 1, map[crypto.PublicKey]uint64{a: 5})
	want := ref.apply(t, 2, nil)

	select {
	case r := <-rootCh:
		if r.Err != nil || r.Root != want {
			t.Fatalf("b2 root %v (err %v), want %v", r.Root, r.Err, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for b2")
	}

	// the queued commit went through once b1 was executed; requests for an
	// unknown block below the committed tip fail instead of hanging
	h.state.Commit(b2.Digest)
	if r := h.root(crypto.NewDigest([]byte("unknown")), 1); r.Err == nil {
		t.Fatal("unknown old block has a root")
	}
}
