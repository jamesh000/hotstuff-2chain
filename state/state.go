// Package state keeps the replicated state in a Jellyfish Merkle Tree.
//
// Blocks are executed speculatively as soon as consensus processes them, each
// on top of the state at the end of its parent. Each block's state is one JMT
// version (the block's round, which strictly increases along any chain).
// Uncommitted blocks live in in-memory overlays, so competing forks never
// interfere; when a block is committed its overlay is written to disk
// atomically and every fork that does not extend it is dropped.
package state

import (
	"fmt"
	"log"

	jmt "github.com/jamesh000/SimpleJMT"
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/execution"
	"github.com/jamesh000/hotstuff-2chain/mempool"
	"github.com/jamesh000/hotstuff-2chain/store"
)

const CHANNEL_CAPACITY = 1000

// BlockInfo is what the state module needs to know about a block.
type BlockInfo struct {
	Digest crypto.Digest
	// Digest of the parent block; the zero digest is the genesis block
	Parent  crypto.Digest
	Round   uint64
	Payload []crypto.Digest
}

// RootResult is the JMT root hash at the end of a block.
type RootResult struct {
	Root crypto.Digest
	Err  error
}

type message interface {
	stateMessageMember()
}

type executeMessage struct{ info BlockInfo }
type commitMessage struct{ digest crypto.Digest }
type rootMessage struct {
	digest crypto.Digest
	round  uint64
	reply  chan RootResult
}
type closeMessage struct{ done chan struct{} }

func (executeMessage) stateMessageMember() {}
func (commitMessage) stateMessageMember()  {}
func (rootMessage) stateMessageMember()    {}
func (closeMessage) stateMessageMember()   {}

// Client is the handle other components use to talk to the state module. It
// is safe for concurrent use. Messages are processed in the order they are
// sent.
type Client struct {
	ch chan<- message
}

// Execute schedules a block for speculative execution. The block's parent
// must be (or have been) sent before it.
func (c Client) Execute(info BlockInfo) {
	c.ch <- executeMessage{info}
}

// Commit marks a block as committed. Blocks must be committed in chain order.
func (c Client) Commit(digest crypto.Digest) {
	c.ch <- commitMessage{digest}
}

// Root returns a channel that receives the root hash at the end of the block
// once it has been executed. round is the round of the block; if the
// committed chain moves past it without including the block, an error is
// returned instead.
func (c Client) Root(digest crypto.Digest, round uint64) <-chan RootResult {
	reply := make(chan RootResult, 1)
	c.ch <- rootMessage{digest, round, reply}
	return reply
}

// Close stops the state module and closes its database.
func (c Client) Close() {
	done := make(chan struct{})
	c.ch <- closeMessage{done}
	<-done
}

type loadedBlock struct {
	info BlockInfo
	txs  [][]byte
	err  error
}

type rootWaiter struct {
	round uint64
	reply chan<- RootResult
}

type State struct {
	db       *db
	store    store.Store
	executor execution.Executor
	rx       <-chan message
	loaded   chan loadedBlock

	tip tip
	// executed, uncommitted blocks
	speculative map[crypto.Digest]*speculativeBlock
	// rounds of blocks received but not yet executed (or committed)
	pending map[crypto.Digest]uint64
	// loaded blocks waiting for their parent to be executed, by parent digest
	waitingParent map[crypto.Digest][]loadedBlock
	// commits waiting for their block to be executed, in order
	commitQueue []crypto.Digest
	// root requests for blocks that have not been executed yet
	waiters map[crypto.Digest][]rootWaiter
	// rounds of blocks that were dropped (failed to execute or pruned)
	dropped map[crypto.Digest]uint64
}

// Spawn opens (or creates) the state database at path and starts the state
// module. Block payloads are read from store.
func Spawn(path string, store store.Store, executor execution.Executor) (*Client, error) {
	db, err := openDB(path)
	if err != nil {
		return nil, err
	}

	t, err := db.loadTip()
	if err != nil {
		db.close()
		return nil, err
	}

	if t.Version != 0 {
		log.Printf("Loaded committed state at version %d (block %v, root %v). "+
			"Consensus does not recover after a restart, so new blocks may not extend it\n",
			t.Version, t.Digest, t.Root)
	}

	ch := make(chan message, CHANNEL_CAPACITY)

	s := &State{
		db:            db,
		store:         store,
		executor:      executor,
		rx:            ch,
		loaded:        make(chan loadedBlock, CHANNEL_CAPACITY),
		tip:           t,
		speculative:   make(map[crypto.Digest]*speculativeBlock),
		pending:       make(map[crypto.Digest]uint64),
		waitingParent: make(map[crypto.Digest][]loadedBlock),
		waiters:       make(map[crypto.Digest][]rootWaiter),
		dropped:       make(map[crypto.Digest]uint64),
	}

	go s.run()

	return &Client{ch}, nil
}

func (s *State) run() {
	for {
		select {
		case msg := <-s.rx:
			switch m := msg.(type) {
			case executeMessage:
				s.handleExecute(m.info)
			case commitMessage:
				s.commitQueue = append(s.commitQueue, m.digest)
				s.processCommits()
			case rootMessage:
				s.handleRoot(m)
			case closeMessage:
				if err := s.db.close(); err != nil {
					log.Printf("Failed to close the state database: %v\n", err)
				}
				close(m.done)
				return
			}
		case lb := <-s.loaded:
			s.handleLoaded(lb)
		}
	}
}

// committed returns the state at the end of digest if it has been committed.
func (s *State) committed(digest crypto.Digest) (*committedInfo, error) {
	if digest == s.tip.Digest {
		return &s.tip.committedInfo, nil
	}
	return s.db.getCommitted(digest)
}

func (s *State) handleExecute(info BlockInfo) {
	if _, ok := s.speculative[info.Digest]; ok {
		return
	}
	if _, ok := s.pending[info.Digest]; ok {
		return
	}
	if _, ok := s.dropped[info.Digest]; ok {
		return
	}

	// Anything at or below the committed tip is either committed already or
	// on a fork that can never be committed.
	if info.Round <= s.tip.Version {
		return
	}

	s.pending[info.Digest] = info.Round
	go s.loadPayload(info)
}

// loadPayload reads the block's batches from the store, waiting for any that
// are still being synchronized by the mempool.
func (s *State) loadPayload(info BlockInfo) {
	txs := make([][]byte, 0)

	for _, digest := range info.Payload {
		data, err := s.store.NotifyRead(digest[:])
		if err != nil {
			s.loaded <- loadedBlock{info: info, err: err}
			return
		}

		batch, err := mempool.DeserializeBatch(*data)
		if err != nil {
			s.loaded <- loadedBlock{info: info, err: fmt.Errorf("batch %v: %w", digest, err)}
			return
		}

		txs = append(txs, batch...)
	}

	s.loaded <- loadedBlock{info: info, txs: txs}
}

func (s *State) handleLoaded(lb loadedBlock) {
	if _, ok := s.pending[lb.info.Digest]; !ok {
		// pruned while loading
		return
	}

	if lb.err != nil {
		s.fail(lb.info.Digest, fmt.Errorf("failed to load payload of %v: %w", lb.info.Digest, lb.err))
		return
	}

	s.tryExecute(lb)
	s.processCommits()
}

// fail drops a block that could not be executed, along with any blocks
// waiting on it.
func (s *State) fail(digest crypto.Digest, err error) {
	log.Printf("State: %v\n", err)
	if round, ok := s.pending[digest]; ok {
		s.dropped[digest] = round
	}
	delete(s.pending, digest)
	s.replyWaiters(digest, RootResult{Err: err})

	children := s.waitingParent[digest]
	delete(s.waitingParent, digest)
	for _, child := range children {
		s.fail(child.info.Digest, fmt.Errorf("parent %v of %v failed", digest, child.info.Digest))
	}
}

// tryExecute executes the block if its parent's state is available, and then
// any blocks that were waiting on it.
func (s *State) tryExecute(lb loadedBlock) {
	queue := []loadedBlock{lb}

	for len(queue) > 0 {
		lb := queue[0]
		queue = queue[1:]
		info := lb.info

		var parentVersion jmt.Version
		if parent, ok := s.speculative[info.Parent]; ok {
			parentVersion = parent.version
		} else if info.Parent == s.tip.Digest {
			parentVersion = s.tip.Version
		} else {
			committed, err := s.db.getCommitted(info.Parent)
			if err != nil {
				s.fail(info.Digest, err)
				continue
			}
			if committed != nil {
				s.fail(info.Digest, fmt.Errorf("%v extends committed block %v, which is not the tip", info.Digest, info.Parent))
				continue
			}

			// Parents are always sent before their children, so a parent that
			// is not pending was dropped (pruned fork or failed execution)
			if _, ok := s.pending[info.Parent]; !ok {
				s.fail(info.Digest, fmt.Errorf("parent %v of %v is not available", info.Parent, info.Digest))
				continue
			}

			// parent has not been executed yet
			s.waitingParent[info.Parent] = append(s.waitingParent[info.Parent], lb)
			continue
		}

		block, err := s.execute(lb, parentVersion)
		if err != nil {
			s.fail(info.Digest, fmt.Errorf("failed to execute %v: %w", info.Digest, err))
			continue
		}

		delete(s.pending, info.Digest)
		s.speculative[info.Digest] = block
		log.Printf("State: executed %v (version %d, root %v, %d writes)\n",
			info.Digest, block.version, crypto.Digest(block.root), len(block.values))

		s.replyWaiters(info.Digest, RootResult{Root: crypto.Digest(block.root)})

		queue = append(queue, s.waitingParent[info.Digest]...)
		delete(s.waitingParent, info.Digest)
	}
}

func (s *State) execute(lb loadedBlock, parentVersion jmt.Version) (*speculativeBlock, error) {
	version := jmt.Version(lb.info.Round)
	if version <= parentVersion {
		return nil, fmt.Errorf("round %d does not follow parent version %d", version, parentVersion)
	}

	parentView := overlayReader{s, lb.info.Parent}

	writes, err := s.executor.Execute(lb.txs, stateReader{parentView, parentVersion})
	if err != nil {
		return nil, err
	}

	updates := make(map[jmt.Hash]jmt.Hash, len(writes))
	values := make(map[jmt.Hash][]byte, len(writes))
	for key, value := range writes {
		if value == nil {
			return nil, fmt.Errorf("executor deleted key %v, deletion is not supported", key)
		}
		updates[key] = jmt.NewHash(value)
		values[key] = append([]byte{}, value...)
	}

	writer := &captureWriter{make(map[jmt.NodeKey]jmt.Node)}
	tree := jmt.NewJellyfishMerkleTree(parentView, writer)

	root, err := tree.BatchUpdate(parentVersion, version, updates)
	if err != nil {
		return nil, err
	}

	return &speculativeBlock{
		info:    lb.info,
		version: version,
		root:    *root,
		nodes:   writer.nodes,
		values:  values,
	}, nil
}

func (s *State) replyWaiters(digest crypto.Digest, result RootResult) {
	for _, w := range s.waiters[digest] {
		w.reply <- result
	}
	delete(s.waiters, digest)
}

func (s *State) handleRoot(m rootMessage) {
	if block, ok := s.speculative[m.digest]; ok {
		m.reply <- RootResult{Root: crypto.Digest(block.root)}
		return
	}

	committed, err := s.committed(m.digest)
	if err != nil {
		m.reply <- RootResult{Err: err}
		return
	}
	if committed != nil {
		m.reply <- RootResult{Root: crypto.Digest(committed.Root)}
		return
	}

	if m.round <= s.tip.Version {
		m.reply <- RootResult{Err: fmt.Errorf("block %v is not on the committed chain", m.digest)}
		return
	}

	if _, ok := s.dropped[m.digest]; ok {
		m.reply <- RootResult{Err: fmt.Errorf("block %v could not be executed", m.digest)}
		return
	}

	s.waiters[m.digest] = append(s.waiters[m.digest], rootWaiter{m.round, m.reply})
}

// processCommits commits queued blocks, in order, as they become executed.
func (s *State) processCommits() {
	committedAny := false

	for len(s.commitQueue) > 0 {
		digest := s.commitQueue[0]

		committed, err := s.committed(digest)
		if err != nil {
			log.Printf("State: failed to check commit status of %v: %v\n", digest, err)
			return
		}
		if committed != nil {
			// already committed
			s.commitQueue = s.commitQueue[1:]
			continue
		}

		block, ok := s.speculative[digest]
		if !ok {
			// wait for it to be executed
			break
		}

		if block.info.Parent != s.tip.Digest {
			log.Printf("State: committed block %v does not extend the committed tip %v, ignoring\n", digest, s.tip.Digest)
			s.commitQueue = s.commitQueue[1:]
			continue
		}

		newTip := tip{
			Digest:        digest,
			committedInfo: committedInfo{Version: block.version, Root: block.root},
		}
		if err := s.db.commit(newTip, block.nodes, block.values); err != nil {
			log.Fatalf("State: failed to persist committed block %v: %v\n", digest, err)
		}

		s.tip = newTip
		delete(s.speculative, digest)
		s.commitQueue = s.commitQueue[1:]
		committedAny = true

		log.Printf("State: committed %v (version %d, root %v)\n", digest, newTip.Version, crypto.Digest(newTip.Root))
	}

	if committedAny {
		s.prune()
	}
}

// descendsFromTip reports whether a speculative block extends the committed tip.
func (s *State) descendsFromTip(block *speculativeBlock) bool {
	for parent := block.info.Parent; ; {
		if parent == s.tip.Digest {
			return true
		}

		p, ok := s.speculative[parent]
		if !ok {
			return false
		}
		parent = p.info.Parent
	}
}

// prune drops everything that can no longer extend the committed tip.
func (s *State) prune() {
	forkErr := func(digest crypto.Digest) error {
		return fmt.Errorf("block %v is not on the committed chain", digest)
	}

	// Decide first, delete after: descendsFromTip walks the speculative map
	forks := make([]*speculativeBlock, 0)
	for _, block := range s.speculative {
		if !s.descendsFromTip(block) {
			forks = append(forks, block)
		}
	}
	for _, block := range forks {
		digest := block.info.Digest
		delete(s.speculative, digest)
		s.dropped[digest] = block.info.Round
		s.replyWaiters(digest, RootResult{Err: forkErr(digest)})
	}
	if len(forks) != 0 {
		log.Printf("State: pruned %d speculative blocks on abandoned forks\n", len(forks))
	}

	// Pending blocks at or below the tip are on forks. fail also drops any
	// blocks waiting on them.
	for digest, round := range s.pending {
		if round <= s.tip.Version {
			s.fail(digest, forkErr(digest))
		}
	}

	for digest, round := range s.dropped {
		if round <= s.tip.Version {
			delete(s.dropped, digest)
		}
	}

	for parent, children := range s.waitingParent {
		kept := children[:0]
		for _, lb := range children {
			if _, ok := s.pending[lb.info.Digest]; ok {
				kept = append(kept, lb)
			}
		}
		if len(kept) == 0 {
			delete(s.waitingParent, parent)
		} else {
			s.waitingParent[parent] = kept
		}
	}

	for digest, waiters := range s.waiters {
		kept := waiters[:0]
		for _, w := range waiters {
			if w.round <= s.tip.Version {
				w.reply <- RootResult{Err: forkErr(digest)}
			} else {
				kept = append(kept, w)
			}
		}
		if len(kept) == 0 {
			delete(s.waiters, digest)
		} else {
			s.waiters[digest] = kept
		}
	}
}
