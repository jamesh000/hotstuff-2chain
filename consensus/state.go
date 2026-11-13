package consensus

import (
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/state"
)

// stateRootResult is the outcome of a state root query, delivered back to the
// core loop.
type stateRootResult struct {
	digest crypto.Digest
	root   crypto.Digest
	err    error
}

// StateDriver connects consensus to the state module.
type StateDriver struct {
	client   state.Client
	rxResult chan stateRootResult
}

func NewStateDriver(client state.Client) StateDriver {
	return StateDriver{
		client:   client,
		rxResult: make(chan stateRootResult, CHANNEL_CAPACITY),
	}
}

// Execute sends a block to the state module for speculative execution.
func (d StateDriver) Execute(b *Block) {
	d.client.Execute(state.BlockInfo{
		Digest:  b.Digest(),
		Parent:  b.Parent(),
		Round:   b.Round,
		Payload: b.Payload,
	})
}

// Commit notifies the state module that a block is committed.
func (d StateDriver) Commit(digest crypto.Digest) {
	d.client.Commit(digest)
}

// RequestRoot asks for the root hash at the end of the given block. The
// result is delivered on rxResult asynchronously so the core loop never
// blocks.
func (d StateDriver) RequestRoot(digest crypto.Digest, round Round) {
	go func() {
		res := <-d.client.Root(digest, round)
		d.rxResult <- stateRootResult{digest: digest, root: res.Root, err: res.Err}
	}()
}
