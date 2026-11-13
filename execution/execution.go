// Package execution turns the transactions of a block into a write set that
// the state module applies to the Jellyfish Merkle Tree.
package execution

import (
	jmt "github.com/jamesh000/SimpleJMT"
)

// Key is the hashed state key a value is stored under.
type Key = jmt.Hash

// WriteSet maps state keys to the raw value they hold after executing a block.
// Values must be non-nil: the JMT does not support deletion.
type WriteSet map[Key][]byte

// StateReader gives read-only access to the state a block is executed on top
// of (the state at the end of its parent block).
type StateReader interface {
	// Get returns the raw value stored under key, or nil if there is none.
	Get(key Key) ([]byte, error)
}

// Executor executes the transactions of a block, in order, against the parent
// state and returns the resulting writes. Execution must be deterministic. An
// error means the block could not be executed (e.g. a storage failure);
// invalid transactions should be skipped instead.
type Executor interface {
	Execute(txs [][]byte, state StateReader) (WriteSet, error)
}
