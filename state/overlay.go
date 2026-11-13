package state

import (
	"fmt"
	"maps"

	jmt "github.com/jamesh000/SimpleJMT"
	"github.com/jamesh000/hotstuff-2chain/crypto"
)

// speculativeBlock is a block that has been executed but not committed. Its
// tree nodes and values are kept in memory, layered on top of its parent's.
//
// JMT nodes are keyed by (version, nibble path), so two branches writing the
// same version into a shared store would overwrite each other. Keeping every
// uncommitted block in its own overlay means forks never collide; only the
// committed chain is written to disk.
type speculativeBlock struct {
	info    BlockInfo
	version jmt.Version
	root    jmt.Hash
	nodes   map[jmt.NodeKey]jmt.Node
	values  map[jmt.Hash][]byte
}

// overlayReader reads the tree as seen from the end of block head: the
// overlays of head and its speculative ancestors, then the committed tree.
type overlayReader struct {
	s    *State
	head crypto.Digest
}

func (r overlayReader) GetNode(key jmt.NodeKey) (jmt.Node, error) {
	for b := r.s.speculative[r.head]; b != nil; b = r.s.speculative[b.info.Parent] {
		if node, ok := b.nodes[key]; ok {
			return node, nil
		}
	}

	return r.s.db.GetNode(key)
}

// value returns the value stored at exactly key as seen from head.
func (r overlayReader) value(key jmt.ValueKey) ([]byte, error) {
	for b := r.s.speculative[r.head]; b != nil; b = r.s.speculative[b.info.Parent] {
		if b.version == key.Version {
			if value, ok := b.values[key.KeyHash]; ok {
				return value, nil
			}
			return nil, fmt.Errorf("value for %v missing from version %d", key.KeyHash, key.Version)
		}
	}

	return r.s.db.getValue(key)
}

// captureWriter collects the nodes written by a JMT update into an overlay.
type captureWriter struct {
	nodes map[jmt.NodeKey]jmt.Node
}

func (w *captureWriter) WriteNodeBatch(batch map[jmt.NodeKey]jmt.Node) error {
	maps.Copy(w.nodes, batch)
	return nil
}

// stateReader implements execution.StateReader over the state at the end of a
// block.
type stateReader struct {
	overlay overlayReader
	version jmt.Version
}

func (r stateReader) Get(key jmt.Hash) ([]byte, error) {
	tree := jmt.NewJellyfishMerkleTree(r.overlay, nil)

	valueHash, valueIndex, err := tree.LookupWithIndex(r.version, key)
	if err != nil {
		return nil, err
	}
	if valueHash == nil {
		return nil, nil
	}

	value, err := r.overlay.value(*valueIndex)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, fmt.Errorf("value for %v at version %d is missing", key, valueIndex.Version)
	}
	if jmt.NewHash(value) != *valueHash {
		return nil, fmt.Errorf("value for %v at version %d does not match the tree", key, valueIndex.Version)
	}

	return value, nil
}
