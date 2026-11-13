package state

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	jmt "github.com/jamesh000/SimpleJMT"
	"github.com/jamesh000/hotstuff-2chain/crypto"
)

// Key prefixes of the persistent state database.
const (
	prefixNode  byte = 0x00 // jmt.NodeKey -> serialized node
	prefixValue byte = 0x01 // jmt.ValueKey -> raw value
	prefixBlock byte = 0x02 // block digest -> committedInfo
	prefixMeta  byte = 0x03 // committed tip
)

const committedInfoLen = 8 + jmt.HashLen

// committedInfo is the state at the end of a committed block.
type committedInfo struct {
	Version jmt.Version
	Root    jmt.Hash
}

func (ci committedInfo) encode() []byte {
	data := make([]byte, committedInfoLen)
	binary.BigEndian.PutUint64(data[:8], ci.Version)
	copy(data[8:], ci.Root[:])
	return data
}

func decodeCommittedInfo(data []byte) (committedInfo, error) {
	if len(data) != committedInfoLen {
		return committedInfo{}, fmt.Errorf("committed info has length %d, want %d", len(data), committedInfoLen)
	}

	var ci committedInfo
	ci.Version = binary.BigEndian.Uint64(data[:8])
	copy(ci.Root[:], data[8:])
	return ci, nil
}

// tip is the last committed block and the state at its end.
type tip struct {
	Digest crypto.Digest
	committedInfo
}

func genesisTip() tip {
	return tip{
		Digest:        crypto.Digest{},
		committedInfo: committedInfo{Version: 0, Root: jmt.EmptyRootHash},
	}
}

// db is the persistent, committed part of the state. It implements
// jmt.TreeReader for the committed tree.
type db struct {
	pdb *pebble.DB
}

var _ jmt.TreeReader = (*db)(nil)

func openDB(path string) (*db, error) {
	pdb, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	return &db{pdb}, nil
}

func (d *db) close() error {
	return d.pdb.Close()
}

func prefixed(prefix byte, key []byte) []byte {
	result := make([]byte, 1+len(key))
	result[0] = prefix
	copy(result[1:], key)
	return result
}

func nodeKey(key jmt.NodeKey) ([]byte, error) {
	encoded, err := key.Encode()
	if err != nil {
		return nil, err
	}
	return prefixed(prefixNode, encoded), nil
}

// get returns a copy of the value under key, or nil if it does not exist.
func (d *db) get(key []byte) ([]byte, error) {
	data, closer, err := d.pdb.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	return append([]byte{}, data...), nil
}

func (d *db) GetNode(key jmt.NodeKey) (jmt.Node, error) {
	encoded, err := nodeKey(key)
	if err != nil {
		return nil, err
	}

	data, err := d.get(encoded)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, fmt.Errorf("%w: version %d path %s", jmt.ErrNodeNotFound, key.Version, key.NibblePath.String())
	}

	return jmt.DeserializeNode(data)
}

// getValue returns the value stored at exactly key, or nil if there is none.
func (d *db) getValue(key jmt.ValueKey) ([]byte, error) {
	return d.get(prefixed(prefixValue, key.Encode()))
}

// getCommitted returns the state at the end of a committed block, or nil if
// the block has not been committed.
func (d *db) getCommitted(digest crypto.Digest) (*committedInfo, error) {
	if digest == (crypto.Digest{}) {
		genesis := genesisTip().committedInfo
		return &genesis, nil
	}

	data, err := d.get(prefixed(prefixBlock, digest[:]))
	if err != nil || data == nil {
		return nil, err
	}

	ci, err := decodeCommittedInfo(data)
	if err != nil {
		return nil, err
	}
	return &ci, nil
}

// loadTip returns the last committed block, or the genesis tip if nothing has
// been committed yet.
func (d *db) loadTip() (tip, error) {
	data, err := d.get([]byte{prefixMeta})
	if err != nil {
		return tip{}, err
	}
	if data == nil {
		return genesisTip(), nil
	}

	if len(data) != crypto.DIGEST_LEN+committedInfoLen {
		return tip{}, fmt.Errorf("state metadata has length %d", len(data))
	}

	var t tip
	copy(t.Digest[:], data[:crypto.DIGEST_LEN])
	t.committedInfo, err = decodeCommittedInfo(data[crypto.DIGEST_LEN:])
	if err != nil {
		return tip{}, err
	}
	return t, nil
}

// commit atomically writes the nodes and values of a block and makes it the
// new committed tip.
func (d *db) commit(newTip tip, nodes map[jmt.NodeKey]jmt.Node, values map[jmt.Hash][]byte) error {
	batch := d.pdb.NewBatch()
	defer batch.Close()

	for key, node := range nodes {
		encodedKey, err := nodeKey(key)
		if err != nil {
			return err
		}

		data, err := node.SerializeNode()
		if err != nil {
			return err
		}

		if err := batch.Set(encodedKey, data, nil); err != nil {
			return err
		}
	}

	for keyHash, value := range values {
		valueKey := jmt.ValueKey{KeyHash: keyHash, Version: newTip.Version}
		if err := batch.Set(prefixed(prefixValue, valueKey.Encode()), value, nil); err != nil {
			return err
		}
	}

	info := newTip.committedInfo.encode()
	if err := batch.Set(prefixed(prefixBlock, newTip.Digest[:]), info, nil); err != nil {
		return err
	}

	meta := append(append([]byte{}, newTip.Digest[:]...), info...)
	if err := batch.Set([]byte{prefixMeta}, meta, nil); err != nil {
		return err
	}

	return d.pdb.Apply(batch, pebble.Sync)
}
