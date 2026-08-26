package mempool

import (
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/jamesh000/hotstuff-2chain/store"
)

const channelCapacity = 1_000

const mempoolProtocol string = "mempool"

type Round = uint64

type ConsensusMessage interface {
	consensusMempoolMessageMember()
}

type consensusSynchronizeMessage struct {
	missing []crypto.Digest
	target  crypto.PublicKey
}

func (msg consensusSynchronizeMessage) consensusMempoolMessageMember() {}

type consensusCleanupMessage struct {
	round Round
}

func (msg consensusCleanupMessage) consensusMempoolMessageMember() {}

type Mempool struct {
	name        crypto.PublicKey
	committee   Committee
	parameters  Parameters
	store       store.Store
	txConsensus chan<- crypto.Digest
}

func SpawnMempool(
	name crypto.PublicKey,
	host *network.RoutedHost,
	committee Committee,
	parameters Parameters,
	store store.Store,
	rxConsensus <-chan ConsensusMessage,
	txConsensus chan<- crypto.Digest,
) {
}
