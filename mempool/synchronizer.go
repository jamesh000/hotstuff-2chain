package mempool

import (
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/store"
)

const timerResolution uint64 = 1_000

type synchronizer struct {
	name           crypto.PublicKey
	committee      Committee
	store          store.Store
	gcDepth        Round
	syncRetryDelay uint64
	syncRetryNodes uint
	rxMessage      <-chan ConsensusMempoolMessage
}
