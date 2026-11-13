package mempool

import (
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/store"
)

type sealedBatch = []byte

type Processor struct{}

func spawnProcessor(store store.Store, rxBatch <-chan sealedBatch, txDigest chan<- crypto.Digest) {
	go func() {
		for batch := range rxBatch {
			digest := crypto.NewDigest(batch)

			store.Write(digest[:], batch)

			txDigest <- digest
		}
	}()
}
