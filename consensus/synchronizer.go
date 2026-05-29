package consensus

import (
	"log"
	"time"

	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/jamesh000/hotstuff-2chain/store"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const TIMER_ACCURACY uint64 = 5000

type Synchronizer struct {
	store        store.Store
	innerChannel chan<- Block
}

func NewSynchronizer(
	name crypto.PublicKey,
	committee Committee,
	store store.Store,
	txLoopback chan<- Block,
	syncRetryDelay uint64,
	host network.RoutedHost,
	proto protocol.ID,
) Synchronizer {
	network := network.NewSimpleSender(host, proto)
	innerChannel := make(chan Block, CHANNEL_CAPACITY)

	go func() {
		waiting := make(chan Block)
		pending := make(map[crypto.Digest]struct{})
		requests := make(map[crypto.Digest]int64)

		timer := time.NewTimer(time.Duration(TIMER_ACCURACY) * time.Millisecond)

		for {
			select {
			case block := <-innerChannel:
				blockDigest := block.Digest()
				if _, ok := pending[blockDigest]; !ok {
					continue
				}

				pending[blockDigest] = struct{}{}

				parent := block.Parent()
				author := block.Author
				go blockWaiter(store, parent, block, waiting)

				if _, ok := requests[parent]; !ok {
					log.Printf("Requesting sync for block %v\n", parent)
					now := time.Now().UnixMilli()
					requests[parent] = now

					address, ok := committee.Address(author)
					if !ok {
						log.Fatal("Author of valid block is not in the committee")
					}

					message, err := (syncRequestMessage{parent, name}).SerializeConsensusMessage()
					if err != nil {
						log.Fatal("Failed to serialize sync request")
					}
					network.Send(address, message)
				}
			case block := <-waiting:
				delete(pending, block.Digest())
				delete(requests, block.Parent())
				txLoopback <- block
			case <-timer.C:
				for digest, timestamp := range requests {
					now := time.Now().UnixMilli()
					if timestamp+int64(syncRetryDelay) < now {
						log.Printf("Requesting sync for block %v (retry)\n", digest)

						_, addresses := committee.BroadcastAddresses(name)
						message, err := (syncRequestMessage{digest, name}).SerializeConsensusMessage()
						if err != nil {
							log.Fatal("Failed to serialize sync request")
						}
						network.Broadcast(addresses, message)
					}
				}
				timer.Reset(time.Duration(TIMER_ACCURACY) * time.Millisecond)
			}
		}
	}()

	return Synchronizer{
		store:        store,
		innerChannel: innerChannel,
	}
}

func blockWaiter(store store.Store, waitOn crypto.Digest, deliver Block, deliverTo chan<- Block) {
	_, err := store.NotifyRead(waitOn[:])
	if err != nil {
		return
	}
	deliverTo <- deliver
}

func (s Synchronizer) getParentBlock(block *Block) (*Block, error) {
	if block.Qc.IsGenesisQC() {
		genesis := GenesisBlock()
		return &genesis, nil
	}

	parent := block.Parent()
	bytes, err := s.store.Read(parent[:])
	if err != nil {
		if err == store.ErrNotFound {
			s.innerChannel <- *block
		}

		return nil, err
	}

	readBlock, err := new(Block).Deserialize(*bytes)
	if err != nil {
		return nil, err
	}

	return readBlock, nil
}

func (s Synchronizer) getAncestors(block *Block) (*Block, *Block, error) {
	b1, err := s.getParentBlock(block)
	if err != nil {
		return nil, nil, err
	}

	b0, err := s.getParentBlock(b1)
	if err != nil {
		log.Fatal("We should have all ancestors of delivered blocks")
	}

	return b0, b1, nil
}
