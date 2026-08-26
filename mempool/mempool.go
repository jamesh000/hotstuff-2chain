package mempool

import (
	"context"
	"log"

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

type SynchronizeMessage struct {
	Missing []crypto.Digest
	Author  crypto.PublicKey
}

func (msg SynchronizeMessage) consensusMempoolMessageMember() {}

type CleanupMessage struct {
	Round Round
}

func (msg CleanupMessage) consensusMempoolMessageMember() {}

func SpawnMempool(
	name crypto.PublicKey,
	host *network.RoutedHost,
	committee Committee,
	parameters Parameters,
	store store.Store,
	fromConsensus <-chan ConsensusMessage,
	toConsensus chan<- crypto.Digest,
) {
	go func() {
		ps, err := network.NewPubsub(context.Background(), host, "mempool")
		if err != nil {
			panic(err)
		}

		for {
			msg, err := ps.Next(context.Background())
			if err != nil {
				panic(err)
			}

			log.Printf("Got message \"%v\" from client\n", string(msg))

			msgDigest := crypto.NewDigest(msg)

			store.Write(msgDigest[:], msgDigest[:])

			toConsensus <- msgDigest
		}
	}()

	go func() {
		for {
			<-fromConsensus
		}
	}()
}
