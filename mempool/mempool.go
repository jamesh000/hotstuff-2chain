package mempool

import "github.com/jamesh000/hotstuff-2chain/crypto"

type Round = uint64

type ConsensusMessage interface {
	consensusMempoolMessageMember()
}

type SynchronizeMessage struct {
	Missing []crypto.Digest
	Author  crypto.PublicKey
}

func (msg SynchronizeMessage) consensusMempoolMessageMember() {}

type CleanupMessage struct {
	Round Round
}

func (msg CleanupMessage) consensusMempoolMessageMember() {}
