package consensus

import (
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Stake = uint32
type EpochNumber = uint64

type Parameters struct {
	TimeoutDelay   uint64
	SyncRetryDelay uint64
}

func (Parameters) Default() Parameters {
	return Parameters{
		TimeoutDelay:   5000,
		SyncRetryDelay: 10000,
	}
}

type Authority struct {
	Stake   Stake   `json:"stake"`
	Address peer.ID `json:"address"`
}

type AuthorityInfo struct {
	Author  crypto.PublicKey
	Stake   Stake
	Address peer.ID
}

type Committee struct {
	Authorities map[crypto.PublicKey]Authority `json:"authorities"`
	Epoch       EpochNumber                    `json:"epoch"`
}

func NewCommittee(info []AuthorityInfo, epoch EpochNumber) Committee {
	committee := Committee{
		Authorities: make(map[crypto.PublicKey]Authority),
		Epoch:       epoch,
	}

	for _, a := range info {
		committee.Authorities[a.Author] = Authority{
			stake:   a.Stake,
			address: a.Address,
		}
	}

	return committee
}

func (c Committee) Size() int {
	return len(c.Authorities)
}

func (c Committee) Stake(name crypto.PublicKey) Stake {
	if a, ok := c.Authorities[name]; ok {
		return a.stake
	}
	return 0
}

func (c Committee) QuorumThreshold() Stake {
	totalStake := Stake(0)
	for _, a := range c.Authorities {
		totalStake += a.stake
	}
	return totalStake
}

func (c Committee) Address(name crypto.PublicKey) interface{} {
	return c.Authorities[name].address
}

func (c Committee) BroadcastAddresses(myself crypto.PublicKey) []Authority {
	addresses := make([]Authority, 0, len(c.Authorities))

	for pk, a := range c.Authorities {
		if pk != myself {
			addresses = append(addresses, a)
		}
	}

	return addresses
}
