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

func DefaultParameters() Parameters {
	return Parameters{
		TimeoutDelay:   100000,
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
	Epoch       EpochNumber                    `json:"epoch"`b
}

func NewCommittee(info []AuthorityInfo, epoch EpochNumber) Committee {
	committee := Committee{
		Authorities: make(map[crypto.PublicKey]Authority),
		Epoch:       epoch,
	}

	for _, a := range info {
		committee.Authorities[a.Author] = Authority{
			Stake:   a.Stake,
			Address: a.Address,
		}
	}

	return committee
}

func (c Committee) Size() int {
	return len(c.Authorities)
}

func (c Committee) Stake(name crypto.PublicKey) Stake {
	if a, ok := c.Authorities[name]; ok {
		return a.Stake
	}
	return 0
}

func (c Committee) QuorumThreshold() Stake {
	totalStake := Stake(0)
	for _, a := range c.Authorities {
		totalStake += a.Stake
	}
	return 2*totalStake/3 + 1
}

func (c Committee) Address(name crypto.PublicKey) (peer.ID, bool) {
	authority, ok := c.Authorities[name]
	return authority.Address, ok
}

func (c Committee) BroadcastAddresses(myself crypto.PublicKey) ([]crypto.PublicKey, []peer.ID) {
	names := make([]crypto.PublicKey, 0, len(c.Authorities))
	addresses := make([]peer.ID, 0, len(c.Authorities))

	for pk, a := range c.Authorities {
		if pk != myself {
			names = append(names, pk)
			addresses = append(addresses, a.Address)
		}
	}

	return names, addresses
}
