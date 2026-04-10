package consensus

import (
	"github.com/jamesh000/hotstuff-2chain/crypto"
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

type authority struct {
	stake   Stake
	address interface{} // to be fixed
}

type AuthorityInfo struct {
	Author  crypto.PublicKey
	Stake   Stake
	Address interface{}
}

type Committee struct {
	authorities map[crypto.PublicKey]authority
	Epoch       EpochNumber
}

func (Committee) New(info []AuthorityInfo, epoch EpochNumber) Committee {
	committee := Committee{
		authorities: make(map[crypto.PublicKey]authority),
		Epoch:       epoch,
	}

	for _, a := range info {
		committee.authorities[a.Author] = authority{
			stake:   a.Stake,
			address: a.Address,
		}
	}

	return committee
}

func (c Committee) Size() int {
	return len(c.authorities)
}

func (c Committee) Stake(name crypto.PublicKey) Stake {
	if a, ok := c.authorities[name]; ok {
		return a.stake
	}
	return 0
}

func (c Committee) QuorumThreshold() Stake {
	totalStake := Stake(0)
	for _, a := range c.authorities {
		totalStake += a.stake
	}
	return totalStake
}

func (c Committee) Address(name crypto.PublicKey) interface{} {
	return c.authorities[name].address
}

// func (c Committee) BroadcastAddresses(myself crypto.PublicKey)
