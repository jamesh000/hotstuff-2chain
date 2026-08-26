package mempool

import (
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

type EpochNumber = uint64
type Stake = uint32

type Parameters struct {
	gcDepth        uint64 `json:"gcdepth"`
	syncRetryDelay uint64 `json:"syncretrydelay"`
	syncRetryNodes uint   `json:"syncretrynodes"`
	batchSize      uint   `json:"batchsize"`
	maxBatchDelay  uint   `json:"maxbatchdelay"`
}

func DefaultParameters() Parameters {
	return Parameters{
		gcDepth:        50,
		syncRetryDelay: 5_000,
		syncRetryNodes: 3,
		batchSize:      500_000,
		maxBatchDelay:  100,
	}
}

type Authority struct {
	Stake               Stake   `json:"stake"`
	TransactionsAddress peer.ID `json:"txaddress"`
	MempoolAddress      peer.ID `json:"mpaddress"`
}

type authorityInfo struct {
	Author              crypto.PublicKey
	Stake               Stake
	TransactionsAddress peer.ID
	MempoolAddress      peer.ID
}

type Committee struct {
	Authorities map[crypto.PublicKey]Authority `json:"authorities"`
	Epoch       EpochNumber                    `json:"epoch"`
}

func NewCommittee(info []authorityInfo, epoch EpochNumber) Committee {
	authorities := make(map[crypto.PublicKey]Authority)
	for _, authInfo := range info {
		authorities[authInfo.Author] = Authority{
			Stake:               authInfo.Stake,
			TransactionsAddress: authInfo.TransactionsAddress,
			MempoolAddress:      authInfo.MempoolAddress,
		}
	}

	return Committee{
		Authorities: authorities,
		Epoch:       epoch,
	}
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

func (c Committee) TransactionsAddress(name crypto.PublicKey) (peer.ID, bool) {
	authority, ok := c.Authorities[name]
	return authority.TransactionsAddress, ok
}

func (c Committee) MempoolAddress(name crypto.PublicKey) (peer.ID, bool) {
	authority, ok := c.Authorities[name]
	return authority.MempoolAddress, ok
}

// Specifically gets mempool addresses
func (c Committee) BroadcastAddresses(myself crypto.PublicKey) ([]crypto.PublicKey, []peer.ID) {
	names := make([]crypto.PublicKey, 0, len(c.Authorities))
	addresses := make([]peer.ID, 0, len(c.Authorities))

	for pk, a := range c.Authorities {
		if pk != myself {
			names = append(names, pk)
			addresses = append(addresses, a.MempoolAddress)
		}
	}

	return names, addresses
}
