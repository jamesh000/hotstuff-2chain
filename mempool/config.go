package mempool

import "github.com/libp2p/go-libp2p/core/peer"

type EpochNumber = uint64
type Stake = uint32

type Parameters struct {
	Empty int `json:"empty"`
}

func DefaultParameters() Parameters {
	return Parameters{
		Empty: 0,
	}
}

type Authority struct {
	Stake               Stake
	TransactionsAddress peer.ID
	MempoolAddress      peer.ID
}

type Committee struct {
	Empty string `json:"empty"`
}
