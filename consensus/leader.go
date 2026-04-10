package consensus

import (
	"bytes"
	"slices"

	"github.com/jamesh000/hotstuff-2chain/crypto"
)

type leaderElector interface {
	getLeader(Round) crypto.PublicKey
}

type rrLeaderElector struct {
	committee Committee
}

func (rrLeaderElector) New(committee Committee) rrLeaderElector {
	return rrLeaderElector{
		committee: committee,
	}
}

func (rle rrLeaderElector) getLeader(round Round) crypto.PublicKey {
	keys := make([]crypto.PublicKey, 0, rle.committee.Size())
	for k := range rle.committee.authorities {
		keys = append(keys, k)
	}

	slices.SortStableFunc(keys, func(a, b crypto.PublicKey) int {
		return bytes.Compare(a[:], b[:])
	})

	return keys[int(round)%rle.committee.Size()]
}
