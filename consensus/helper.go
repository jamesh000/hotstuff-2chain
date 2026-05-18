package consensus

import "github.com/jamesh000/hotstuff-2chain/crypto"

type HelperMessage struct {
	missing crypto.Digest
	origin  crypto.Digest
}
