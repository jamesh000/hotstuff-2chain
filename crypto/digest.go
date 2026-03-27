package crypto

import (
	"crypto/sha256"
	"encoding/base64"
)

type Digest [32]byte

func (d Digest) String() string {
	return base64.StdEncoding.EncodeToString(d[:])
}

func NewDigest(bytes []byte) Digest {
	return sha256.Sum256(bytes)
}
