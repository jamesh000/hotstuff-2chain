package crypto

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

const DIGEST_LEN = 32

type Digest [DIGEST_LEN]byte

func (d Digest) String() string {
	return base64.StdEncoding.EncodeToString(d[:])
}

func (d *Digest) FromBytes(data []byte) (*Digest, error) {
	if len(data) != DIGEST_LEN {
		return nil, fmt.Errorf("Digest is wrong length")
	}

	copy(d[:], data)

	return d, nil
}

func NewDigest(bytes []byte) Digest {
	return sha256.Sum256(bytes)
}
