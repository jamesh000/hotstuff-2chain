package crypto

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log"

	blst "github.com/supranational/blst/bindings/go"
)

const dst string = "BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_POP_"

type blstPublicKey = blst.P1Affine
type blstAggregatePublicKey = blst.P1Aggregate

type blstSignature = blst.P2Affine
type blstAggregateSignature = blst.P2Aggregate

type blstSecretKey = blst.SecretKey

const PUBLICKEY_LEN = 48

type PublicKey [PUBLICKEY_LEN]byte

func (pk PublicKey) encodeBase64() string {
	return base64.StdEncoding.Strict().EncodeToString(pk[:])
}

func (pk *PublicKey) decodeBase64(s string) error {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return err
	}

	if len(b) != 48 {
		return fmt.Errorf("Public keys must be 48 bytes long")
	}

	copy(pk[:], b)

	return nil
}

func (pk PublicKey) String() string {
	return pk.encodeBase64()
}

func (pk *PublicKey) FromBytes(data []byte) (*PublicKey, error) {
	if len(data) != PUBLICKEY_LEN {
		return nil, fmt.Errorf("Signature is wrong length")
	}

	copy(pk[:], data)

	return pk, nil
}

const SECRETKEY_LEN = 32

type SecretKey [SECRETKEY_LEN]byte

func (sk SecretKey) encodeBase64() string {
	return base64.StdEncoding.Strict().EncodeToString(sk[:])
}

func (sk *SecretKey) decodeBase64(s string) error {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return err
	}

	if len(b) != 32 {
		return fmt.Errorf("Public keys must be 48 bytes long")
	}

	copy(sk[:], b)

	return nil
}

func (sk *SecretKey) FromBytes(data []byte) (*SecretKey, error) {
	if len(data) != SECRETKEY_LEN {
		return nil, fmt.Errorf("Signature is wrong length")
	}

	copy(sk[:], data)

	return sk, nil
}

func GenerateKeypair() (SecretKey, PublicKey) {
	var ikm [32]byte
	_, _ = rand.Read(ikm[:])
	bsk := blst.KeyGen(ikm[:])
	bpk := new(blstPublicKey).From(bsk)

	var sk SecretKey
	var pk PublicKey

	copy(sk[:], bsk.Serialize())
	copy(pk[:], bpk.Compress())

	return sk, pk
}

const SIGNATURE_LEN = 96

type Signature [SIGNATURE_LEN]byte

func (s *Signature) FromBytes(data []byte) (*Signature, error) {
	if len(data) != SIGNATURE_LEN {
		return nil, fmt.Errorf("Signature is wrong length")
	}

	copy(s[:], data)

	return s, nil
}

func Sign(d Digest, sk SecretKey) Signature {
	bsk := new(blstSecretKey).Deserialize(sk[:])
	bsig := new(blstSignature).Sign(bsk, d[:], []byte(dst))

	var sig Signature
	copy(sig[:], bsig.Compress())

	return sig
}

func (sig Signature) Verify(d Digest, pk PublicKey) bool {
	//bsig := new(blstSignature).Uncompress(sig[:])
	//bpk := new(blstPublicKey).Uncompress(pk[:])

	//return bsig.Verify(true, bpk, true, d[:], []byte(dst))

	return new(blstSignature).VerifyCompressed(sig[:], true, pk[:], true, d[:], []byte(dst))
}

type AggregateSignature struct {
	bas blstAggregateSignature
}

func (s *AggregateSignature) Aggregate(sigs []Signature) {
	bsigs := make([]*blstSignature, 0, len(sigs))

	for _, sig := range sigs {
		bsigs = append(bsigs, new(blstSignature).Uncompress(sig[:]))
	}

	s.bas.Aggregate(bsigs, true)
}

func (s *AggregateSignature) Add(sig Signature) {
	s.bas.Add(new(blstSignature).Uncompress(sig[:]), true)
}

func (s AggregateSignature) ToSignature() Signature {
	return Signature(s.bas.ToAffine().Compress())
}

func (s Signature) AggregateVerify(digests []Digest, pks []PublicKey) bool {
	bpks := make([]*blstPublicKey, 0, len(pks))

	for _, pk := range pks {
		bpks = append(bpks, new(blstPublicKey).Uncompress(pk[:]))
	}

	msgs := make([]blst.Message, 0, len(digests))

	for _, d := range digests {
		msgs = append(msgs, d[:])
	}

	return new(blstSignature).Uncompress(s[:]).AggregateVerify(true, bpks, true, msgs, []byte(dst))
}

func (s Signature) FastAggregateVerify(d Digest, pks []PublicKey) bool {
	bpks := make([]*blstPublicKey, 0, len(pks))

	for _, pk := range pks {
		bpks = append(bpks, new(blstPublicKey).Uncompress(pk[:]))
	}

	log.Printf("Verifying the signature on the message: %v\n", d)

	return new(blstSignature).Uncompress(s[:]).FastAggregateVerify(true, bpks, d[:], []byte(dst))
}
