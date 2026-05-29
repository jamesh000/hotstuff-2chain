package consensus

import (
	"bytes"
	"encoding/binary"
	"fmt"

	pb "github.com/jamesh000/hotstuff-2chain/consensuspb"
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"google.golang.org/protobuf/proto"
)

type Block struct {
	Qc        QC
	Tc        *TC
	Author    crypto.PublicKey
	Round     Round
	Payload   []crypto.Digest
	Signature crypto.Signature
}

func NewBlock(qc QC, tc *TC, author crypto.PublicKey, round Round, payload []crypto.Digest, sigservice crypto.SignatureService) Block {
	block := Block{
		Qc:      qc,
		Tc:      tc,
		Author:  author,
		Round:   round,
		Payload: payload,
	}

	sig := sigservice.RequestSignature(block.Digest())

	block.Signature = sig

	return block
}

func (Block) Genesis() Block {
	return Block{}
}

func (b Block) Parent() crypto.Digest {
	return b.Qc.Hash
}

func (b Block) Verify(committee Committee) error {
	if committee.Stake(b.Author) == 0 {
		return fmt.Errorf("Unknown authority %v", b.Author)
	}

	if !b.Signature.Verify(b.Digest(), b.Author) {
		return fmt.Errorf("%v is not correctly signed", b)
	}

	if !b.Qc.IsGenesisQC() {
		err := b.Qc.Verify(committee)
		if err != nil {
			return fmt.Errorf("%v is not valid: %w", b, err)
		}
	}

	if b.Tc != nil {
		err := b.Tc.Verify(committee)
		if err != nil {
			return fmt.Errorf("%v is not valid: %w", b, err)
		}
	}

	return nil
}

func (b Block) Digest() crypto.Digest {
	buf := bytes.Buffer{}

	buf.Write(b.Author[:])
	binary.Write(&buf, binary.BigEndian, b.Round)
	for _, p := range b.Payload {
		buf.Write(p[:])
	}

	return crypto.NewDigest(buf.Bytes())
}

func (b Block) Serialize() ([]byte, error) {
	pblock := b.toProto()

	data, err := proto.Marshal(pblock)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (b *Block) Deserialize(data []byte) (*Block, error) {
	pblock := &pb.Block{}
	err := proto.Unmarshal(data, pblock)
	if err != nil {
		return nil, err
	}

	b, err = blockFromProto(pblock)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (b Block) String() string {
	return fmt.Sprintf("Block<%v>(%v, %v, %v)", b.Digest(), b.Author, b.Round, b.Payload)
}

type vote struct {
	hash      crypto.Digest
	round     Round
	author    crypto.PublicKey
	signature crypto.Signature
}

func NewVote(block Block, author crypto.PublicKey, sigService crypto.SignatureService) vote {
	vote := vote{
		hash:   block.Digest(),
		round:  block.Round,
		author: author,
	}

	sig := sigService.RequestSignature(vote.Digest())
	vote.signature = sig

	return vote
}

func (v vote) verify(committee Committee) error {
	if committee.Stake(v.author) == 0 {
		return fmt.Errorf("Unknown authority %v", v.author)
	}

	if !v.signature.Verify(v.Digest(), v.author) {
		return fmt.Errorf("Vote %v is not correctly signed", v)
	}

	return nil
}

func (v vote) Digest() crypto.Digest {
	buf := bytes.Buffer{}

	buf.Write(v.hash[:])
	binary.Write(&buf, binary.BigEndian, v.round)

	return crypto.NewDigest(buf.Bytes())
}

func (v vote) String() string {
	return fmt.Sprintf("Vote(%v, %v, %v", v.author, v.round, v.hash)
}

type QC struct {
	Hash      crypto.Digest
	Round     Round
	Voters    []crypto.PublicKey
	Signature crypto.Signature
}

func GenesisQC() QC {
	return QC{
		Hash:      crypto.Digest{},
		Round:     0,
		Voters:    nil,
		Signature: crypto.Signature{},
	}
}

func (qc QC) IsGenesisQC() bool {
	return qc.Hash == crypto.Digest{} && qc.Round == 0 && qc.Voters == nil && qc.Signature == crypto.Signature{}
}

func (qc QC) Timeout() bool {
	return qc.Hash == crypto.Digest{} && qc.Round != 0
}

func (qc QC) Verify(committee Committee) error {
	weight := Stake(0)
	used := make(map[crypto.PublicKey]struct{})
	for _, name := range qc.Voters {
		if _, ok := used[name]; ok {
			return fmt.Errorf("Authority %v appears multiple times", name)
		}

		votingRights := committee.Stake(name)
		if votingRights == 0 {
			return fmt.Errorf("Unknown authority %v", name)
		}

		used[name] = struct{}{}
		weight += votingRights
	}

	if weight < committee.QuorumThreshold() {
		return fmt.Errorf("%v does not have a quorum", qc)
	}

	if !qc.Signature.FastAggregateVerify(qc.Hash, qc.Voters) {
		return fmt.Errorf("%v failed signature verification", qc)
	}

	return nil
}

func (qc QC) Digest() crypto.Digest {
	buf := bytes.Buffer{}

	buf.Write(qc.Hash[:])
	binary.Write(&buf, binary.BigEndian, qc.Round)

	return crypto.NewDigest(buf.Bytes())
}

func (qc QC) String() string {
	return fmt.Sprintf("QC(%v, %v)", qc.Hash, qc.Round)
}

type timeout struct {
	highQC    QC
	round     Round
	author    crypto.PublicKey
	signature crypto.Signature
}

func NewTimeout(highQC QC, round Round, author crypto.PublicKey, sigService crypto.SignatureService) timeout {
	timeout := timeout{
		highQC: highQC,
		round:  round,
		author: author,
	}

	sig := sigService.RequestSignature(timeout.Digest())
	timeout.signature = sig

	return timeout
}

func (t timeout) Verify(committee Committee) error {
	if committee.Stake(t.author) == 0 {
		return fmt.Errorf("Unknown authority %v", t.author)
	}

	if !t.signature.Verify(t.Digest(), t.author) {
		return fmt.Errorf("Timeout %v is not correctly signed", t)
	}

	if !t.highQC.IsGenesisQC() {
		err := t.highQC.Verify(committee)
		if err != nil {
			return fmt.Errorf("Timeout %v not valid: %w", t, err)
		}
	}

	return nil
}

func (t timeout) Digest() crypto.Digest {
	buf := bytes.Buffer{}

	binary.Write(&buf, binary.BigEndian, t.round)
	binary.Write(&buf, binary.BigEndian, t.highQC.Round)

	return crypto.NewDigest(buf.Bytes())
}

func (t timeout) String() string {
	return fmt.Sprintf("TC(%v, %v, %v)", t.author, t.round, t.highQC.Round)
}

type authorityTimeoutRound struct {
	name  crypto.PublicKey
	round Round
}

type TC struct {
	Round     Round
	Votes     []authorityTimeoutRound
	Signature crypto.Signature
}

func (tc TC) Verify(committee Committee) error {
	weight := Stake(0)
	used := make(map[crypto.PublicKey]struct{})
	for _, toVote := range tc.Votes {
		if _, ok := used[toVote.name]; ok {
			return fmt.Errorf("Authority %v appears multiple times", toVote.name)
		}

		votingRights := committee.Stake(toVote.name)
		if votingRights == 0 {
			return fmt.Errorf("Unknown authority %v", toVote.name)
		}

		used[toVote.name] = struct{}{}
		weight += votingRights
	}

	if weight < committee.QuorumThreshold() {
		return fmt.Errorf("%v does not have a quorum", tc)
	}

	authorities := make([]crypto.PublicKey, 0, len(tc.Votes))
	digests := make([]crypto.Digest, 0, len(tc.Votes))
	for _, vote := range tc.Votes {
		buf := bytes.Buffer{}
		binary.Write(&buf, binary.BigEndian, tc.Round)
		binary.Write(&buf, binary.BigEndian, vote.round)

		digests = append(digests, crypto.NewDigest(buf.Bytes()))
		authorities = append(authorities, vote.name)
	}

	if !tc.Signature.AggregateVerify(digests, authorities) {
		return fmt.Errorf("%v failed signature verification", tc)
	}

	return nil
}

func (tc TC) HighQCRounds() []Round {
	highQCRounds := make([]Round, 0, len(tc.Votes))
	for _, vote := range tc.Votes {
		highQCRounds = append(highQCRounds, vote.round)
	}

	return highQCRounds
}

func (tc TC) String() string {
	return fmt.Sprintf("TC(%v, %v)", tc.Round, tc.HighQCRounds())
}
