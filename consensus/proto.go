package consensus

import (
	pb "github.com/jamesh000/hotstuff-2chain/consensuspb"
	"github.com/jamesh000/hotstuff-2chain/crypto"
)

func (b Block) toProto() *pb.Block {
	payloadData := make([][]byte, len(b.Payload))
	for i, digest := range b.Payload {
		payloadData[i] = digest[:]
	}

	protoBlock := &pb.Block{
		QuorumCert: b.Qc.toProto(),
		Author:     b.Author[:],
		Round:      b.Round,
		Payload:    payloadData,
		Signature:  b.Signature[:],
	}

	if b.Tc != nil {
		protoBlock.TimeoutCert = b.Tc.toProto()
	}

	return protoBlock
}

func blockFromProto(pblock *pb.Block) (*Block, error) {
	payloadDigests := make([]crypto.Digest, len(pblock.Payload))
	for i, digestBytes := range pblock.Payload {
		d, err := new(crypto.Digest).FromBytes(digestBytes)
		if err != nil {
			return nil, err
		}

		payloadDigests[i] = *d
	}

	qc, err := qcFromProto(pblock.QuorumCert)
	if err != nil {
		return nil, err
	}

	var tc *TC
	tc = nil
	if pblock.TimeoutCert != nil {
		tc, err = tcFromProto(pblock.TimeoutCert)
		if err != nil {
			return nil, err
		}
	}

	author, err := new(crypto.PublicKey).FromBytes(pblock.Author)
	if err != nil {
		return nil, err
	}

	signature, err := new(crypto.Signature).FromBytes(pblock.Signature)
	if err != nil {
		return nil, err
	}

	return &Block{
			Qc:        *qc,
			Tc:        tc,
			Author:    *author,
			Round:     pblock.Round,
			Payload:   payloadDigests,
			Signature: *signature,
		},
		nil
}

func (qc QC) toProto() *pb.QC {
	voterData := make([][]byte, len(qc.Voters))
	for i, voter := range qc.Voters {
		voterData[i] = voter[:]
	}

	return &pb.QC{
		Hash:      qc.Hash[:],
		Round:     qc.Round,
		Voters:    voterData,
		Signature: qc.Signature[:],
	}
}

func qcFromProto(pqc *pb.QC) (*QC, error) {
	voters := make([]crypto.PublicKey, len(pqc.Voters))
	for i, voter := range pqc.Voters {
		unmarshalledVoter, err := new(crypto.PublicKey).FromBytes(voter)
		if err != nil {
			return nil, err
		}

		voters[i] = *unmarshalledVoter
	}

	hashValue, err := new(crypto.Digest).FromBytes(pqc.Hash)
	if err != nil {
		return nil, err
	}

	sigValue, err := new(crypto.Signature).FromBytes(pqc.Signature)
	if err != nil {
		return nil, err
	}

	return &QC{
			Hash:      *hashValue,
			Round:     pqc.Round,
			Voters:    voters,
			Signature: *sigValue,
		},
		nil
}

func (tc TC) toProto() *pb.TC {
	atrs := make([]*pb.TC_AuthorityTimeoutRound, len(tc.Votes))
	for i, atr := range tc.Votes {
		atrs[i] = &pb.TC_AuthorityTimeoutRound{
			Name:  atr.name[:],
			Round: atr.round,
		}
	}

	return &pb.TC{
		Round:     tc.Round,
		Atrs:      atrs,
		Signature: tc.Signature[:],
	}
}

func tcFromProto(ptc *pb.TC) (*TC, error) {
	atrs := make([]authorityTimeoutRound, len(ptc.Atrs))
	for i, atr := range ptc.Atrs {
		atrName, err := new(crypto.PublicKey).FromBytes(atr.Name)
		if err != nil {
			return nil, err
		}

		unmarshalledAttr := authorityTimeoutRound{
			name:  *atrName,
			round: atr.Round,
		}

		atrs[i] = unmarshalledAttr
	}

	sigValue, err := new(crypto.Signature).FromBytes(ptc.Signature)
	if err != nil {
		return nil, err
	}

	return &TC{
			Round:     ptc.Round,
			Votes:     atrs,
			Signature: *sigValue,
		},
		nil
}

func (v vote) toProto() *pb.Vote {
	return &pb.Vote{
		Hash:      v.hash[:],
		Round:     v.round,
		Author:    v.author[:],
		Signature: v.signature[:],
	}
}

func voteFromProto(pvote *pb.Vote) (*vote, error) {
	hashValue, err := new(crypto.Digest).FromBytes(pvote.Hash)
	if err != nil {
		return nil, err
	}

	author, err := new(crypto.PublicKey).FromBytes(pvote.Author)
	if err != nil {
		return nil, err
	}

	signature, err := new(crypto.Signature).FromBytes(pvote.Signature)
	if err != nil {
		return nil, err
	}

	return &vote{
			hash:      *hashValue,
			round:     pvote.Round,
			author:    *author,
			signature: *signature,
		},
		nil
}

func (t timeout) toProto() *pb.Timeout {
	return &pb.Timeout{
		HighQC:    t.highQC.toProto(),
		Round:     t.round,
		Author:    t.author[:],
		Signature: t.signature[:],
	}
}

func timeoutFromProto(ptimeout *pb.Timeout) (*timeout, error) {
	highQC, err := qcFromProto(ptimeout.HighQC)
	if err != nil {
		return nil, err
	}

	author, err := new(crypto.PublicKey).FromBytes(ptimeout.Author)
	if err != nil {
		return nil, err
	}

	signature, err := new(crypto.Signature).FromBytes(ptimeout.Signature)
	if err != nil {
		return nil, err
	}

	return &timeout{
			highQC:    *highQC,
			round:     ptimeout.Round,
			author:    *author,
			signature: *signature,
		},
		nil
}
