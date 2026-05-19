package consensus

import pb "github.com/jamesh000/hotstuff-2chain/consensuspb"

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

func (v vote) toProto() *pb.Vote {
	return &pb.Vote{
		Hash:      v.hash[:],
		Round:     v.round,
		Author:    v.author[:],
		Signature: v.signature[:],
	}
}

func (t timeout) toProto() *pb.Timeout {
	return &pb.Timeout{
		HighQC:    t.highQC.toProto(),
		Round:     t.round,
		Author:    t.author[:],
		Signature: t.signature[:],
	}
}
