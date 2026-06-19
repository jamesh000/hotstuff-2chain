package consensus_test

import (
	"testing"

	"github.com/jamesh000/hotstuff-2chain/consensus"
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/testutil"
)

func TestAggregator(t *testing.T) {
	committeeFile, secretFiles, err := testutil.CreateConfig(t, 3, nil)
	if err != nil {
		t.Errorf("Couldn't create config: %v", err)
	}

	committee, secrets, err := testutil.LoadConfig(committeeFile, secretFiles)
	if err != nil {
		t.Errorf("Couldn't load config: %v", err)
	}

	aggregator := consensus.NewAggregator(committee.Consensus)

	var qc *consensus.QC
	for _, s := range secrets {
		sigService := crypto.NewSignatureService(s.Secret)

		block := consensus.GenesisBlock()
		vote := consensus.NewVote(block, s.Name, sigService)

		qc, err = aggregator.AddVote(vote)
		if err != nil {
			t.Errorf("Error adding votes: %v", err)
		}

		if qc != nil {
			break
		}
	}

	if err := qc.Verify(committee.Consensus); err != nil {
		t.Errorf("QC failed verification: %v", err)
	}
}
