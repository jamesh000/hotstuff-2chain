package consensus

import (
	"path/filepath"
	"testing"

	"github.com/jamesh000/hotstuff-2chain/execution"
	"github.com/jamesh000/hotstuff-2chain/state"
	"github.com/jamesh000/hotstuff-2chain/store"
)

// buildChain stores a chain of blocks with the given rounds (each block's QC
// certifies the previous block) and returns them in order.
func buildChain(t *testing.T, s *store.Store, rounds []Round) []Block {
	t.Helper()

	blocks := make([]Block, 0, len(rounds))
	qc := GenesisQC()
	for _, r := range rounds {
		b := Block{Qc: qc, Round: r}
		data, err := b.Serialize()
		if err != nil {
			t.Fatal(err)
		}
		d := b.Digest()
		s.Write(d[:], data)
		blocks = append(blocks, b)
		qc = QC{Hash: d, Round: r}
	}

	return blocks
}

func commitOrder(t *testing.T, lastCommitted Round, chain []Block, target Block) []Round {
	t.Helper()

	stateClient, err := state.Spawn(filepath.Join(t.TempDir(), "state"), storeForTest, execution.TestExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	defer stateClient.Close()

	commitCh := make(chan Block, 100)
	c := &Core{
		synchronizer:       Synchronizer{store: storeForTest, innerChannel: make(chan Block, 10)},
		txCommit:           commitCh,
		stateDriver:        NewStateDriver(*stateClient),
		lastCommittedRound: lastCommitted,
	}

	if err := c.commit(target); err != nil {
		t.Fatal(err)
	}
	close(commitCh)

	rounds := make([]Round, 0)
	for b := range commitCh {
		rounds = append(rounds, b.Round)
	}
	return rounds
}

var storeForTest store.Store

func TestCommitOrder(t *testing.T) {
	s, err := store.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	storeForTest = *s

	cases := []struct {
		name          string
		rounds        []Round
		lastCommitted Round
		want          []Round
	}{
		{"single", []Round{1, 2, 3}, 2, []Round{3}},
		{"one ancestor", []Round{1, 2, 3}, 1, []Round{2, 3}},
		{"several ancestors", []Round{1, 2, 3, 4}, 0, []Round{1, 2, 3, 4}},
		{"round gap", []Round{1, 3, 5}, 1, []Round{3, 5}},
		{"round gaps from genesis", []Round{1, 3, 5, 6}, 0, []Round{1, 3, 5, 6}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chain := buildChain(t, s, tc.rounds)
			got := commitOrder(t, tc.lastCommitted, chain, chain[len(chain)-1])
			if len(got) != len(tc.want) {
				t.Fatalf("committed rounds %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("committed rounds %v, want %v", got, tc.want)
				}
			}
		})
	}
}
