package consensus

import (
	"fmt"

	"github.com/jamesh000/hotstuff-2chain/crypto"
)

const quorumSizeGuess uint64 = 16

type aggregator struct {
	committee          Committee
	voteAggregators    map[Round]map[crypto.Digest]*qcMaker
	timeoutAggregators map[Round]*tcMaker
}

func NewAggregator(committee Committee) aggregator {
	return aggregator{
		committee:          committee,
		voteAggregators:    make(map[Round]map[crypto.Digest]*qcMaker),
		timeoutAggregators: make(map[Round]*tcMaker),
	}
}

func (a aggregator) addVote(v vote) (*QC, error) {
	roundMap, ok := a.voteAggregators[v.round]
	if !ok {
		roundMap = make(map[crypto.Digest]*qcMaker)
		a.voteAggregators[v.round] = roundMap
	}

	vdigest := v.Digest()
	qcMaker, ok := roundMap[vdigest]
	if !ok {
		qcMaker = NewQcMaker()
		roundMap[vdigest] = qcMaker
	}

	return qcMaker.append(v, a.committee)
}

func (a aggregator) addTimeout(t timeout) (*TC, error) {
	tcMaker, ok := a.timeoutAggregators[t.round]
	if !ok {
		tcMaker = NewTcMaker()
		a.timeoutAggregators[t.round] = tcMaker
	}

	return tcMaker.append(t, a.committee)
}

func (a aggregator) cleanup(round Round) {
	for r := range a.voteAggregators {
		if r < round {
			delete(a.voteAggregators, r)
		}
	}

	for r := range a.timeoutAggregators {
		if r < round {
			delete(a.timeoutAggregators, r)
		}
	}
}

type qcMaker struct {
	weight       Stake
	voters       []crypto.PublicKey
	used         map[crypto.PublicKey]struct{}
	aggregateSig crypto.AggregateSignature
}

func NewQcMaker() *qcMaker {
	return &qcMaker{
		weight: 0,
		voters: make([]crypto.PublicKey, 0, quorumSizeGuess),
		used:   make(map[crypto.PublicKey]struct{}),
	}
}

func (maker *qcMaker) append(v vote, committee Committee) (*QC, error) {
	if _, ok := maker.used[v.author]; ok {
		return nil, fmt.Errorf("Authority %v appears multiple times", v.author)
	}
	maker.used[v.author] = struct{}{}

	maker.voters = append(maker.voters, v.author)
	maker.weight += committee.Stake(v.author)

	maker.aggregateSig.Add(v.signature)

	if maker.weight >= committee.QuorumThreshold() {
		maker.weight = 0
		return &QC{
				Hash:      v.hash,
				Round:     v.round,
				Voters:    maker.voters,
				Signature: maker.aggregateSig.ToSignature(),
			},
			nil
	}
	return nil, nil
}

type tcMaker struct {
	weight       Stake
	votes        []authorityTimeoutRound
	used         map[crypto.PublicKey]struct{}
	aggregateSig crypto.AggregateSignature
}

func NewTcMaker() *tcMaker {
	return &tcMaker{
		weight: 0,
		votes:  make([]authorityTimeoutRound, 0, quorumSizeGuess),
		used:   make(map[crypto.PublicKey]struct{}),
	}
}

func (maker *tcMaker) append(t timeout, committee Committee) (*TC, error) {
	if _, ok := maker.used[t.author]; ok {
		return nil, fmt.Errorf("Authority %v appears multiple times", t.author)
	}
	maker.used[t.author] = struct{}{}

	maker.votes = append(maker.votes, authorityTimeoutRound{t.author, t.highQC.Round})
	maker.weight += committee.Stake(t.author)

	maker.aggregateSig.Add(t.signature)

	if maker.weight >= committee.QuorumThreshold() {
		maker.weight = 0
		return &TC{
				Round:     t.round,
				Votes:     maker.votes,
				Signature: maker.aggregateSig.ToSignature(),
			},
			nil
	}
	return nil, nil
}
