package consensus

import (
	"fmt"

	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/jamesh000/hotstuff-2chain/store"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type Core struct {
	name             crypto.PublicKey
	committee        Committee
	store            store.Store
	signatureService crypto.SignatureService
	leaderElector    leaderElector
	//mempoolDriver    MempoolDriver
	//synchronizer Synchronizer
	rxMessage          <-chan ConsensusMessage
	rxLoopback         <-chan Block
	txProposer         chan<- ProposerMessage
	txCommit           chan<- Block
	round              Round
	lastVotedRound     Round
	lastCommittedRound Round
	highQC             QC
	timer              *Timer
	aggregator         aggregator
	network            network.SimpleSender
}

func SpawnCore(
	name crypto.PublicKey,
	committee Committee,
	signatureService crypto.SignatureService,
	store store.Store,
	leaderElector leaderElector,
	//mempoolDriver MempoolDriver,
	//synchronizer Synchronizer,
	timeoutDelay uint64,
	rxMessage <-chan ConsensusMessage,
	rxLoopback <-chan Block,
	txProposer chan<- ProposerMessage,
	txCommit chan<- Block,
) {
	newCore := Core{
		name:             name,
		committee:        committee,
		store:            store,
		signatureService: signatureService,
		leaderElector:    leaderElector,
		//mempoolDriver: mempoolDriver,
		//synchronizer: synchronizer,
		rxMessage:          rxMessage,
		rxLoopback:         rxLoopback,
		txProposer:         txProposer,
		txCommit:           txCommit,
		round:              1,
		lastVotedRound:     0,
		lastCommittedRound: 0,
		highQC:             GenesisQC(),
		timer:              NewTimer(timeoutDelay),
		aggregator:         NewAggregator(committee),
		network:            *network.NewSimpleSender(protocol.ID(CONSENSUS_PROTOCOL)),
	}

	go newCore.run()
}

func (c *Core) run() {
	for msg := range c.rxMessage {
		switch m := msg.(type) {
		case TestMessage:
			fmt.Println(m.message)
		default:
			panic(fmt.Errorf("Unexpected protocol message"))
		}
	}
}
