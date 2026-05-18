package consensus

import (
	"bufio"
	"fmt"
	"os"

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
	mempoolDriver    MempoolDriver
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
	host network.RoutedHost,
	signatureService crypto.SignatureService,
	store store.Store,
	leaderElector leaderElector,
	mempoolDriver MempoolDriver,
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
		mempoolDriver:    mempoolDriver,
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
		network:            *network.NewSimpleSender(host, protocol.ID(CONSENSUS_PROTOCOL)),
	}

	//go newCore.run()

	// No parallelism for sloppy testing
	newCore.run()
}

func (c *Core) run() {
	/*
		for msg := range c.rxMessage {
			switch m := msg.(type) {
			case TestMessage:
				fmt.Println(m.message)
			default:
				panic(fmt.Errorf("Unexpected protocol message"))
			}
		}
	*/

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")

		if !scanner.Scan() {
			// Stop loop on EOF or error
			break
		}

		line := scanner.Text()

		testMsg := TestMessage{message: line}
		data, err := testMsg.SerializeConsensusMessage()
		if err != nil {
			panic(err)
		}

		c.network.Broadcast(c.committee.BroadcastAddresses(c.name), data)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "error reading input:", err)
	}
}
