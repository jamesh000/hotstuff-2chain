package consensus

import (
	"fmt"
	"log"

	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/jamesh000/hotstuff-2chain/store"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
)

// Should be be this if running in a serious libp2p network
const CONSENSUS_PROTOCOL string = "consensus"

// default channel capacity
const CHANNEL_CAPACITY = 1000

// consensus round number
type Round = uint64

type ConsensusMessage interface {
	ConsensusMessageMember()
}

type TestMessage struct {
	message string
}

func (msg TestMessage) ConsensusMessageMember() {}

type Consensus struct{}

func SpawnConsensus(
	name crypto.PublicKey,
	host network.RoutedHost,
	committee Committee,
	parameters Parameters,
	signatureService crypto.SignatureService,
	store store.Store,
	rxMempool <-chan crypto.Digest,
	txMempool chan<- struct{},
	txCommit chan<- Block,
) {
	consensusCh := make(chan ConsensusMessage, CHANNEL_CAPACITY)
	loopbackCh := make(chan Block, CHANNEL_CAPACITY)
	proposerCh := make(chan ProposerMessage, CHANNEL_CAPACITY)
	helperCh := make(chan HelperMessage, CHANNEL_CAPACITY)

	network.SpawnReceiver(
		host,
		ConsensusReceiverHandler{consensusCh, helperCh},
		protocol.ID(CONSENSUS_PROTOCOL),
	)
	log.Printf("Node %v listening to consensus messages with peerid %v", name, host.ID())

	leaderElector := NewRRLeaderElector(committee)

	// Not yet implemented
	//mempoolDriver := nil

	// Not yet implemented
	//synchronizer := nil

	SpawnCore(
		name,
		committee,
		host,
		signatureService,
		store,
		leaderElector,
		//mempoolDriver,
		//synchronizer,
		parameters.TimeoutDelay,
		consensusCh,
		loopbackCh,
		proposerCh,
		txCommit,
	)

	// Spawn the proposer (not implmented)
	//spawnProposer

	// Spawn the Helper (not implemented
	//spawnHelper
}

type ConsensusReceiverHandler struct {
	txConsensus chan<- ConsensusMessage
	txHelper    chan<- HelperMessage
}

func (c ConsensusReceiverHandler) Dispatch(writer msgio.WriteCloser, msg []byte) error {
	fmt.Println(string(msg))

	return nil
}
