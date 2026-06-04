package consensus

import (
	"fmt"
	"log"

	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/mempool"
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

func SpawnConsensus(
	name crypto.PublicKey,
	host network.RoutedHost,
	committee Committee,
	parameters Parameters,
	signatureService crypto.SignatureService,
	store store.Store,
	rxMempool <-chan crypto.Digest,
	txMempool chan<- mempool.ConsensusMessage,
	txCommit chan<- Block,
) {
	consensusCh := make(chan consensusMessage, CHANNEL_CAPACITY)
	loopbackCh := make(chan Block, CHANNEL_CAPACITY)
	proposerCh := make(chan proposerMessage, CHANNEL_CAPACITY)
	helperCh := make(chan helperMessage, CHANNEL_CAPACITY)

	network.SpawnReceiver(
		host,
		ConsensusReceiverHandler{consensusCh, helperCh},
		protocol.ID(CONSENSUS_PROTOCOL),
	)
	log.Printf("Node %v listening to consensus messages with peerid %v", name, host.ID())

	leaderElector := NewRRLeaderElector(committee)

	// Create the mempool driver
	mempoolDriver := NewMempoolDriver(store, txMempool, loopbackCh)

	// Create the synchronizer
	synchronizer := NewSynchronizer(
		name,
		committee,
		store,
		loopbackCh,
		parameters.SyncRetryDelay,
		host,
		protocol.ID(CONSENSUS_PROTOCOL),
	)

	SpawnCore(
		name,
		committee,
		host,
		signatureService,
		store,
		leaderElector,
		mempoolDriver,
		synchronizer,
		parameters.TimeoutDelay,
		consensusCh,
		loopbackCh,
		proposerCh,
		txCommit,
	)

	// Spawn the proposer
	spawnProposer(
		name,
		committee,
		signatureService,
		rxMempool,
		proposerCh,
		loopbackCh,
		host,
		protocol.ID(CONSENSUS_PROTOCOL),
	)

	// Spawn the Helper
	SpawnHelper(committee, store, helperCh, host)
}

type ConsensusReceiverHandler struct {
	txConsensus chan<- consensusMessage
	txHelper    chan<- helperMessage
}

func (c ConsensusReceiverHandler) Dispatch(writer msgio.WriteCloser, msg []byte) error {
	// Deserialize the message
	consensusMsg, err := DeserializeConsensusMessage(msg)
	if err != nil {
		panic(err)
	}

	// Parse the message
	switch msg := consensusMsg.(type) {
	case *testMessage:
		fmt.Println(msg.message)
	case *syncRequestMessage:
		c.txHelper <- helperMessage{msg.missing, msg.origin}
	case *proposeMessage:
		writer.WriteMsg([]byte("Ack"))
		c.txConsensus <- msg
	default:
		c.txConsensus <- msg
	}

	return nil
}
