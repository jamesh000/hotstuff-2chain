package consensus

import (
	"fmt"
	"log"

	pb "github.com/jamesh000/hotstuff-2chain/consensuspb"
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/mempool"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/jamesh000/hotstuff-2chain/store"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"
)

// Should be be this if running in a serious libp2p network
const CONSENSUS_PROTOCOL string = "consensus"

// default channel capacity
const CHANNEL_CAPACITY = 1000

// consensus round number
type Round = uint64

type ConsensusMessage interface {
	SerializeConsensusMessage() ([]byte, error)
}

type TestMessage struct {
	message string
}

func (msg TestMessage) SerializeConsensusMessage() ([]byte, error) {
	testmsg := &pb.ConsensusMessage{
		Message: &pb.ConsensusMessage_Testfield{
			Testfield: &pb.ConsensusMessage_Test{
				Messagetext: msg.message,
			},
		},
	}

	data, err := proto.Marshal(testmsg)
	if err != nil {
		return nil, err
	}

	return data, nil
}

type ProposeMessage struct {
	block Block
}

func (msg ProposeMessage) SerializeConsensusMessage() ([]byte, error) {
	return nil, nil
}

func DeserializeConsensusMessage(data []byte) (ConsensusMessage, error) {
	msg := &pb.ConsensusMessage{}
	err := proto.Unmarshal(data, msg)
	if err != nil {
		return nil, err
	}

	switch msg.Message.(type) {
	case *pb.ConsensusMessage_Testfield:
		newTestMsg := &TestMessage{
			message: msg.GetTestfield().GetMessagetext(),
		}

		return newTestMsg, nil
	case *pb.ConsensusMessage_Proposal:
		return nil, fmt.Errorf("Proposal deserialization is not implemented yet")
	default:
		log.Fatal("Erroneous consensus message type")
	}

	return nil, fmt.Errorf("End of the line")
}

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

	mempoolDriver := NewMempoolDriver(store, txMempool, loopbackCh)

	// Not yet implemented
	//synchronizer := nil

	SpawnCore(
		name,
		committee,
		host,
		signatureService,
		store,
		leaderElector,
		mempoolDriver,
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
	consensusMsg, err := DeserializeConsensusMessage(msg)
	if err != nil {
		panic(err)
	}

	switch messageContents := consensusMsg.(type) {
	case *TestMessage:
		fmt.Println(messageContents.message)
	default:
		fmt.Println("Something terrible has occured")
	}

	return nil
}
