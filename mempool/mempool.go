package mempool

import (
	"fmt"
	"log"

	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/jamesh000/hotstuff-2chain/store"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
)

const channelCapacity = 1_000

const mempoolProtocol string = "mempool"
const clientProtocol string = "client"

type Round = uint64

type ConsensusMessage interface {
	consensusMempoolMessageMember()
}

type SynchronizeMessage struct {
	Missing []crypto.Digest
	Target  crypto.PublicKey
}

func (msg SynchronizeMessage) consensusMempoolMessageMember() {}

type CleanupMessage struct {
	Round Round
}

func (msg CleanupMessage) consensusMempoolMessageMember() {}

type Mempool struct {
	name        crypto.PublicKey
	committee   Committee
	parameters  Parameters
	store       store.Store
	txConsensus chan<- crypto.Digest
}

func SpawnMempool(
	name crypto.PublicKey,
	host *network.RoutedHost,
	committee Committee,
	parameters Parameters,
	store store.Store,
	rxConsensus <-chan ConsensusMessage,
	txConsensus chan<- crypto.Digest,
) {
	newMempool := Mempool{
		name:        name,
		committee:   committee,
		parameters:  parameters,
		store:       store,
		txConsensus: txConsensus,
	}

	newMempool.handleConsensusMessages(rxConsensus, host)
	newMempool.handleClientsTransactions(host)
	newMempool.handleMempoolMessages(host)

	addr, ok := newMempool.committee.MempoolAddress(newMempool.name)
	if !ok {
		log.Fatalf("Our publickey is not in the committee\n")
	}

	log.Printf("Mempool booted on %v", addr)
}

func (mp *Mempool) handleConsensusMessages(rxConsensus <-chan ConsensusMessage, host *network.RoutedHost) {
	spawnSynchronizer(
		mp.name,
		mp.committee,
		mp.store,
		mp.parameters.GcDepth,
		mp.parameters.SyncRetryDelay,
		mp.parameters.SyncRetryNodes,
		rxConsensus,
		host,
	)
}

func (mp *Mempool) handleClientsTransactions(host *network.RoutedHost) {
	batchMakerCh := make(chan transaction, channelCapacity)
	quorumWaiterCh := make(chan quorumWaiterMessage, channelCapacity)
	processorCh := make(chan sealedBatch, channelCapacity)

	network.SpawnReceiver(*host, txReceiverHandler{batchMakerCh}, protocol.ID(clientProtocol))

	names, addresses := mp.committee.BroadcastAddresses(mp.name)
	spawnBatchMaker(
		uint64(mp.parameters.BatchSize),
		uint64(mp.parameters.MaxBatchDelay),
		batchMakerCh,
		quorumWaiterCh,
		names,
		addresses,
		*host,
	)

	spawnQuorumWaiter(
		mp.committee,
		mp.committee.Stake(mp.name),
		quorumWaiterCh,
		processorCh,
	)

	spawnProcessor(
		mp.store,
		processorCh,
		mp.txConsensus,
	)

	address, _ := mp.committee.MempoolAddress(mp.name)
	log.Printf("Mempool listening for client transactions on %v\n", address)
}

func (mp *Mempool) handleMempoolMessages(host *network.RoutedHost) {
	helperCh := make(chan helperRequest, channelCapacity)
	processorCh := make(chan sealedBatch, channelCapacity)

	network.SpawnReceiver(
		*host,
		mempoolReceiverHandler{
			helperCh,
			processorCh,
		},
		protocol.ID(mempoolProtocol),
	)

	SpawnHelper(
		mp.committee,
		mp.store,
		helperCh,
		*host,
	)

	spawnProcessor(
		mp.store,
		processorCh,
		mp.txConsensus,
	)

	address, _ := mp.committee.MempoolAddress(mp.name)
	log.Printf("Mempool listening for client transactions on %v\n", address)
}

type txReceiverHandler struct {
	txBatchMaker chan<- transaction
}

func (h txReceiverHandler) Dispatch(writer msgio.WriteCloser, msg []byte) error {
	h.txBatchMaker <- msg

	return nil
}

type mempoolReceiverHandler struct {
	txHelper    chan<- helperRequest
	txProcessor chan<- sealedBatch
}

func (h mempoolReceiverHandler) Dispatch(writer msgio.WriteCloser, msg []byte) error {
	writer.WriteMsg([]byte("Ack"))

	deserialized, err := DeserializeMempoolMessage(msg)
	if err != nil {
		return err
	}

	switch m := deserialized.(type) {
	case batchMessage:
		h.txProcessor <- m.batch
	case requestMessage:
		h.txHelper <- helperRequest{m.missing, m.origin}
	default:
		return fmt.Errorf("Invalid mempool message type")
	}

	return nil
}
