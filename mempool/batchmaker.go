package mempool

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type transaction = []byte
type batch = []transaction

type mempoolAddr struct {
	name crypto.PublicKey
	addr peer.ID
}

type batchMaker struct {
	batchSize        uint64
	maxBatchDelay    uint64
	rxTransaction    <-chan transaction
	txMessage        chan<- quorumWaiterMessage
	mempoolAddresses []mempoolAddr
	currentBatch     batch
	currentBatchSize uint64
	network          network.ReliableSender
}

func spawnBatchMaker(
	batchSize uint64,
	maxBatchDelay uint64,
	rxTransaction <-chan transaction,
	txMessage chan<- quorumWaiterMessage,
	mempoolAddresses []mempoolAddr,
	host network.RoutedHost,
) {
	newBatchMaker := batchMaker{
		batchSize:        batchSize,
		maxBatchDelay:    maxBatchDelay,
		rxTransaction:    rxTransaction,
		txMessage:        txMessage,
		mempoolAddresses: mempoolAddresses,
		currentBatch:     make(batch, 0, 2*batchSize),
		currentBatchSize: 0,
		network:          network.NewReliableSender(host, protocol.ID(mempoolProtocol)),
	}

	go newBatchMaker.run()
}

func (bm *batchMaker) run() {
	timer := time.NewTimer(time.Duration(bm.maxBatchDelay) * time.Millisecond)

	for {
		select {
		case tx := <-bm.rxTransaction:
			bm.currentBatchSize += uint64(len(tx))
			bm.currentBatch = append(bm.currentBatch, tx)
			if bm.currentBatchSize >= bm.batchSize {
				bm.seal()
				timer.Reset(time.Duration(bm.maxBatchDelay) * time.Millisecond)
			}
		case <-timer.C:
			if len(bm.currentBatch) != 0 {
				bm.seal()
			}
			timer.Reset(time.Duration(bm.maxBatchDelay) * time.Millisecond)
		}
	}
}

func serializeBatch(batch batch) []byte {
	buf := bytes.Buffer{}

	for _, tx := range batch {
		// write length as a 64 bit uint (8 bytes)
		binary.Write(&buf, binary.BigEndian, uint64(len(tx)))

		buf.Write(tx[:])
	}

	return buf.Bytes()
}

func deserializeBatch(data []byte) (batch, error) {
	r := bytes.NewReader(data)

	var batch batch

	for r.Len() > 0 {
		var length uint64

		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			return nil, err
		}

		if length > uint64(r.Len()) {
			return nil, fmt.Errorf("Invalid transaction length %d, only %d bytes left in batch", length, r.Len())
		}

		tx := make(transaction, length)
		if _, err := io.ReadFull(r, tx); err != nil {
			return nil, err
		}

		batch = append(batch, tx)
	}

	return batch, nil
}

func (bm *batchMaker) seal() {
	size := bm.currentBatchSize

	sealed := serializeBatch(bm.currentBatch)
	message := batchMessage{sealed}
	serialized, err := message.serializeMempoolMessage()
	if err != nil {
		log.Fatalf("Failed to serialize a batch message")
	}

	names := make([]crypto.PublicKey, len(bm.mempoolAddresses))
	addresses := make([]peer.ID, len(bm.mempoolAddresses))
	for i, ma := range bm.mempoolAddresses {
		names[i] = ma.name
		addresses[i] = ma.addr
	}

	ctx, cancel := context.WithCancel(context.Background())
	channels := bm.network.Broadcast(ctx, addresses, serialized)

	handlers := make([]handler, len(names))
	for i, name := range names {
		handlers[i] = handler{
			name: name,
			h:    channels[i],
		}
	}

	bm.txMessage <- quorumWaiterMessage{
		Batch:    sealed,
		Handlers: handlers,
		cancel:   cancel,
	}
}
