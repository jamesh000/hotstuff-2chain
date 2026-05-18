package consensus

import (
	"log"

	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/mempool"
	"github.com/jamesh000/hotstuff-2chain/store"
)

type pwCommandType = uint

type MempoolDriver struct {
	store           store.Store
	txMempool       chan<- mempool.ConsensusMessage
	txPayloadWaiter chan<- PayloadWaiterMessage
}

func NewMempoolDriver(store store.Store, txMempool chan<- mempool.ConsensusMessage, txLoopback chan<- Block) MempoolDriver {
	payloadWaiterCh := make(chan PayloadWaiterMessage, CHANNEL_CAPACITY)

	SpawnPayloadWaiter(store, payloadWaiterCh, txLoopback)

	return MempoolDriver{
		store:           store,
		txMempool:       txMempool,
		txPayloadWaiter: payloadWaiterCh,
	}
}

func (driver MempoolDriver) verify(block Block) bool {
	missing := make([]crypto.Digest, 0, len(block.Payload))
	for _, x := range block.Payload {
		if _, err := driver.store.Read(x[:]); err != nil {
			missing = append(missing, x)
		}
	}

	if len(missing) == 0 {
		return true
	}

	message := mempool.SynchronizeMessage{Missing: missing, Author: block.Author}
	driver.txMempool <- message

	driver.txPayloadWaiter <- PayloadWaiterMessage{msgType: waitCommand, missing: missing, block: &block}

	return false
}

func (driver MempoolDriver) cleanup(round Round) {
	driver.txMempool <- mempool.CleanupMessage{Round: round}

	driver.txPayloadWaiter <- PayloadWaiterMessage{cleanupCommand, nil, nil, round}
}

const (
	waitCommand pwCommandType = iota
	cleanupCommand
)

type PayloadWaiterMessage struct {
	msgType pwCommandType
	missing []crypto.Digest
	block   *Block
	round   Round
}

type PayloadWaiter struct {
	store      store.Store
	rxMessage  <-chan PayloadWaiterMessage
	txLoopback chan<- Block
}

func SpawnPayloadWaiter(store store.Store, rxMessage <-chan PayloadWaiterMessage, txLoopback chan<- Block) {
	newWaiter := PayloadWaiter{
		store:      store,
		rxMessage:  rxMessage,
		txLoopback: txLoopback,
	}

	go newWaiter.run()
}

type waiterResult struct {
	block *Block
	err   error
}

func spawnWaiter(missing []crypto.Digest, store store.Store, deliver *Block, handler chan struct{}, result chan<- waiterResult) {
	go func() {
		nrCount := len(missing)
		errCh := make(chan error, 10)

		for _, d := range missing {
			go func() {
				_, err := store.NotifyRead(d[:])
				errCh <- err
			}()
		}

		select {
		case err := <-errCh:
			if err != nil {
				result <- waiterResult{nil, err}
				return
			}

			nrCount := nrCount - 1
			if nrCount == 0 {
				result <- waiterResult{deliver, nil}
				return
			}
		case <-handler:
			return
		}
	}()
}

type pendingInfo struct {
	round   Round
	handler chan struct{}
}

func (pw PayloadWaiter) run() {
	resultCh := make(chan waiterResult, CHANNEL_CAPACITY)
	pending := make(map[crypto.Digest]pendingInfo)

	for {
		select {
		case msg := <-pw.rxMessage:
			switch msg.msgType {
			case waitCommand:
				blockDigest := msg.block.Digest()

				if _, ok := pending[blockDigest]; ok {
					continue
				}

				cancelCh := make(chan struct{}, 1)
				pending[blockDigest] = pendingInfo{msg.block.Round, cancelCh}
				spawnWaiter(msg.missing, pw.store, msg.block, cancelCh, resultCh)
			case cleanupCommand:
				for blockDigest, pi := range pending {
					if pi.round <= msg.round {
						pi.handler <- struct{}{}
						delete(pending, blockDigest)
					}
				}
			}
		case result := <-resultCh:
			if result.err != nil {
				log.Println(result.err)
			} else {
				delete(pending, result.block.Digest())
				pw.txLoopback <- *result.block
			}
		}
	}
}
