package mempool

import (
	"context"
	"time"

	"github.com/jamesh000/hotstuff-2chain/crypto"
)

const disseminationDeadline = 500
const disseminationQueueMax = 10_000

type handler struct {
	name crypto.PublicKey
	h    <-chan []byte
}

type quorumWaiterMessage struct {
	Batch    sealedBatch
	Handlers []handler
	cancel   context.CancelFunc
}

type quorumWaiter struct {
	committee Committee
	stake     Stake
	rxMessage <-chan quorumWaiterMessage
	txBatch   chan<- sealedBatch
}

func spawnQuorumWaiter(committee Committee, stake Stake, rxMessage <-chan quorumWaiterMessage, txBatch chan<- sealedBatch) {
	newQW := quorumWaiter{
		committee: committee,
		stake:     stake,
		rxMessage: rxMessage,
		txBatch:   txBatch,
	}

	go newQW.run()
}

func (qw *quorumWaiter) run() {
	pending := make(chan struct{}, channelCapacity)
	pendingCounter := 0

	for {
		select {
		case qwMsg, ok := <-qw.rxMessage:
			if !ok {
				return
			}

			ctx, cancel := context.WithCancel(context.Background())

			waitForQuorum := make(chan Stake, channelCapacity)
			for _, handler := range qwMsg.Handlers {
				stake := qw.committee.Stake(handler.name)
				go func() {
					select {
					case <-handler.h:
						select {
						case waitForQuorum <- stake:
						case <-ctx.Done():
						}
					case <-ctx.Done():
					}
				}()
			}

			totalStake := Stake(0)

			// this makes it so progress is possible when the whole quorum is just this node
			go func() { waitForQuorum <- qw.stake }()

			// Get a quorum to acknowledge the batch before processing it further
			for s := range waitForQuorum {
				totalStake += s
				if totalStake >= qw.committee.QuorumThreshold() {
					qw.txBatch <- qwMsg.Batch
					break
				}
			}

			if pendingCounter <= disseminationQueueMax {
				pendingCounter += 1
				go func() {
					t := time.NewTimer(disseminationDeadline * time.Millisecond)
					defer t.Stop()

					defer func() { pending <- struct{}{} }()

					for {
						select {
						case <-waitForQuorum:
						case <-t.C:
							cancel()
							qwMsg.cancel()
							return
						}
					}
				}()
			}
		case <-pending:
			if pendingCounter > 0 {
				pendingCounter -= 1
			}
		}
	}
}
