package mempool

import (
	"log"
	"time"

	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/jamesh000/hotstuff-2chain/store"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const timerResolution uint64 = 1_000

type batchInfo struct {
	round     Round
	c         chan<- struct{}
	timestamp int64
}

type synchronizer struct {
	name           crypto.PublicKey
	committee      Committee
	store          store.Store
	gcDepth        Round
	syncRetryDelay uint64
	syncRetryNodes uint
	rxMessage      <-chan ConsensusMessage
	network        network.SimpleSender
	round          Round
	pending        map[crypto.Digest]batchInfo
}

func spawnSynchronizer(
	name crypto.PublicKey,
	committee Committee,
	store store.Store,
	gcDepth Round,
	syncRetryDelay uint64,
	syncRetryNodes uint,
	rxMessage <-chan ConsensusMessage,
	host network.RoutedHost,
) {
	newSynchronizer := synchronizer{
		name:           name,
		committee:      committee,
		store:          store,
		gcDepth:        gcDepth,
		syncRetryDelay: syncRetryDelay,
		syncRetryNodes: syncRetryNodes,
		rxMessage:      rxMessage,
		network:        *network.NewSimpleSender(host, protocol.ID(mempoolProtocol)),
		round:          0,
		pending:        make(map[crypto.Digest]batchInfo),
	}

	go newSynchronizer.run()
}

func (s *synchronizer) run() {
	waiting := make(chan crypto.Digest, channelCapacity)

	timer := time.NewTimer(time.Duration(timerResolution) * time.Millisecond)

	for {
		select {
		case consensusMessage := <-s.rxMessage:
			switch msg := consensusMessage.(type) {
			case consensusSynchronizeMessage:
				now := time.Now().UnixMilli()

				missing := make([]crypto.Digest, 0, len(msg.missing))
				for _, d := range msg.missing {
					if _, ok := s.pending[d]; ok {
						continue
					}

					missing = append(missing, d)

					cancelHandler := make(chan struct{})
					go func() {
						resultCh := s.store.NotifyReadChannel(d[:])
						select {
						case result := <-resultCh:
							if result.Err != nil {
								log.Println(result.Err)
								return
							}
							waiting <- crypto.Digest(*result.Value)
						case <-cancelHandler:
							return
						}
					}()

					s.pending[d] = batchInfo{
						round:     s.round,
						c:         cancelHandler,
						timestamp: now,
					}
				}

				address, ok := s.committee.MempoolAddress(msg.target)
				if !ok {
					log.Printf("Consensus asked us to sync with an unknown node: %v\n", msg.target)
					continue
				}

				message := requestMessage{
					missing: missing,
					origin:  s.name,
				}
				serialized, err := message.serializeMempoolMessage()
				if err != nil {
					log.Println(err)
					continue
				}
				s.network.Send(address, serialized)

			case consensusCleanupMessage:
				s.round = msg.round

				if s.round < s.gcDepth {
					continue
				}

				gcRound := s.round - s.gcDepth
				for d, info := range s.pending {
					if info.round < gcRound {
						info.c <- struct{}{}
						delete(s.pending, d)
					}
				}
			}
		case result := <-waiting:
			delete(s.pending, result)

		case <-timer.C:
			// Check if any requests are due to be resent
			now := time.Now().UnixMilli()

			retry := make([]crypto.Digest, 0, 10)
			for d, info := range s.pending {
				if info.timestamp+int64(s.syncRetryDelay) < now {
					retry = append(retry, d)
				}
			}

			if len(retry) != 0 {
				_, addresses := s.committee.BroadcastAddresses(s.name)
				message := requestMessage{
					missing: retry,
					origin:  s.name,
				}
				serialized, err := message.serializeMempoolMessage()
				if err != nil {
					log.Println(err)
					continue
				}
				s.network.LuckyBroadcast(addresses, serialized, s.syncRetryNodes)
			}

			timer.Reset(time.Duration(timerResolution) * time.Millisecond)
		}
	}
}
