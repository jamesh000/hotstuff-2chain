package consensus

import (
	"context"
	"log"
	"time"

	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/jamesh000/hotstuff-2chain/state"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type proposerMessage interface {
	proposerMessageMember()
}

type proposerMakeMessage struct {
	round Round
	qc    QC
	tc    *TC
}

func (msg proposerMakeMessage) proposerMessageMember() {}

type proposerCleanupMessage struct {
	digests []crypto.Digest
}

func (msg proposerCleanupMessage) proposerMessageMember() {}

// makeBlockRequest is a proposal whose parent state root we are waiting for.
type makeBlockRequest struct {
	id    uint64
	round Round
	qc    QC
	tc    *TC
}

type makeBlockResult struct {
	id   uint64
	root state.RootResult
}

type Proposer struct {
	name             crypto.PublicKey
	committee        Committee
	signatureService crypto.SignatureService
	rxMempool        <-chan crypto.Digest
	rxMessage        <-chan proposerMessage
	txLoopback       chan<- Block
	buffer           map[crypto.Digest]struct{}
	network          network.ReliableSender
	state            state.Client
	results          chan makeBlockResult
	nextMakeID       uint64
	currentMake      *makeBlockRequest
}

func spawnProposer(
	name crypto.PublicKey,
	committee Committee,
	signatureService crypto.SignatureService,
	rxMempool <-chan crypto.Digest,
	rxMessage <-chan proposerMessage,
	txLoopback chan<- Block,
	stateClient state.Client,
	host network.RoutedHost,
	proto protocol.ID,
) {
	go Proposer{
		name:             name,
		committee:        committee,
		signatureService: signatureService,
		rxMempool:        rxMempool,
		rxMessage:        rxMessage,
		txLoopback:       txLoopback,
		buffer:           make(map[crypto.Digest]struct{}),
		network:          network.NewReliableSender(host, proto),
		state:            stateClient,
		results:          make(chan makeBlockResult, CHANNEL_CAPACITY),
	}.run()
}

func stakeWaiter(waitFor <-chan []byte, deliver Stake, deliverTo chan<- Stake) {
	<-waitFor
	deliverTo <- deliver
}

func (p Proposer) makeBlock(round Round, qc QC, tc *TC, stateRoot crypto.Digest) {
	payload := make([]crypto.Digest, 0, len(p.buffer))
	for d, _ := range p.buffer {
		payload = append(payload, d)
		delete(p.buffer, d)
	}

	// for debugging, slows the creation of blocks when there is no payload
	if len(payload) == 0 {
		time.Sleep(3 * time.Second)
	}

	block := NewBlock(qc, tc, p.name, round, payload, stateRoot, p.signatureService)

	log.Printf("Created %v\n", block)
	log.Printf("Broadcasting %v\n", block)

	names, addresses := p.committee.BroadcastAddresses(p.name)

	message, err := (proposeMessage{block}).SerializeConsensusMessage()
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	responses := p.network.Broadcast(ctx, addresses, message)

	p.txLoopback <- block

	stakeCh := make(chan Stake)
	for i, response := range responses {
		go stakeWaiter(response, p.committee.Stake(names[i]), stakeCh)
	}

	totalStake := Stake(0)

	// Allows progress when only one node in committee
	go func() { stakeCh <- p.committee.Stake(p.name) }()

	for stake := range stakeCh {
		totalStake += stake
		log.Printf("Got ack, totalstake is %v, threshold is %v\n", totalStake, p.committee.QuorumThreshold())
		if totalStake >= p.committee.QuorumThreshold() {
			break
		}
	}
	log.Printf("Got all acks, totalStake = %v\n", totalStake)
}

func (p Proposer) run() {
	for {
		select {
		case digest := <-p.rxMempool:
			log.Printf("Got digest %v from mempool\n", digest)
			p.buffer[digest] = struct{}{}
		case message := <-p.rxMessage:
			switch msg := message.(type) {
			case proposerMakeMessage:
				p.currentMake = &makeBlockRequest{
					id:    p.nextMakeID,
					round: msg.round,
					qc:    msg.qc,
					tc:    msg.tc,
				}
				p.nextMakeID++

				// Wait for the parent block's state root without blocking the
				// loop; a newer proposal supersedes this one.
				go func(req makeBlockRequest) {
					res := <-p.state.Root(req.qc.Hash, req.qc.Round)
					p.results <- makeBlockResult{id: req.id, root: res}
				}(*p.currentMake)
			case proposerCleanupMessage:
				for _, x := range msg.digests {
					delete(p.buffer, x)
				}
			}
		case res := <-p.results:
			if p.currentMake == nil || res.id != p.currentMake.id {
				// a newer proposal superseded this one
				continue
			}

			req := p.currentMake
			p.currentMake = nil

			if res.root.Err != nil {
				log.Printf("Failed to get state root of parent %v: %v\n", req.qc.Hash, res.root.Err)
				continue
			}

			p.makeBlock(req.round, req.qc, req.tc, res.root.Root)
		}
	}
}
