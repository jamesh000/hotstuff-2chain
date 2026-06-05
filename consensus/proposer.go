package consensus

import (
	"context"
	"log"

	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/network"
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

type Proposer struct {
	name             crypto.PublicKey
	committee        Committee
	signatureService crypto.SignatureService
	rxMempool        <-chan crypto.Digest
	rxMessage        <-chan proposerMessage
	txLoopback       chan<- Block
	buffer           map[crypto.Digest]struct{}
	network          network.ReliableSender
}

func spawnProposer(
	name crypto.PublicKey,
	committee Committee,
	signatureService crypto.SignatureService,
	rxMempool <-chan crypto.Digest,
	rxMessage <-chan proposerMessage,
	txLoopback chan<- Block,
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
	}.run()
}

func stakeWaiter(waitFor <-chan []byte, deliver Stake, deliverTo chan<- Stake) {
	<-waitFor
	deliverTo <- deliver
}

func (p Proposer) makeBlock(round Round, qc QC, tc *TC) {
	payload := make([]crypto.Digest, len(p.buffer))
	for d, _ := range p.buffer {
		payload = append(payload, d)
	}

	block := NewBlock(qc, tc, p.name, round, payload, p.signatureService)

	log.Printf("Created %v\n", block)
	log.Printf("Broadcasting %v\n", block)

	names, addresses := p.committee.BroadcastAddresses(p.name)

	log.Printf("is the genesis: %v\n", block.Qc.IsGenesisQC())
	message, err := (proposeMessage{block}).SerializeConsensusMessage()
	if err != nil {
		panic(err)
	}
	msgb, err := DeserializeConsensusMessage(message)
	switch m := msgb.(type) {
	case *proposeMessage:
		log.Printf("is the genesis: %v\n", m.block.Qc.IsGenesisQC())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	responses := p.network.Broadcast(ctx, addresses, message)

	p.txLoopback <- block

	stakeCh := make(chan Stake)
	for i, response := range responses {
		go stakeWaiter(response, p.committee.Stake(names[i]), stakeCh)
	}

	totalStake := p.committee.Stake(p.name)
	for stake := range stakeCh {
		totalStake += stake
		if totalStake > p.committee.QuorumThreshold() {
			break
		}
	}
}

func (p Proposer) run() {
	for {
		select {
		case digest := <-p.rxMempool:
			p.buffer[digest] = struct{}{}
		case message := <-p.rxMessage:
			switch msg := message.(type) {
			case proposerMakeMessage:
				p.makeBlock(msg.round, msg.qc, msg.tc)
			case proposerCleanupMessage:
				for _, x := range msg.digests {
					delete(p.buffer, x)
				}
			}
		}
	}
}
