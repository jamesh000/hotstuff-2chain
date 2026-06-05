package consensus

import (
	"fmt"
	"log"
	"slices"

	"github.com/gammazero/deque"
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/jamesh000/hotstuff-2chain/store"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type Core struct {
	name               crypto.PublicKey
	committee          Committee
	store              store.Store
	signatureService   crypto.SignatureService
	leaderElector      leaderElector
	mempoolDriver      MempoolDriver
	synchronizer       Synchronizer
	rxMessage          <-chan consensusMessage
	rxLoopback         <-chan Block
	txProposer         chan<- proposerMessage
	txCommit           chan<- Block
	round              Round
	lastVotedRound     Round
	lastCommittedRound Round
	highQC             QC
	timer              *Timer
	aggregator         aggregator
	network            network.SimpleSender
}

func SpawnCore(
	name crypto.PublicKey,
	committee Committee,
	host network.RoutedHost,
	signatureService crypto.SignatureService,
	store store.Store,
	leaderElector leaderElector,
	mempoolDriver MempoolDriver,
	synchronizer Synchronizer,
	timeoutDelay uint64,
	rxMessage <-chan consensusMessage,
	rxLoopback <-chan Block,
	txProposer chan<- proposerMessage,
	txCommit chan<- Block,
) {
	newCore := Core{
		name:               name,
		committee:          committee,
		store:              store,
		signatureService:   signatureService,
		leaderElector:      leaderElector,
		mempoolDriver:      mempoolDriver,
		synchronizer:       synchronizer,
		rxMessage:          rxMessage,
		rxLoopback:         rxLoopback,
		txProposer:         txProposer,
		txCommit:           txCommit,
		round:              1,
		lastVotedRound:     0,
		lastCommittedRound: 0,
		highQC:             GenesisQC(),
		timer:              NewTimer(timeoutDelay),
		aggregator:         NewAggregator(committee),
		network:            *network.NewSimpleSender(host, protocol.ID(CONSENSUS_PROTOCOL)),
	}

	go newCore.run()
}

func (c *Core) storeBlock(block *Block) error {
	key := block.Digest()
	value, err := block.Serialize()
	if err != nil {
		return err
	}

	c.store.Write(key[:], value)

	return nil
}

func (c *Core) increaseLastVotedRound(target Round) {
	c.lastVotedRound = max(c.lastVotedRound, target)
}

func (c *Core) make_vote(block *Block) *vote {
	safetyRule1 := block.Round > c.lastVotedRound
	safetyRule2 := block.Qc.Round+1 == block.Round
	if tc := block.Tc; tc != nil {
		canExtend := tc.Round+1 == block.Round

		canExtend = canExtend && block.Qc.Round >= slices.Max(tc.HighQCRounds()[:])
		safetyRule2 = safetyRule2 || canExtend
	}

	if !(safetyRule1 && safetyRule2) {
		return nil
	}

	c.increaseLastVotedRound(block.Round)
	vote := NewVote(*block, c.name, c.signatureService)
	return &vote
}

func (c *Core) commit(block Block) error {
	if c.lastCommittedRound >= block.Round {
		return nil
	}

	toCommit := new(deque.Deque[Block])
	parent := &block
	for c.lastCommittedRound+1 < parent.Round {
		ancestor, err := c.synchronizer.getParentBlock(parent)
		if err != nil {
			panic(err)
		}

		toCommit.PushFront(*ancestor)
		parent = ancestor
	}
	toCommit.PushFront(block)

	c.lastCommittedRound = block.Round

	for b := range toCommit.Iter() {
		if len(b.Payload) != 0 {
			log.Printf("Committed %v\n", b)
		}

		c.txCommit <- block
	}

	return nil
}

func (c *Core) updateHighQc(qc *QC) {
	if qc.Round > c.highQC.Round {
		c.highQC = *qc
	}
}

func (c *Core) localTimeoutRound() error {
	log.Printf("Timeout reaqched for round %v\n", c.round)

	c.increaseLastVotedRound(c.round)

	timeout := NewTimeout(c.highQC, c.round, c.name, c.signatureService)

	c.timer.Reset()

	_, addresses := c.committee.BroadcastAddresses(c.name)
	message, err := (timeoutMessage{timeout}).SerializeConsensusMessage()
	if err != nil {
		return err
	}
	c.network.Broadcast(addresses, message)

	return c.handleTimeout(&timeout)
}

func (c *Core) handleVote(vote *vote) error {
	if vote.round < c.round {
		return nil
	}

	if err := vote.verify(c.committee); err != nil {
		return err
	}

	qc, err := c.aggregator.addVote(*vote)
	if err != nil {
		return err
	}

	if qc != nil {
		// process the QC
		c.processQC(qc)

		// Make new block if we are the next leader
		if c.name == c.leaderElector.getLeader(c.round) {
			c.generateProposal(nil)
		}
	}
	return nil
}

func (c *Core) handleTimeout(timeout *timeout) error {
	if timeout.round < c.round {
		return nil
	}

	if err := timeout.Verify(c.committee); err != nil {
		return err
	}

	c.processQC(&timeout.highQC)

	tc, err := c.aggregator.addTimeout(*timeout)
	if err != nil {
		return err
	}

	if tc != nil {
		c.advanceRound(tc.Round)

		_, addresses := c.committee.BroadcastAddresses(c.name)
		message, err := (tcMessage{*tc}).SerializeConsensusMessage()
		if err != nil {
			return err
		}
		c.network.Broadcast(addresses, message)

		if c.name == c.leaderElector.getLeader(c.round) {
			c.generateProposal(tc)
		}
	}
	return nil
}

func (c *Core) advanceRound(round Round) {
	if round < c.round {
		return
	}

	c.timer.Reset()

	c.round = round + 1
	log.Printf("Moved to round %v\n", c.round)

	// cleanup the vote aggregator
	c.aggregator.cleanup(c.round)
}

func (c *Core) generateProposal(tc *TC) {
	c.txProposer <- proposerMakeMessage{
		round: c.round,
		qc:    c.highQC,
		tc:    tc,
	}
}

func (c *Core) cleanupProposer(b0 *Block, b1 *Block, block *Block) {
	digests := make([]crypto.Digest, len(b0.Payload)+len(b1.Payload)+len(block.Payload))
	digests = append(digests, b0.Payload...)
	digests = append(digests, b1.Payload...)
	digests = append(digests, block.Payload...)

	c.txProposer <- proposerCleanupMessage{digests}
}

func (c *Core) processQC(qc *QC) {
	c.advanceRound(qc.Round)
	c.updateHighQc(qc)
}

func (c *Core) processBlock(block *Block) error {
	log.Printf("Processing %v\n", block)

	// check that we have block ancestors
	b0, b1, err := c.synchronizer.getAncestors(block)
	if err != nil {
		return nil
	}

	if b0 == nil || b1 == nil {
		log.Printf("Processing of %v suspended due to missing parent\n", block.Digest())
		return nil
	}

	// Only store the block if all ancestors have been processed
	c.storeBlock(block)

	c.cleanupProposer(b0, b1, block)

	// Check if we can commit the head of the 2-chain
	if b0.Round+1 == b1.Round {
		c.mempoolDriver.cleanup(b0.Round)
		if err := c.commit(*b0); err != nil {
			return err
		}
	}

	if vote := c.make_vote(block); vote != nil {
		nextLeader := c.leaderElector.getLeader(c.round + 1)
		if nextLeader == c.name {
			c.handleVote(vote)
		} else {
			log.Printf("Sending %v to %v\n", vote, nextLeader)
			address, ok := c.committee.Address(nextLeader)
			if !ok {
				return fmt.Errorf("The next leader is not in the committee")
			}
			message, err := (voteMessage{*vote}).SerializeConsensusMessage()
			if err != nil {
				return err
			}
			c.network.Send(address, message)
		}
	}
	return nil
}

func (c *Core) handleProposal(block *Block) error {
	digest := block.Digest()

	if block.Author != c.leaderElector.getLeader(block.Round) {
		return fmt.Errorf("Wrong leader %v for block %v in round %v", block.Author, digest, block.Round)
	}

	if err := block.Verify(c.committee); err != nil {
		return err
	}

	c.processQC(&block.Qc)

	if block.Tc != nil {
		c.advanceRound(block.Tc.Round)
	}

	if !c.mempoolDriver.verify(*block) {
		log.Printf("Processing of %v suspended: missing payload\n", digest)
		return nil
	}

	return c.processBlock(block)
}

func (c *Core) handleTC(tc TC) error {
	if err := tc.Verify(c.committee); err != nil {
		return err
	}

	if tc.Round < c.round {
		return nil
	}
	c.advanceRound(tc.Round)

	if c.name == c.leaderElector.getLeader(c.round) {
		c.generateProposal(&tc)
	}

	return nil
}

func (c *Core) run() {
	c.timer.Reset()
	if c.name == c.leaderElector.getLeader(c.round) {
		c.generateProposal(nil)
	}

	for {
		var result error
		select {
		case msg := <-c.rxMessage:
			switch m := msg.(type) {
			case *proposeMessage:
				result = c.handleProposal(&m.block)
			case *voteMessage:
				result = c.handleVote(&m.vote)
			case *timeoutMessage:
				result = c.handleTimeout(&m.timeout)
			case *tcMessage:
				result = c.handleTC(m.tc)
			default:
				panic(fmt.Errorf("Unexpected protocol message"))
			}
		case block := <-c.rxLoopback:
			result = c.processBlock(&block)
		case <-c.timer.timer.C:
			result = c.localTimeoutRound()
		}

		if result != nil {
			log.Printf("Error during consensus: %v\n", result)
		}
	}
}
