package node

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/jamesh000/hotstuff-2chain/consensus"
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/execution"
	"github.com/jamesh000/hotstuff-2chain/mempool"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/jamesh000/hotstuff-2chain/state"
	"github.com/jamesh000/hotstuff-2chain/store"
)

const CHANNEL_CAPACITY = 1000

type Node struct {
	commit chan consensus.Block
	store  store.Store
}

func NewNode(committeeFile string, keyFile string, storePath string, parameterFile *string) (*Node, error) {
	commit := make(chan consensus.Block, CHANNEL_CAPACITY)
	consensusToMempoolCh := make(chan mempool.ConsensusMessage, CHANNEL_CAPACITY)
	mempoolToConsensusCh := make(chan crypto.Digest, CHANNEL_CAPACITY)

	committee, err := ReadJSON[Committee](committeeFile)
	if err != nil {
		return nil, err
	}

	secret, err := ReadJSON[Secret](keyFile)
	if err != nil {
		return nil, err
	}
	name := secret.Name
	secretKey := secret.Secret
	peerPriv := secret.PeerKey.Key

	var parameters *Parameters
	if parameterFile != nil {
		parameters, err = ReadJSON[Parameters](*parameterFile)
	} else {
		p := DefaultParameters()
		parameters = &p
	}

	store, err := store.NewStore(storePath)
	if err != nil {
		return nil, err
	}

	stateClient, err := state.Spawn(storePath+".state", *store, execution.TestExecutor{})
	if err != nil {
		return nil, err
	}

	signatureService := crypto.NewSignatureService(secretKey)

	host, err := network.NewRoutedHost(context.Background(), "/ip4/0.0.0.0/tcp/0", peerPriv, committee.BootstrapPeers)
	if err != nil {
		return nil, err
	}

	mempool.SpawnMempool(
		name,
		host,
		committee.Mempool,
		parameters.Mempool,
		*store,
		consensusToMempoolCh,
		mempoolToConsensusCh,
	)

	consensus.SpawnConsensus(
		name,
		*host,
		committee.Consensus,
		parameters.Consensus,
		signatureService,
		*store,
		*stateClient,
		mempoolToConsensusCh,
		consensusToMempoolCh,
		commit,
	)

	return &Node{commit, *store}, nil
}

func (n *Node) ProcessBlocks() {
	w := bufio.NewWriter(os.Stdout)
	for block := range n.commit {
		fmt.Fprintf(w, "%v HAS BEEN COMMITTED!\n", block)
		fmt.Fprintln(w, "It contains the following batches:")
		for _, digest := range block.Payload {
			fmt.Fprintf(w, "Batch %v:\n", digest)

			batchBytes, err := n.store.Read(digest[:])
			if err != nil {
				fmt.Fprintln(w, "Failed to read batch from store")
				continue
			}

			if batchBytes == nil {
				fmt.Fprintln(w, "We don't have this batch in our store")
				continue
			}

			batch, err := mempool.DeserializeBatch(*batchBytes)
			if err != nil {
				fmt.Fprintln(w, "Failed to deserialize batch")
				continue
			}

			for _, tx := range batch {
				parsed, err := execution.DeserializeTransaction(tx)
				if err != nil {
					fmt.Fprintf(w, "  (invalid transaction: %v)\n", err)
					continue
				}
				fmt.Fprintf(w, "  %v\n", parsed)
			}
		}
		w.Flush()
	}
}
