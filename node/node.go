package node

import (
	"log"

	"github.com/jamesh000/hotstuff-2chain/consensus"
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/store"
)

const CHANNEL_CAPACITY = 1000

type Node struct {
	commit chan consensus.Block
}

func NewNode(committeeFile string, keyFile string, storePath string, parameterFile *string) (*Node, error) {
	commit := make(chan consensus.Block, CHANNEL_CAPACITY)

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

	var parameters *Parameters
	if parameterFile != nil {
		parameters, err = ReadJSON[Parameters](*parameterFile)

	}

	store, err := store.NewStore(storePath)
	if err != nil {
		log.Fatal(err)
	}

	signatureService = crypto.NewSignatureService(secretKey)

	consensus.Spawn()

	log.Printf("Node %v successfully booted\n", name)
	return &Node{commit}, nil
}
