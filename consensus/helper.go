package consensus

import (
	"log"

	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/jamesh000/hotstuff-2chain/store"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type helperMessage struct {
	missing crypto.Digest
	origin  crypto.PublicKey
}

type Helper struct {
	committee  Committee
	store      store.Store
	rxRequests <-chan helperMessage
	network    network.SimpleSender
}

func SpawnHelper(committee Committee, store store.Store, rxRequests <-chan helperMessage, host network.RoutedHost) {
	go Helper{
		committee:  committee,
		store:      store,
		rxRequests: rxRequests,
		network:    *network.NewSimpleSender(host, protocol.ID(CONSENSUS_PROTOCOL)),
	}.run()
}

func (h Helper) run() {
	for req := range h.rxRequests {
		address, ok := h.committee.Address(req.origin)
		if !ok {
			log.Printf("Received sync request from unknown authority: %v\n", req.origin)
			continue
		}

		bytes, err := h.store.Read(req.missing[:])
		if err != nil {
			continue
		}

		block, err := new(Block).Deserialize(*bytes)
		if err != nil {
			log.Println("Failed to deserialize our own block")
			continue
		}

		message, err := (proposeMessage{*block}).SerializeConsensusMessage()
		if err != nil {
			log.Println("Failed to serialize propose message")
			continue
		}

		h.network.Send(address, message)
	}
}
