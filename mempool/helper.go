package mempool

import (
	"log"

	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/jamesh000/hotstuff-2chain/store"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type helperRequest struct {
	digests   []crypto.Digest
	requester crypto.PublicKey
}

type helper struct {
	committee Committee
	store     store.Store
	rxRequest <-chan helperRequest
	network   network.SimpleSender
}

func SpawnHelper(committee Committee, store store.Store, rxRequest <-chan helperRequest, host network.RoutedHost) {
	newHelper := helper{
		committee: committee,
		store:     store,
		rxRequest: rxRequest,
		network:   *network.NewSimpleSender(host, protocol.ID(mempoolProtocol)),
	}

	go newHelper.run()
}

func (h *helper) run() {
	for req := range h.rxRequest {
		address, ok := h.committee.MempoolAddress(req.requester)
		if !ok {
			log.Printf("Recieved batch request from unknown authority %v", req.requester)
			continue
		}

		for _, digest := range req.digests {
			batch, err := h.store.Read(digest[:])
			if err != nil {
				log.Println(err)
				continue
			}

			if batch != nil {
				message := batchMessage{batch: *batch}
				data, err := message.serializeMempoolMessage()
				if err != nil {
					log.Println(err)
					continue
				}

				h.network.Send(address, data)
			}
		}
	}
}
