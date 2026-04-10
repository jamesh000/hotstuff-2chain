package network

import (
	"log"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
)

type MessageHandler interface {
	Dispatch(msgio.WriteCloser, []byte) error
}

/* Isn't really needed in libp2p
type Receiver struct {
	host    RoutedHost
	handler MessageHandler
	proto   protocol.ID
}
*/

func SpawnReceiver(host RoutedHost, handler MessageHandler, proto protocol.ID) {
	host.node.SetStreamHandler(proto, func(stream network.Stream) {
		peer := stream.Conn().RemotePeer()
		log.Printf("Incoming connection establised with %v\n", peer)
		spawnRunner(stream, peer, handler)
	})
}

func spawnRunner(stream network.Stream, pid peer.ID, handler MessageHandler) {
	reader := msgio.NewReader(stream)
	writer := msgio.NewWriter(stream)

	for {
		msg, err := reader.ReadMsg()
		if err != nil {
			log.Printf("Error reading from peer %v: %v\n", pid, err.Error())
			return
		}

		if err = handler.Dispatch(writer, msg); err != nil {
			log.Printf("Error dispatching received msg from peer %v: %v", pid, err.Error())
			return
		}
	}
}
