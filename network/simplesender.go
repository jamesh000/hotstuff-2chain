package network

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type SimpleSender struct {
	connections map[peer.ID]connChannels
	host        RoutedHost
	proto       protocol.ID
}

func NewSimpleSender(proto protocol.ID) *SimpleSender {
	return &SimpleSender{
		connections: make(map[peer.ID]connChannels),
		proto:       proto,
	}
}

func (sender SimpleSender) Send(pid peer.ID, data []byte) error {
	conn, ok := sender.connections[pid]
	if ok {
		select {
		case conn.receiver <- data:
			return nil
		case <-conn.errCh:
		}
	}

	conn = spawnConnection(pid, sender.host, sender.proto)
	select {
	case conn.receiver <- data:
		sender.connections[pid] = conn
		return nil
	case err := <-conn.errCh:
		return fmt.Errorf("Send failed: %w", err)
	}
}

func (sender SimpleSender) Broadcast(pids []peer.ID, data []byte) {
	for _, pid := range pids {
		sender.Send(pid, data)
	}
}

func (sender SimpleSender) LuckyBroadCast(pids []peer.ID, data []byte, nodes uint) {
	rand.Shuffle(len(pids), func(i, j int) {
		pids[i], pids[j] = pids[j], pids[i]
	})

	if nodes < uint(len(pids)) {
		pids = pids[:nodes]
	}

	sender.Broadcast(pids, data)
}

type connChannels struct {
	receiver chan []byte
	errCh    chan error
}

type connection struct {
	pid      peer.ID
	receiver chan []byte
	errCh    chan error
}

func spawnConnection(pid peer.ID, host RoutedHost, proto protocol.ID) connChannels {
	receiver := make(chan []byte, 1000)
	errCh := make(chan error, 1)
	c := connection{pid, receiver, errCh}
	ctx := context.Background()

	go c.run(host, proto, ctx)

	return connChannels{receiver, errCh}
}

func (c *connection) run(host RoutedHost, proto protocol.ID, ctx context.Context) {
	// connect
	peerInfo := host.node.Peerstore().PeerInfo(c.pid)

	if len(peerInfo.Addrs) == 0 {
		var err error
		peerInfo, err = host.dht.FindPeer(ctx, c.pid)
		if err != nil {
			c.errCh <- fmt.Errorf("Failed to locate peer %v through dht", c.pid)
			return
		}
	}

	if err := host.node.Connect(ctx, peerInfo); err != nil {
		c.errCh <- fmt.Errorf("Failed to connect to peer %v", c.pid)
		return
	}

	stream, err := host.node.NewStream(ctx, c.pid, proto)
	if err != nil {
		c.errCh <- fmt.Errorf("Failed to open stream to peer %v with protocol %v", c.pid, proto)
		return
	}

	// transmit messages from the receiver
	for data := range c.receiver {
		if _, err := stream.Write(data); err != nil {
			c.errCh <- fmt.Errorf("Write broke for peer %v", c.pid)
			return
		}
	}
}
