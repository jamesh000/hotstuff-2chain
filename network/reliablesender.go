package network

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/gammazero/deque"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
)

const RELIABLE_RETRY_DELAY = 200

type ReliableSender struct {
	connections map[peer.ID]chan<- innerMessage
	host        RoutedHost
	proto       protocol.ID
}

func NewReliableSender(host RoutedHost, proto protocol.ID) ReliableSender {
	return ReliableSender{
		connections: make(map[peer.ID]chan<- innerMessage),
		host:        host,
		proto:       proto,
	}
}

func (sender ReliableSender) spawnCon(address peer.ID, host RoutedHost, proto protocol.ID) chan<- innerMessage {
	ch := make(chan innerMessage)
	SpawnConnection(address, host, ch, proto)
	return ch
}

func (sender ReliableSender) Send(ctx context.Context, address peer.ID, data []byte) <-chan []byte {
	responseCh := make(chan []byte, 1)

	conn, ok := sender.connections[address]
	if !ok {
		conn = sender.spawnCon(address, sender.host, sender.proto)
		sender.connections[address] = conn
	}

	conn <- innerMessage{ctx, data, responseCh}

	return responseCh
}

func (sender ReliableSender) Broadcast(ctx context.Context, addresses []peer.ID, data []byte) []<-chan []byte {
	responses := make([]<-chan []byte, len(addresses))
	for i, a := range addresses {
		responses[i] = sender.Send(ctx, a, data)
	}

	return responses
}

func (sender ReliableSender) LuckyBroadcast(ctx context.Context, addresses []peer.ID, data []byte, nodes uint) []<-chan []byte {
	shuffledPids := make([]peer.ID, len(addresses))
	copy(shuffledPids, addresses)
	rand.Shuffle(len(shuffledPids), func(i, j int) {
		shuffledPids[i], shuffledPids[j] = shuffledPids[j], shuffledPids[i]
	})

	if nodes < uint(len(shuffledPids)) {
		shuffledPids = shuffledPids[:nodes]
	}

	return sender.Broadcast(ctx, shuffledPids, data)
}

type innerMessage struct {
	ctx      context.Context
	data     []byte
	response chan<- []byte
}

type reliableConnection struct {
	address    peer.ID
	proto      protocol.ID
	host       RoutedHost
	receiver   <-chan innerMessage
	retryDelay uint64
	buffer     *deque.Deque[innerMessage]
}

func SpawnConnection(address peer.ID, host RoutedHost, receiver <-chan innerMessage, proto protocol.ID) {
	go reliableConnection{
		address:    address,
		proto:      proto,
		host:       host,
		receiver:   receiver,
		retryDelay: RELIABLE_RETRY_DELAY,
		buffer:     new(deque.Deque[innerMessage]),
	}.run()
}

func (c reliableConnection) run() {
	delay := c.retryDelay
	retry := 0

	for {
		ctx := context.Background()
		stream, err := c.host.dhtConnect(ctx, c.address, c.proto)
		if err != nil {
			log.Printf("Failed to connect to peer %v: %v\n", c.address, err)

			timer := time.NewTimer(time.Duration(c.retryDelay) * time.Millisecond)

		waiter:
			for {
				select {
				case <-timer.C:
					delay = min(2*delay, 60000)
					retry = retry + 1
					break waiter
				case msg := <-c.receiver:
					c.buffer.PushBack(msg)
				}
			}
		} else {
			log.Printf("Outgoing connection established with %v\n", c.address)

			delay = c.retryDelay
			retry = 0

			err := c.keepAlive(stream)
			log.Printf("%v\n", err)
		}
	}
}

func (c reliableConnection) keepAlive(stream msgio.ReadWriteCloser) error {
	pendingReplies := new(deque.Deque[innerMessage])

	readerCh := make(chan []byte, 10)

	go func() {
		for {
			data, err := stream.ReadMsg()
			if err != nil {
				close(readerCh)
			}
			readerCh <- data
		}
	}()

	var finalErr error = nil

connection:
	for {
		for c.buffer.Len() != 0 {
			msg := c.buffer.PopFront()
			if msg.ctx.Err() != nil {
				continue
			}

			err := stream.WriteMsg(msg.data)
			if err != nil {
				c.buffer.PushFront(msg)
				finalErr = fmt.Errorf("Failed to send message to peer %v: %v", c.address, err)
				break connection
			}

			pendingReplies.PushBack(msg)
		}

		select {
		case msg := <-c.receiver:
			c.buffer.PushBack(msg)
		case response, ok := <-readerCh:
			if !ok {
				finalErr = fmt.Errorf("Read from peer %v broke", c.address)
				break connection
			}

			if pendingReplies.Len() == 0 {
				finalErr = fmt.Errorf("Unexpected ack from %v", c.address)
				break connection
			}

			msg := pendingReplies.PopFront()
			msg.response <- response
		}
	}

	for pendingReplies.Len() != 0 {
		c.buffer.PushFront(pendingReplies.PopBack())
	}

	return finalErr
}
