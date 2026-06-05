package network

import (
	"context"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type Pubsub struct {
	*pubsub.PubSub
	topic *pubsub.Topic
	sub   *pubsub.Subscription
}

func NewPubsub(ctx context.Context, host *RoutedHost, proto string) (*Pubsub, error) {
	var err error

	ps := new(Pubsub)
	ps.PubSub, err = pubsub.NewGossipSub(ctx, host.node)
	if err != nil {
		return nil, err
	}

	ps.topic, err = ps.Join(proto)
	if err != nil {
		return nil, err
	}

	ps.sub, err = ps.topic.Subscribe()
	if err != nil {
		return nil, err
	}

	return ps, nil
}

func (ps *Pubsub) Publish(ctx context.Context, data []byte) error {
	return ps.topic.Publish(ctx, data)
}

func (ps *Pubsub) Next(ctx context.Context) ([]byte, error) {
	msg, err := ps.sub.Next(ctx)
	if err != nil {
		return nil, err
	}

	return msg.Data, nil
}
