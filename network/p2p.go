package network

import (
	"context"
	"fmt"
	"log"

	libp2p "github.com/libp2p/go-libp2p"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	multiaddr "github.com/multiformats/go-multiaddr"
)

type RoutedHost struct {
	node host.Host
	dht  *kaddht.IpfsDHT
}

func (rh RoutedHost) ID() peer.ID {
	return rh.node.ID()
}

func (rh RoutedHost) String() string {
	return rh.node.ID().String()
}

func (rh RoutedHost) PrintAddrs() {
	for _, addr := range rh.node.Addrs() {
		fmt.Printf("%s/p2p/%s\n", addr, rh.node.ID())
	}
}

func NewRoutedHost(ctx context.Context, addr string, priv crypto.PrivKey, bsPeers []string) (*RoutedHost, error) {
	// Create the address of the node
	nodeAddr, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	bootstrapPeers := make([]peer.AddrInfo, len(bsPeers))
	for _, p := range bsPeers {
		pinfo, err := peer.AddrInfoFromString(p)
		if err != nil {
			return nil, err
		}
		bootstrapPeers = append(bootstrapPeers, *pinfo)
	}

	// Set up routing
	var dht *kaddht.IpfsDHT
	newDHT := func(h host.Host) (routing.PeerRouting, error) {
		dht, err = kaddht.New(ctx, h, kaddht.Mode(kaddht.ModeServer), kaddht.BootstrapPeers(bootstrapPeers...))
		return dht, err
	}

	// Create node
	h, err := libp2p.New(
		libp2p.ListenAddrs(nodeAddr),
		libp2p.Identity(priv),
		libp2p.Routing(newDHT),
	)
	if err != nil {
		return nil, err
	}

	if err = dht.Bootstrap(ctx); err != nil {
		return nil, err
	}

	return &RoutedHost{h, dht}, nil
}
