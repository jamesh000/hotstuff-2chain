package node

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/jamesh000/hotstuff-2chain/consensus"
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/mempool"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
)

type Committee struct {
	Consensus      consensus.Committee `json:"consensus"`
	Mempool        mempool.Committee   `json:"mempool"`
	BootstrapPeers []string            `json:"bspeers"`
}

type Secret struct {
	Name    crypto.PublicKey     `json:"consensusname"`
	Secret  crypto.SecretKey     `json:"consensussecret"`
	PeerKey libp2pcrypto.PrivKey `json:"peerkey"`
}

type Parameters struct {
	Consensus consensus.Parameters `json:"consensus"`
	Mempool   mempool.Parameters   `json:"mempool"`
}

func WriteJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize JSON: %w", err)
	}

	data = append(data, '\n')

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file %q: %w", path, err)
	}

	return nil
}

func ReadJSON[T any](path string) (*T, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %q: %w", path, err)
	}

	var v T

	if err := json.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("failed to deserialize JSON: %w", err)
	}

	return &v, nil
}
