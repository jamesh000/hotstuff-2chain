package node

import (
	"encoding/base64"
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

type SerializablePeerKey struct {
	Key libp2pcrypto.PrivKey
}

func (pk SerializablePeerKey) MarshalText() ([]byte, error) {
	data, err := libp2pcrypto.MarshalPrivateKey(pk.Key)
	if err != nil {
		return nil, err
	}

	return []byte(base64.StdEncoding.EncodeToString(data)), nil
}

func (pk *SerializablePeerKey) UnmarshalText(data []byte) error {
	decoded, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return err
	}

	key, err := libp2pcrypto.UnmarshalPrivateKey(decoded)
	if err != nil {
		return err
	}

	pk.Key = key
	return nil
}

type Secret struct {
	Name    crypto.PublicKey    `json:"name"`
	Secret  crypto.SecretKey    `json:"secret"`
	PeerKey SerializablePeerKey `json:"peerkey"`
}

type Parameters struct {
	Consensus consensus.Parameters `json:"consensus"`
	Mempool   mempool.Parameters   `json:"mempool"`
}

func DefaultParameters() Parameters {
	return Parameters{
		Consensus: consensus.DefaultParameters(),
		Mempool:   mempool.DefaultParameters(),
	}
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
