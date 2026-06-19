package testutil

import (
	"context"
	"crypto/rand"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/jamesh000/hotstuff-2chain/consensus"
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/mempool"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/jamesh000/hotstuff-2chain/node"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Helper function for creating a boostrap node
func CreateBootstrap(t *testing.T) ([]string, error) {
	priv, _, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}

	host, err := network.NewRoutedHost(context.Background(), "/ip4/0.0.0.0/tcp/0", priv, nil)
	if err != nil {
		return nil, err
	}

	return host.Addrs(), nil
}

// Helper function for creating the secrets and committee
func CreateConfig(t *testing.T, count uint, bootstrapAddrs []string) (string, []string, error) {
	dir := t.TempDir()

	secretFiles := make([]string, 0, count)
	authorityInfos := make([]consensus.AuthorityInfo, 0, count)

	for i := range count {
		secret, name := crypto.GenerateKeypair()
		peerkey, _, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return "", nil, err
		}

		newSecret := node.Secret{
			Secret:  secret,
			Name:    name,
			PeerKey: node.SerializablePeerKey{Key: peerkey},
		}

		secretFileName := fmt.Sprintf("%v/%v.sc", dir, i)

		node.WriteJSON(secretFileName, newSecret)

		secretFiles = append(secretFiles, secretFileName)

		// Add to the committee
		address, err := peer.IDFromPrivateKey(peerkey)
		if err != nil {
			return "", nil, err
		}
		ithAuthority := consensus.AuthorityInfo{
			Author:  name,
			Stake:   1,
			Address: address,
		}

		authorityInfos = append(authorityInfos, ithAuthority)
	}

	consensusCommittee := consensus.NewCommittee(authorityInfos, 1)
	mempoolCommitee := mempool.Committee{Empty: "nothing for now"}

	newCommittee := node.Committee{
		Consensus:      consensusCommittee,
		Mempool:        mempoolCommitee,
		BootstrapPeers: bootstrapAddrs,
	}

	committeeFile := filepath.Join(dir, "committee")
	node.WriteJSON(committeeFile, newCommittee)

	return committeeFile, secretFiles, nil
}

func LoadConfig(committeeFile string, secretFiles []string) (*node.Committee, []node.Secret, error) {
	committee, err := node.ReadJSON[node.Committee](committeeFile)
	if err != nil {
		return nil, nil, err
	}

	secrets := make([]node.Secret, 0, len(secretFiles))
	for _, secretFile := range secretFiles {
		secret, err := node.ReadJSON[node.Secret](secretFile)
		if err != nil {
			return nil, nil, err
		}

		secrets = append(secrets, *secret)
	}

	return committee, secrets, nil
}
