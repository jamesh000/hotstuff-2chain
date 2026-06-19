package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/jamesh000/hotstuff-2chain/consensus"
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/mempool"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/jamesh000/hotstuff-2chain/node"
	"github.com/jamesh000/hotstuff-2chain/store"
	"github.com/urfave/cli/v3"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Used to check the store is working properly
func storeTest(ctx context.Context, cmd *cli.Command) error {
	storage, err := store.NewStore("storefile")
	if err != nil {
		panic(err)
	}

	storage.Write([]byte("billy"), []byte("bob"))
	storage.Write([]byte("keith"), []byte("woods"))

	result, err := storage.Read([]byte("billy"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("Got the value for billy, it's %v\n", string(*result))

	storage.Write([]byte("billy"), []byte("the bat"))

	result, err = storage.Read([]byte("billy"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("Got the value for billy, it's %v\n", string(*result))

	go func() {
		time.Sleep(5 * time.Second)
		storage.Write([]byte("critical value"), []byte("super critical"))
	}()

	go func() {
		resultNR, err := storage.NotifyRead([]byte("critical value"))
		if err != nil {
			panic(err)
		}
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("I got it too, %v\n", string(resultNR))
	}()

	result, err = storage.Read([]byte("critical value"))
	if err == pebble.ErrNotFound {
		fmt.Println("Just as expected, it wasn't found")
	} else if err != nil {
		panic(err)
	}

	resultNR, err := storage.NotifyRead([]byte("critical value"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("Finally got the value, %v\n", string(resultNR))

	time.Sleep(1 * time.Second)

	storage.Close()

	return nil
}

// generate a bunch of secrets
const secretCount = 2

func genSecrets(ctx context.Context, cmd *cli.Command) error {

	for i := range secretCount {
		secret, name := crypto.GenerateKeypair()
		peerkey, _, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			panic(err)
		}

		newSecret := node.Secret{
			Secret:  secret,
			Name:    name,
			PeerKey: node.SerializablePeerKey{Key: peerkey},
		}

		secretFileName := fmt.Sprintf("secret_%v.sc", i)

		node.WriteJSON(secretFileName, newSecret)
	}

	return nil
}

func bootstrapper(ctx context.Context, cmd *cli.Command) error {
	priv, _, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		panic(err)
	}

	host, err := network.NewRoutedHost(context.Background(), "/ip4/0.0.0.0/tcp/0", priv, nil)
	if err != nil {
		panic(err)
	}

	host.PrintAddrs()

	select {}
}

func testClient(ctx context.Context, cmd *cli.Command) error {
	priv, _, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return err
	}

	committee, err := node.ReadJSON[node.Committee]("testcommittee.cmt")
	if err != nil {
		return err
	}

	host, err := network.NewRoutedHost(context.Background(), "/ip4/0.0.0.0/tcp/0", priv, committee.BootstrapPeers)
	if err != nil {
		return err
	}
	ps, err := network.NewPubsub(context.Background(), host, "mempool")
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")

		if !scanner.Scan() {
			// Stop loop on EOF or error
			break
		}

		line := scanner.Text()

		fmt.Printf("sending text: %v\n", line)

		ps.Publish(context.Background(), []byte(line))
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func generateCommittee(ctx context.Context, cmd *cli.Command) error {
	bsNode := cmd.Args().Get(0)

	authorityInfos := make([]consensus.AuthorityInfo, 0, secretCount)

	for i := range secretCount {
		secretFileName := fmt.Sprintf("secret_%v.sc", i)

		ithSecret, err := node.ReadJSON[node.Secret](secretFileName)
		if err != nil {
			panic(err)
		}

		name := ithSecret.Name

		address, err := peer.IDFromPrivateKey(ithSecret.PeerKey.Key)
		if err != nil {
			panic(err)
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
	bootstrapNodes := []string{bsNode}

	newCommittee := node.Committee{
		Consensus:      consensusCommittee,
		Mempool:        mempoolCommitee,
		BootstrapPeers: bootstrapNodes,
	}

	node.WriteJSON("testcommittee.cmt", newCommittee)

	return nil
}

func testRun(ctx context.Context, cmd *cli.Command) error {
	secretNo := cmd.Args().Get(0)

	secretFileName := fmt.Sprintf("secret_%v.sc", secretNo)
	testStoreName := fmt.Sprintf("teststore_%v.sc", secretNo)

	node, err := node.NewNode("testcommittee.cmt", secretFileName, testStoreName, nil)
	if err != nil {
		panic(err)
	}

	fmt.Println(node)

	node.ProcessBlocks()

	return nil
}

func fullTest(ctx context.Context, cmd *cli.Command) error {
	for i := range secretCount {
		secretFileName := fmt.Sprintf("secret_%v.sc", i)
		testStoreName := fmt.Sprintf("teststore_%v.sc", i)

		node, err := node.NewNode("testcommittee.cmt", secretFileName, testStoreName, nil)
		if err != nil {
			panic(err)
		}

		fmt.Println(node)
	}

	for {
	}
}

func testCrypto(ctx context.Context, cmd *cli.Command) error {
	sk, pk := crypto.GenerateKeypair()
	skB64 := base64.StdEncoding.EncodeToString(sk[:])
	fmt.Printf("sk: %v, pk: %v\n", skB64, pk)

	sk2, pk2 := crypto.GenerateKeypair()
	skB642 := base64.StdEncoding.EncodeToString(sk2[:])
	fmt.Printf("sk2: %v, pk2: %v\n", skB642, pk2)

	signatureService := crypto.NewSignatureService(sk)
	//signatureService2 := crypto.NewSignatureService(sk2)

	msg := []byte("Hello, world!")
	d := crypto.NewDigest(msg)

	testBlock := consensus.NewBlock(consensus.QC{}, nil, pk, 1, []crypto.Digest{d}, signatureService)

	fmt.Printf("Block is : %v\n", testBlock)

	var as crypto.AggregateSignature

	as.Add(signatureService.RequestSignature(d))
	//as.Add(signatureService2.RequestSignature(d))

	finalSig := as.ToSignature()

	if finalSig.FastAggregateVerify(d, []crypto.PublicKey{pk}) {
		fmt.Println("Verified the block!")
	} else {
		fmt.Println("Failed to verify the block.")
	}

	return nil
}
