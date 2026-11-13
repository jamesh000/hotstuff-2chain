// Package client provides a command line client that submits testing
// transactions to the mempool.
package client

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/jamesh000/hotstuff-2chain/crypto"
	"github.com/jamesh000/hotstuff-2chain/execution"
	"github.com/jamesh000/hotstuff-2chain/network"
	"github.com/jamesh000/hotstuff-2chain/node"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
)

// Run starts an interactive transaction client. A fresh BLS keypair is
// generated for every run, so several clients can be started side by side.
//
// Commands:
//
//	set <number>   write number to this client's account
//	inc            increment this client's account by one (set to 1 if absent)
//	quit           exit
func Run(committeeFile string) error {
	priv, _, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return err
	}

	committee, err := node.ReadJSON[node.Committee](committeeFile)
	if err != nil {
		return err
	}

	host, err := network.NewRoutedHost(context.Background(), "/ip4/0.0.0.0/tcp/0", priv, committee.BootstrapPeers)
	if err != nil {
		return err
	}

	sender := network.NewSimpleSender(*host, "client")

	_, account := crypto.GenerateKeypair()
	fmt.Printf("Using account %v\n", account)

	_, addresses := committee.Mempool.BroadcastAddresses(crypto.PublicKey{})

	send := func(tx execution.Transaction) error {
		data := tx.Serialize()
		sender.LuckyBroadcast(addresses, data, 1)
		log.Printf("Sent %v\n", tx)
		return nil
	}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")

		if !scanner.Scan() {
			break
		}

		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}

		switch fields[0] {
		case "set":
			if len(fields) != 2 {
				fmt.Println("usage: set <number>")
				continue
			}
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				fmt.Printf("invalid number: %v\n", fields[1])
				continue
			}
			if err := send(execution.NewSetTransaction(account, value)); err != nil {
				return err
			}
		case "inc":
			if err := send(execution.NewIncrementTransaction(account)); err != nil {
				return err
			}
		case "quit", "exit":
			return nil
		default:
			fmt.Println("commands: set <number>, inc, quit")
		}
	}

	return scanner.Err()
}
