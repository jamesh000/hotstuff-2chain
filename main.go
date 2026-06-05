package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Commands: []*cli.Command{
			{
				Name:  "test",
				Usage: "Create a random committee and run consensus",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					fmt.Println("Not implemented")
					return nil
				},
			},
			{
				Name:  "run",
				Usage: "Run a node",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					fmt.Println("Not implemented")
					return nil
				},
			},
			{
				Name:   "teststore",
				Usage:  "Test the store",
				Action: storeTest,
			},
			{
				Name:   "gensecrets",
				Usage:  "Create a set of secrets",
				Action: genSecrets,
			},
			{
				Name:   "bootstrapper",
				Usage:  "Run a bootstrapper node",
				Action: bootstrapper,
			},
			{
				Name:   "gencommittee",
				Usage:  "Generate a commitee using generated secrets",
				Action: generateCommittee,
			},
			{
				Name:   "testrun",
				Usage:  "Run a test node using generated secrets/committee",
				Action: testRun,
			},
			{
				Name:   "fulltest",
				Usage:  "Run multiple test nodes using generated secrets/committee",
				Action: fullTest,
			},
			{
				Name:   "testclient",
				Usage:  "Run a test client",
				Action: testClient,
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
