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
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
