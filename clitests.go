package main

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/jamesh000/hotstuff-2chain/store"
	"github.com/urfave/cli/v3"
)

// Used to check the store is working properly
func storeTest(ctx context.Context, cmd *cli.Command) error {
	storage := store.NewStore("storefile")

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
