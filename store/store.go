package store

import (
	"github.com/cockroachdb/pebble"
)

const CHANNEL_CAPACITY = 100

type Key = []uint8
type Value = []uint8

type commandType uint

const (
	writeCommand commandType = iota
	readCommand
	notifyReadCommand
	closeCommand
)

type storeResult struct {
	value *Value
	err   error
}

type command struct {
	t        commandType
	key      Key
	value    Value
	response chan storeResult
}

type Store struct {
	channel chan<- command
}

func NewStore(path string) (*Store, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	obligations := make(map[string][]chan storeResult)

	channel := make(chan command, CHANNEL_CAPACITY)

	go func() {
		for cmd := range channel {
			switch cmd.t {
			case writeCommand:
				db.Set(cmd.key, cmd.value, pebble.Sync)
				if senders, ok := obligations[string(cmd.key)]; ok {
					for _, s := range senders {
						value := make(Value, len(cmd.value))
						copy(value, cmd.value)
						s <- storeResult{&value, nil}
					}
					delete(obligations, string(cmd.key))
				}

			case readCommand:
				val, closer, err := db.Get(cmd.key)
				if err != nil {
					cmd.response <- storeResult{nil, err}
					continue
				}
				defer closer.Close()

				responseVal := make(Value, len(val))
				copy(responseVal, val)

				cmd.response <- storeResult{&responseVal, nil}

			case notifyReadCommand:
				val, closer, err := db.Get(cmd.key)
				if err == pebble.ErrNotFound {
					if _, ok := obligations[string(cmd.key)]; !ok {
						obligations[string(cmd.key)] = make([]chan storeResult, 0, 10)
					}
					obligations[string(cmd.key)] = append(obligations[string(cmd.key)], cmd.response)
					continue
				}

				if err != nil {
					cmd.response <- storeResult{nil, err}
					continue
				}
				defer closer.Close()

				responseVal := make(Value, len(val))
				copy(responseVal, val)

				cmd.response <- storeResult{&responseVal, nil}

			case closeCommand:
				db.Close()
				return
			}
		}
	}()

	return &Store{
			channel: channel,
		},
		nil
}

func (s Store) Write(key Key, value Value) {
	s.channel <- command{writeCommand, key, value, nil}
}

func (s Store) Read(key Key) (*Value, error) {
	responseCh := make(chan storeResult, 1)
	s.channel <- command{readCommand, key, nil, responseCh}

	response := <-responseCh

	return response.value, response.err
}

func (s Store) NotifyRead(key Key) (Value, error) {
	responseCh := make(chan storeResult, 1)
	s.channel <- command{notifyReadCommand, key, nil, responseCh}

	response := <-responseCh

	return *response.value, response.err
}

func (s Store) Close() {
	s.channel <- command{closeCommand, nil, nil, nil}
}
