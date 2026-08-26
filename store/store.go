package store

import (
	"errors"

	"github.com/cockroachdb/pebble"
)

const CHANNEL_CAPACITY = 100

type Key = []uint8
type Value = []uint8

var ErrNotFound = errors.New("value not found in store")

type commandType uint

const (
	writeCommand commandType = iota
	readCommand
	notifyReadCommand
	closeCommand
)

type StoreResult struct {
	Value *Value
	Err   error
}

type command struct {
	t        commandType
	key      Key
	value    Value
	response chan StoreResult
}

type Store struct {
	channel chan<- command
}

func NewStore(path string) (*Store, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	obligations := make(map[string][]chan StoreResult)

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
						s <- StoreResult{&value, nil}
					}
					delete(obligations, string(cmd.key))
				}

			case readCommand:
				val, closer, err := db.Get(cmd.key)
				if err != nil {
					if err == pebble.ErrNotFound {
						err = ErrNotFound
					}
					cmd.response <- StoreResult{nil, err}
					continue
				}
				defer closer.Close()

				responseVal := make(Value, len(val))
				copy(responseVal, val)

				cmd.response <- StoreResult{&responseVal, nil}

			case notifyReadCommand:
				val, closer, err := db.Get(cmd.key)
				if err == pebble.ErrNotFound {
					if _, ok := obligations[string(cmd.key)]; !ok {
						obligations[string(cmd.key)] = make([]chan StoreResult, 0, 10)
					}
					obligations[string(cmd.key)] = append(obligations[string(cmd.key)], cmd.response)
					continue
				}

				if err != nil {
					cmd.response <- StoreResult{nil, err}
					continue
				}
				defer closer.Close()

				responseVal := make(Value, len(val))
				copy(responseVal, val)

				cmd.response <- StoreResult{&responseVal, nil}

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
	responseCh := make(chan StoreResult, 1)
	s.channel <- command{readCommand, key, nil, responseCh}

	response := <-responseCh

	return response.Value, response.Err
}

func (s Store) NotifyRead(key Key) (Value, error) {
	responseCh := make(chan StoreResult, 1)
	s.channel <- command{notifyReadCommand, key, nil, responseCh}

	response := <-responseCh

	return *response.Value, response.Err
}

func (s Store) NotifyReadChannel(key Key) <-chan StoreResult {
	responseCh := make(chan StoreResult, 1)
	s.channel <- command{notifyReadCommand, key, nil, responseCh}

	return responseCh
}

func (s Store) Close() {
	s.channel <- command{closeCommand, nil, nil, nil}
}
