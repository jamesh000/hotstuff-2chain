package execution

import (
	"encoding/binary"
	"fmt"
	"log"

	jmt "github.com/jamesh000/SimpleJMT"
	"github.com/jamesh000/hotstuff-2chain/crypto"
)

// Testing transaction format. Transactions are unsigned for now, so anyone can
// write to any account.
//
//	op (1 byte) | account public key (48 bytes) | value (8 bytes, big endian, OpSet only)
const (
	// OpSet sets the account to the given value.
	OpSet byte = 0x00
	// OpIncrement reads the account and increments it by one, or sets it to 1
	// if the account has no value.
	OpIncrement byte = 0x01
)

const (
	setTxLen       = 1 + crypto.PUBLICKEY_LEN + 8
	incrementTxLen = 1 + crypto.PUBLICKEY_LEN
	valueLen       = 8
)

type Transaction struct {
	Op      byte
	Account crypto.PublicKey
	Value   uint64
}

func NewSetTransaction(account crypto.PublicKey, value uint64) Transaction {
	return Transaction{Op: OpSet, Account: account, Value: value}
}

func NewIncrementTransaction(account crypto.PublicKey) Transaction {
	return Transaction{Op: OpIncrement, Account: account}
}

func (tx Transaction) Serialize() []byte {
	switch tx.Op {
	case OpSet:
		data := make([]byte, setTxLen)
		data[0] = OpSet
		copy(data[1:], tx.Account[:])
		binary.BigEndian.PutUint64(data[1+crypto.PUBLICKEY_LEN:], tx.Value)
		return data
	case OpIncrement:
		data := make([]byte, incrementTxLen)
		data[0] = OpIncrement
		copy(data[1:], tx.Account[:])
		return data
	default:
		panic(fmt.Errorf("unknown transaction op %d", tx.Op))
	}
}

func DeserializeTransaction(data []byte) (Transaction, error) {
	if len(data) == 0 {
		return Transaction{}, fmt.Errorf("empty transaction")
	}

	var tx Transaction
	tx.Op = data[0]

	switch tx.Op {
	case OpSet:
		if len(data) != setTxLen {
			return Transaction{}, fmt.Errorf("set transaction has length %d, want %d", len(data), setTxLen)
		}
		tx.Value = binary.BigEndian.Uint64(data[1+crypto.PUBLICKEY_LEN:])
	case OpIncrement:
		if len(data) != incrementTxLen {
			return Transaction{}, fmt.Errorf("increment transaction has length %d, want %d", len(data), incrementTxLen)
		}
	default:
		return Transaction{}, fmt.Errorf("unknown transaction op %d", tx.Op)
	}

	copy(tx.Account[:], data[1:1+crypto.PUBLICKEY_LEN])
	return tx, nil
}

func (tx Transaction) String() string {
	switch tx.Op {
	case OpSet:
		return fmt.Sprintf("Set(%v = %d)", tx.Account, tx.Value)
	case OpIncrement:
		return fmt.Sprintf("Increment(%v)", tx.Account)
	default:
		return fmt.Sprintf("Unknown(op %d)", tx.Op)
	}
}

// AccountKey returns the state key of an account.
func AccountKey(account crypto.PublicKey) Key {
	return jmt.NewHash(account[:])
}

func EncodeValue(value uint64) []byte {
	data := make([]byte, valueLen)
	binary.BigEndian.PutUint64(data, value)
	return data
}

func DecodeValue(data []byte) (uint64, error) {
	if len(data) != valueLen {
		return 0, fmt.Errorf("account value has length %d, want %d", len(data), valueLen)
	}
	return binary.BigEndian.Uint64(data), nil
}

// TestExecutor executes the testing transaction format above.
type TestExecutor struct{}

func (TestExecutor) Execute(txs [][]byte, state StateReader) (WriteSet, error) {
	writes := make(WriteSet)

	for _, raw := range txs {
		tx, err := DeserializeTransaction(raw)
		if err != nil {
			log.Printf("Skipping invalid transaction: %v\n", err)
			continue
		}

		log.Printf("Executing transaction: %v\n", tx)

		key := AccountKey(tx.Account)

		switch tx.Op {
		case OpSet:
			writes[key] = EncodeValue(tx.Value)
		case OpIncrement:
			// Earlier writes in this block take precedence over the parent state
			current, ok := writes[key]
			if !ok {
				current, err = state.Get(key)
				if err != nil {
					return nil, err
				}
			}

			next := uint64(1)
			if current != nil {
				value, err := DecodeValue(current)
				if err != nil {
					log.Printf("Account %v holds an invalid value, resetting it: %v\n", tx.Account, err)
				} else {
					next = value + 1
				}
			}

			writes[key] = EncodeValue(next)
		}
	}

	return writes, nil
}
