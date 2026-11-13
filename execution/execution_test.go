package execution

import (
	"testing"

	"github.com/jamesh000/hotstuff-2chain/crypto"
)

type memState map[Key][]byte

func (m memState) Get(key Key) ([]byte, error) {
	return m[key], nil
}

func TestSet(t *testing.T) {
	a := crypto.PublicKey{}
	a[0] = 1

	writes, err := TestExecutor{}.Execute(
		[][]byte{NewSetTransaction(a, 42).Serialize()},
		memState{},
	)
	if err != nil {
		t.Fatal(err)
	}

	value, ok := writes[AccountKey(a)]
	if !ok {
		t.Fatal("no write for account")
	}
	if got, _ := DecodeValue(value); got != 42 {
		t.Fatalf("value %d, want 42", got)
	}
}

func TestIncrement(t *testing.T) {
	a := crypto.PublicKey{}
	a[0] = 1

	state := memState{AccountKey(a): EncodeValue(10)}

	// absent account starts at 1
	b := crypto.PublicKey{}
	b[0] = 2
	writes, err := TestExecutor{}.Execute(
		[][]byte{
			NewIncrementTransaction(a).Serialize(),
			NewIncrementTransaction(a).Serialize(),
			NewIncrementTransaction(b).Serialize(),
		},
		state,
	)
	if err != nil {
		t.Fatal(err)
	}

	if got, _ := DecodeValue(writes[AccountKey(a)]); got != 12 {
		t.Fatalf("a = %d, want 12", got)
	}
	if got, _ := DecodeValue(writes[AccountKey(b)]); got != 1 {
		t.Fatalf("b = %d, want 1", got)
	}
}

func TestIncrementBadExistingValue(t *testing.T) {
	a := crypto.PublicKey{}
	a[0] = 1

	// a corrupt stored value is treated as absent (increment sets it to 1)
	writes, err := TestExecutor{}.Execute(
		[][]byte{NewIncrementTransaction(a).Serialize()},
		memState{AccountKey(a): []byte{0x01}},
	)
	if err != nil {
		t.Fatal(err)
	}

	if got, _ := DecodeValue(writes[AccountKey(a)]); got != 1 {
		t.Fatalf("a = %d, want 1", got)
	}
}

func TestInvalidTransactionsAreSkipped(t *testing.T) {
	a := crypto.PublicKey{}
	a[0] = 1

	writes, err := TestExecutor{}.Execute(
		[][]byte{
			nil,
			[]byte{0xFF},
			NewSetTransaction(a, 7).Serialize(),
		},
		memState{},
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(writes) != 1 {
		t.Fatalf("writes = %d, want 1", len(writes))
	}
	if got, _ := DecodeValue(writes[AccountKey(a)]); got != 7 {
		t.Fatalf("a = %d, want 7", got)
	}
}

func TestRoundTrip(t *testing.T) {
	a := crypto.PublicKey{}
	a[1] = 0xAB

	for _, tx := range []Transaction{
		NewSetTransaction(a, 123456789),
		NewIncrementTransaction(a),
	} {
		parsed, err := DeserializeTransaction(tx.Serialize())
		if err != nil {
			t.Fatal(err)
		}
		if parsed.Op != tx.Op || parsed.Account != tx.Account || parsed.Value != tx.Value {
			t.Fatalf("round trip mismatch: %v != %v", parsed, tx)
		}
	}
}
