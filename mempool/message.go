package mempool

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/jamesh000/hotstuff-2chain/crypto"
	pb "github.com/jamesh000/hotstuff-2chain/mempoolpb"
)

type mempoolMessage interface {
	serializeMempoolMessage() ([]byte, error)
}

type batchMessage struct {
	batch []byte
}

func (msg batchMessage) serializeMempoolMessage() ([]byte, error) {
	protoBatchMessage := &pb.MempoolMessage{
		Msg: &pb.MempoolMessage_BatchMessage{
			BatchMessage: &pb.MempoolMessage_Batch{
				Batch: msg.batch,
			},
		},
	}

	data, err := proto.Marshal(protoBatchMessage)
	if err != nil {
		return nil, err
	}

	return data, nil
}

type requestMessage struct {
	missing []crypto.Digest
	origin  crypto.PublicKey
}

func (msg requestMessage) serializeMempoolMessage() ([]byte, error) {
	missingData := make([][]byte, len(msg.missing))
	for i, digest := range msg.missing {
		missingData[i] = digest[:]
	}

	protoRequestMessage := &pb.MempoolMessage{
		Msg: &pb.MempoolMessage_ReqMessage{
			ReqMessage: &pb.MempoolMessage_Request{
				Missing: missingData,
				Origin:  msg.origin[:],
			},
		},
	}

	data, err := proto.Marshal(protoRequestMessage)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func DeserializeMempoolMessage(data []byte) (mempoolMessage, error) {
	pmsg := &pb.MempoolMessage{}
	err := proto.Unmarshal(data, pmsg)
	if err != nil {
		return nil, err
	}

	switch msg := pmsg.Msg.(type) {
	case *pb.MempoolMessage_BatchMessage:
		newBatchMessage := &batchMessage{
			batch: msg.BatchMessage.Batch,
		}

		return newBatchMessage, nil
	case *pb.MempoolMessage_ReqMessage:
		missingDigests := make([]crypto.Digest, len(msg.ReqMessage.Missing))
		for i, digestBytes := range msg.ReqMessage.Missing {
			d, err := new(crypto.Digest).FromBytes(digestBytes)
			if err != nil {
				return nil, err
			}

			missingDigests[i] = *d
		}

		newReqMessage := &requestMessage{
			missing: missingDigests,
			origin:  crypto.PublicKey(msg.ReqMessage.Origin),
		}

		return newReqMessage, nil
	default:
		return nil, fmt.Errorf("Erroneous mempool message type")
	}
}
