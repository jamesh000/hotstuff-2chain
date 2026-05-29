package consensus

import (
	"fmt"

	pb "github.com/jamesh000/hotstuff-2chain/consensuspb"
	"github.com/jamesh000/hotstuff-2chain/crypto"
	"google.golang.org/protobuf/proto"
)

type consensusMessage interface {
	SerializeConsensusMessage() ([]byte, error)
}

type testMessage struct {
	message string
}

func (msg testMessage) SerializeConsensusMessage() ([]byte, error) {
	testmsg := &pb.ConsensusMessage{
		Message: &pb.ConsensusMessage_Testfield{
			Testfield: &pb.ConsensusMessage_TestMessage{
				Messagetext: msg.message,
			},
		},
	}

	data, err := proto.Marshal(testmsg)
	if err != nil {
		return nil, err
	}

	return data, nil
}

type proposeMessage struct {
	block Block
}

func (msg proposeMessage) SerializeConsensusMessage() ([]byte, error) {
	proposeMessage := &pb.ConsensusMessage{
		Message: &pb.ConsensusMessage_Proposal{
			Proposal: &pb.ConsensusMessage_ProposeMessage{
				ProposedBlock: msg.block.toProto(),
			},
		},
	}

	data, err := proto.Marshal(proposeMessage)
	if err != nil {
		return nil, err
	}

	return data, nil
}

type voteMessage struct {
	vote vote
}

func (msg voteMessage) SerializeConsensusMessage() ([]byte, error) {
	voteMessage := &pb.ConsensusMessage{
		Message: &pb.ConsensusMessage_VMessage{
			VMessage: &pb.ConsensusMessage_VoteMessage{
				V: msg.vote.toProto(),
			},
		},
	}

	data, err := proto.Marshal(voteMessage)
	if err != nil {
		return nil, err
	}

	return data, nil
}

type timeoutMessage struct {
	timeout timeout
}

func (msg timeoutMessage) SerializeConsensusMessage() ([]byte, error) {
	timeoutMessage := &pb.ConsensusMessage{
		Message: &pb.ConsensusMessage_TMessage{
			TMessage: &pb.ConsensusMessage_TimeoutMessage{
				T: msg.timeout.toProto(),
			},
		},
	}

	data, err := proto.Marshal(timeoutMessage)
	if err != nil {
		return nil, err
	}

	return data, nil
}

type tcMessage struct {
	tc TC
}

func (msg tcMessage) SerializeConsensusMessage() ([]byte, error) {
	tcMessage := &pb.ConsensusMessage{
		Message: &pb.ConsensusMessage_TcMessage{
			TcMessage: &pb.ConsensusMessage_TCMessage{
				TimeoutCert: msg.tc.toProto(),
			},
		},
	}

	data, err := proto.Marshal(tcMessage)
	if err != nil {
		return nil, err
	}

	return data, nil
}

type syncRequestMessage struct {
	missing crypto.Digest
	origin  crypto.PublicKey
}

func (msg syncRequestMessage) SerializeConsensusMessage() ([]byte, error) {
	timeoutMessage := &pb.ConsensusMessage{
		Message: &pb.ConsensusMessage_SrMessage{
			SrMessage: &pb.ConsensusMessage_SyncRequestMessage{
				Digest:    msg.missing[:],
				Publickey: msg.origin[:],
			},
		},
	}

	data, err := proto.Marshal(timeoutMessage)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func DeserializeConsensusMessage(data []byte) (consensusMessage, error) {
	pmsg := &pb.ConsensusMessage{}
	err := proto.Unmarshal(data, pmsg)
	if err != nil {
		return nil, err
	}

	switch msg := pmsg.Message.(type) {
	case *pb.ConsensusMessage_Testfield:
		newTestMsg := &testMessage{
			message: msg.Testfield.Messagetext,
		}

		return newTestMsg, nil
	case *pb.ConsensusMessage_Proposal:
		proposedBlock, err := blockFromProto(msg.Proposal.ProposedBlock)
		if err != nil {
			return nil, err
		}

		return &proposeMessage{*proposedBlock}, nil
	case *pb.ConsensusMessage_VMessage:
		vote, err := voteFromProto(msg.VMessage.V)
		if err != nil {
			return nil, err
		}

		return &voteMessage{*vote}, nil
	case *pb.ConsensusMessage_TMessage:
		timeout, err := timeoutFromProto(msg.TMessage.T)
		if err != nil {
			return nil, err
		}

		return &timeoutMessage{*timeout}, nil
	case *pb.ConsensusMessage_TcMessage:
		tc, err := tcFromProto(msg.TcMessage.TimeoutCert)
		if err != nil {
			return nil, err
		}

		return &tcMessage{*tc}, nil
	case *pb.ConsensusMessage_SrMessage:
		missing, err := new(crypto.Digest).FromBytes(msg.SrMessage.Digest)
		if err != nil {
			return nil, err
		}

		origin, err := new(crypto.PublicKey).FromBytes(msg.SrMessage.Publickey)
		if err != nil {
			return nil, err
		}

		return &syncRequestMessage{*missing, *origin}, nil
	default:
		return nil, fmt.Errorf("Erroneous consensus message type")
	}

}
