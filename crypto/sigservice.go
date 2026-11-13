package crypto

type signatureRequest struct {
	msg      Digest
	response chan Signature
}

type SignatureService struct {
	reqChannel chan signatureRequest
}

func NewSignatureService(sk SecretKey) SignatureService {
	var ss SignatureService
	ss.reqChannel = make(chan signatureRequest)

	go func() {
		for req := range ss.reqChannel {
			sig := Sign(req.msg, sk)
			req.response <- sig
			close(req.response)
		}
	}()

	return ss
}

func (ss SignatureService) RequestSignature(msg Digest) Signature {
	responseCh := make(chan Signature)
	ss.reqChannel <- signatureRequest{msg, responseCh}

	return <-responseCh
}
