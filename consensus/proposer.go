package consensus

type ProposerMessage interface {
	proposerMessageMember()
}
