package crypto

func (pk PublicKey) MarshalText() ([]byte, error) {
	return []byte(pk.encodeBase64()), nil
}

func (pk *PublicKey) UnmarshalText(data []byte) error {
	err := pk.decodeBase64(string(data))
	if err != nil {
		return err
	}

	return nil
}

func (sk SecretKey) MarshalText() ([]byte, error) {
	return []byte(sk.encodeBase64()), nil
}

func (sk *SecretKey) UnmarshalText(data []byte) error {
	err := sk.decodeBase64(string(data))
	if err != nil {
		return err
	}

	return nil
}
