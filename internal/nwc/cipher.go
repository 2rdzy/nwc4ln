package nwc

import (
	"errors"
	"fmt"

	"github.com/getAlby/go-nostr/nip04"
	"github.com/getAlby/go-nostr/nip44"
)

// Encryption schemes, as named in the encryption tag.
const (
	schemeNIP44 = "nip44_v2"
	schemeNIP04 = "nip04"
	// encryptionTag lists what we accept, preferred first.
	encryptionTag = schemeNIP44 + " " + schemeNIP04
)

var errUnsupportedEncryption = errors.New("unsupported encryption")

// cipher encrypts between one service key and one client.
type cipher struct {
	scheme   string
	nip44Key [32]byte
	nip04Key []byte
}

func newCipher(scheme, clientPub, serviceKey string) (cipher, error) {
	c := cipher{scheme: scheme}
	var err error
	switch scheme {
	case schemeNIP44:
		c.nip44Key, err = nip44.GenerateConversationKey(clientPub, serviceKey)
	case schemeNIP04:
		c.nip04Key, err = nip04.ComputeSharedSecret(clientPub, serviceKey)
	default:
		return cipher{}, fmt.Errorf("%w: %q", errUnsupportedEncryption, scheme)
	}
	if err != nil {
		return cipher{}, fmt.Errorf("make %s key: %w", scheme, err)
	}
	return c, nil
}

func (c cipher) encrypt(plain string) (string, error) {
	if c.scheme == schemeNIP44 {
		return nip44.Encrypt(plain, c.nip44Key)
	}
	return nip04.Encrypt(plain, c.nip04Key)
}

func (c cipher) decrypt(content string) (string, error) {
	if c.scheme == schemeNIP44 {
		return nip44.Decrypt(content, c.nip44Key)
	}
	return nip04.Decrypt(content, c.nip04Key)
}
