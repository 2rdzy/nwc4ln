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
)

// maxPlain is the most text NIP-44 can encrypt. Every reply and notification keeps
// to it, on NIP-04 too. It is a variable only so that a test can lower it: rows
// the ledger stores now always fit.
var maxPlain = 65_535

var (
	errUnsupportedEncryption = errors.New("unsupported encryption")
	// errReplyTooLarge marks a reply or notification over maxPlain.
	errReplyTooLarge = errors.New("too large to send")
	// errDecryptPanic marks content that made the library panic.
	errDecryptPanic = errors.New("decryption panicked")
)

// The library calls, held here so that a test can make one panic.
var (
	nip04Decrypt = nip04.Decrypt
	nip44Decrypt = nip44.Decrypt
)

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

// decrypt opens content a stranger can write. The library once panicked on a
// malformed NIP-04 envelope, and a panic here would stop the service, so one is
// turned into an error. The recover covers decryption only, never the command:
// a panic in a payment must not become an ordinary failure reply.
func (c cipher) decrypt(content string) (plain string, err error) {
	defer func() {
		if r := recover(); r != nil {
			plain, err = "", fmt.Errorf("%w: %v", errDecryptPanic, r)
		}
	}()
	if c.scheme == schemeNIP44 {
		return nip44Decrypt(content, c.nip44Key)
	}
	return nip04Decrypt(content, c.nip04Key)
}
