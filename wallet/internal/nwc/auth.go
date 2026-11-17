package nwc

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/getAlby/go-nostr"
	"github.com/getAlby/go-nostr/nip19"
)

// SignedEvent is a Nostr event whose ID and signature have been checked. It lets
// code outside this package, such as the admin login, read a signed event without
// using go-nostr.
type SignedEvent struct {
	PubKey    string // Hex.
	Kind      int
	CreatedAt time.Time
	Tags      [][]string
	Content   string
}

// TagValues returns the first value of each tag with the name.
func (e SignedEvent) TagValues(name string) []string {
	var values []string
	for _, tag := range e.Tags {
		if len(tag) >= 2 && tag[0] == name {
			values = append(values, tag[1])
		}
	}
	return values
}

// VerifyEvent reads an event in JSON, and checks its ID and its signature. Both
// are needed: the signature covers the ID's hash of the content, and the check of
// the signature alone does not compare that hash with the fields.
func VerifyEvent(raw []byte) (SignedEvent, error) {
	var event nostr.Event
	if err := json.Unmarshal(raw, &event); err != nil {
		return SignedEvent{}, fmt.Errorf("read event: %w", err)
	}
	if !event.CheckID() {
		return SignedEvent{}, errors.New("event id does not match its content")
	}
	if valid, err := event.CheckSignature(); err != nil || !valid {
		return SignedEvent{}, errors.New("bad event signature")
	}
	tags := make([][]string, len(event.Tags))
	for i, tag := range event.Tags {
		tags[i] = []string(tag)
	}
	return SignedEvent{
		PubKey: event.PubKey, Kind: event.Kind, CreatedAt: event.CreatedAt.Time(), Tags: tags, Content: event.Content,
	}, nil
}

// DecodeNpub returns the hex public key in an npub.
func DecodeNpub(npub string) (string, error) {
	prefix, value, err := nip19.Decode(npub)
	if err != nil {
		return "", fmt.Errorf("read npub: %w", err)
	}
	public, ok := value.(string)
	if prefix != "npub" || !ok || len(public) != 64 {
		return "", fmt.Errorf("not an npub: %q", npub)
	}
	return public, nil
}

// EncodeNpub returns the npub of a hex public key.
func EncodeNpub(public string) (string, error) {
	return nip19.EncodePublicKey(public)
}
