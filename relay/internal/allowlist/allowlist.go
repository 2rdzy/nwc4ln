// Package allowlist reads, checks and keeps the wallet's allow-list: the full
// list of connections the wallet serves, as pairs of a client key and its
// service key. Only the wallet's control key may sign it (spec section 16).
//
// The format, which the wallet writes the same way (WP17):
//
//	kind:    29194, an ephemeral kind no NIP uses, so a foreign relay does not keep it
//	pubkey:  the control key
//	content: empty
//	tags:    one ["pair", "<client key hex>", "<service key hex>"] per connection
//
// Keys are 64 lowercase hex digits. No key appears twice, the control key is in
// no pair, and any other tag is refused. An empty list is valid: the wallet
// serves no connection.
package allowlist

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"fiatjaf.com/nostr"
)

// Kind is the allow-list's event kind.
const Kind nostr.Kind = 29194

// The time window, by the relay's clock (WP16 STOP 1, owner, 2026-11-19). The
// future window is short, because the wallet stamps each list later than the last
// one and a list far ahead would lock out every later one. The past window covers
// a relay that lost its saved list: an old list cannot be replayed to it.
const (
	MaxFuture = time.Minute
	MaxAge    = 10 * time.Minute
)

// List is a checked allow-list.
type List struct {
	Event    nostr.Event
	clients  map[string]string   // Client key to its service key, both in hex.
	services map[string]struct{} // Service keys, in hex.
}

// Parse checks an event's kind, signer and form, then its id and signature, so
// that the cheap checks run first. Keys are checked as hex only: a key off the
// curve can never sign, so listing one does no harm. It does not check the time:
// see Store.Offer.
func Parse(event nostr.Event, control nostr.PubKey) (*List, error) {
	if event.Kind != Kind {
		return nil, fmt.Errorf("kind %d is not the allow-list kind %d", event.Kind, Kind)
	}
	if event.PubKey != control {
		return nil, errors.New("not signed by the control key")
	}
	if event.Content != "" {
		return nil, errors.New("content must be empty")
	}
	list := &List{
		Event:    event,
		clients:  make(map[string]string, len(event.Tags)),
		services: make(map[string]struct{}, len(event.Tags)),
	}
	controlHex := control.Hex()
	for i, tag := range event.Tags {
		if len(tag) != 3 || tag[0] != "pair" {
			return nil, fmt.Errorf("tag %d is not [\"pair\", client, service]", i)
		}
		client, service := tag[1], tag[2]
		for _, key := range []string{client, service} {
			if !isKeyHex(key) {
				return nil, fmt.Errorf("tag %d: %q is not 64 lowercase hex digits", i, key)
			}
			if key == controlHex {
				return nil, fmt.Errorf("tag %d names the control key", i)
			}
		}
		_, clientSeen := list.clients[client]
		_, clientIsService := list.services[client]
		_, serviceSeen := list.services[service]
		_, serviceIsClient := list.clients[service]
		if client == service || clientSeen || clientIsService || serviceSeen || serviceIsClient {
			return nil, fmt.Errorf("tag %d repeats a key", i)
		}
		list.clients[client] = service
		list.services[service] = struct{}{}
	}
	if !event.CheckID() {
		return nil, errors.New("id does not match the content")
	}
	if !event.VerifySignature() {
		return nil, errors.New("signature is invalid")
	}
	return list, nil
}

// Len is the number of pairs.
func (l *List) Len() int { return len(l.clients) }

// ServiceOf returns a listed client's service key in hex.
func (l *List) ServiceOf(client nostr.PubKey) (string, bool) {
	service, ok := l.clients[client.Hex()]
	return service, ok
}

// IsClient reports whether a key is a listed client key.
func (l *List) IsClient(key nostr.PubKey) bool {
	_, ok := l.clients[key.Hex()]
	return ok
}

// IsService reports whether a key is a listed service key.
func (l *List) IsService(key nostr.PubKey) bool {
	_, ok := l.services[key.Hex()]
	return ok
}

// IsServiceHex reports whether a hex string is a listed service key. Tag values
// are compared as text, as the relay matches them, so only lowercase hex counts.
func (l *List) IsServiceHex(key string) bool {
	_, ok := l.services[key]
	return ok
}

func isKeyHex(s string) bool {
	if len(s) != 64 {
		return false
	}
	return strings.IndexFunc(s, func(r rune) bool {
		return (r < '0' || r > '9') && (r < 'a' || r > 'f')
	}) == -1
}
