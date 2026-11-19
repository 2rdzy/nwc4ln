// Package policy holds the relay's write and read rules (spec section 16). The
// rules are plain functions of the current allow-list, so they are tested
// without a relay. Everything they do not allow is refused.
package policy

import (
	"fiatjaf.com/nostr"

	"github.com/2rdzy/nwc4ln/relay/internal/allowlist"
)

// The NWC event kinds (NIP-47).
const (
	KindInfo         nostr.Kind = 13194
	KindRequest      nostr.Kind = 23194
	KindResponse     nostr.Kind = 23195
	KindNotification nostr.Kind = 23196 // NIP-04 notifications.
	KindNotifyNIP44  nostr.Kind = 23197
)

// Messages for refusals. Each starts with a NIP-01 machine-readable prefix.
const (
	NoList      = "restricted: this relay has no allow-list yet"
	NotAccepted = "restricted: this relay accepts only listed NWC events"
	WrongPair   = "restricted: a request must name its own connection's service key"
	BadFilter   = "restricted: a filter must ask for NWC kinds by listed authors"
)

// Write decides whether to accept an event that is not an allow-list. The
// allow-list itself is checked by allowlist.Store.
func Write(list *allowlist.List, event nostr.Event) (reject bool, msg string) {
	if list == nil {
		return true, NoList
	}
	switch event.Kind {
	case KindInfo, KindResponse, KindNotification, KindNotifyNIP44:
		if list.IsService(event.PubKey) {
			return false, ""
		}
		return true, NotAccepted
	case KindRequest:
		service, ok := list.ServiceOf(event.PubKey)
		if !ok {
			return true, NotAccepted
		}
		// Exactly one p tag, naming this client's own service key.
		count, match := 0, false
		for _, tag := range event.Tags {
			if len(tag) >= 1 && tag[0] == "p" {
				count++
				match = len(tag) >= 2 && tag[1] == service
			}
		}
		if count != 1 || !match {
			return true, WrongPair
		}
		return false, ""
	default:
		return true, NotAccepted
	}
}

// Read decides whether to serve a filter. A filter must ask only for NWC kinds
// and name authors, all of them listed service keys. The one exception is the
// wallet's own filter: requests by #p, every key named a listed service key. So
// a stranger cannot list info events or learn a service key, and the allow-list
// is never served.
func Read(list *allowlist.List, filter nostr.Filter) (reject bool, msg string) {
	if list == nil {
		return true, NoList
	}
	if len(filter.Kinds) == 0 || filter.Search != "" {
		return true, BadFilter
	}
	for _, kind := range filter.Kinds {
		switch kind {
		case KindInfo, KindRequest, KindResponse, KindNotification, KindNotifyNIP44:
		default:
			return true, BadFilter
		}
	}
	if len(filter.Authors) > 0 {
		for _, author := range filter.Authors {
			if !list.IsService(author) {
				return true, BadFilter
			}
		}
		return false, ""
	}
	// The wallet's filter: requests only, by #p only.
	if len(filter.Kinds) != 1 || filter.Kinds[0] != KindRequest || len(filter.IDs) > 0 || len(filter.Tags) != 1 {
		return true, BadFilter
	}
	keys, ok := filter.Tags["p"]
	if !ok || len(keys) == 0 {
		return true, BadFilter
	}
	for _, key := range keys {
		if !list.IsServiceHex(key) {
			return true, BadFilter
		}
	}
	return false, ""
}
