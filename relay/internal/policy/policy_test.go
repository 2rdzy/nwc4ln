package policy

import (
	"testing"

	"fiatjaf.com/nostr"

	"github.com/2rdzy/nwc4ln/relay/internal/allowlist"
)

type keys struct {
	secret nostr.SecretKey
	hex    string
	pub    nostr.PubKey
}

func newKeys() keys {
	secret := nostr.Generate()
	return keys{secret: secret, pub: secret.Public(), hex: secret.Public().Hex()}
}

func listOf(t *testing.T, control nostr.SecretKey, pairs ...keys) *allowlist.List {
	t.Helper()
	event := nostr.Event{Kind: allowlist.Kind, CreatedAt: nostr.Now()}
	for i := 0; i+1 < len(pairs); i += 2 {
		event.Tags = append(event.Tags, nostr.Tag{"pair", pairs[i].hex, pairs[i+1].hex})
	}
	if err := event.Sign(control); err != nil {
		t.Fatal(err)
	}
	list, err := allowlist.Parse(event, control.Public())
	if err != nil {
		t.Fatal(err)
	}
	return list
}

func event(t *testing.T, by keys, kind nostr.Kind, tags nostr.Tags) nostr.Event {
	t.Helper()
	e := nostr.Event{Kind: kind, CreatedAt: nostr.Now(), Tags: tags}
	if err := e.Sign(by.secret); err != nil {
		t.Fatal(err)
	}
	return e
}

func TestWrite(t *testing.T) {
	control, clientA, serviceA, clientB, serviceB, stranger := newKeys(), newKeys(), newKeys(), newKeys(), newKeys(), newKeys()
	list := listOf(t, control.secret, clientA, serviceA, clientB, serviceB)
	p := func(k keys) nostr.Tags { return nostr.Tags{{"p", k.hex}} }
	cases := []struct {
		name   string
		list   *allowlist.List
		event  nostr.Event
		accept bool
	}{
		{"request to own service key", list, event(t, clientA, KindRequest, p(serviceA)), true},
		{"request to another pair's service key", list, event(t, clientA, KindRequest, p(serviceB)), false},
		{"request with two p tags", list, event(t, clientA, KindRequest, nostr.Tags{{"p", serviceA.hex}, {"p", serviceB.hex}}), false},
		{"request with no p tag", list, event(t, clientA, KindRequest, nil), false},
		{"request from a stranger", list, event(t, stranger, KindRequest, p(serviceA)), false},
		{"request signed by a service key", list, event(t, serviceA, KindRequest, p(serviceA)), false},
		{"info from a service key", list, event(t, serviceA, KindInfo, nil), true},
		{"response from a service key", list, event(t, serviceA, KindResponse, p(clientA)), true},
		{"notification from a service key", list, event(t, serviceB, KindNotification, p(clientB)), true},
		{"NIP-44 notification from a service key", list, event(t, serviceB, KindNotifyNIP44, p(clientB)), true},
		{"response from a client key", list, event(t, clientA, KindResponse, p(clientA)), false},
		{"info from a stranger", list, event(t, stranger, KindInfo, nil), false},
		{"info from the control key", list, event(t, control, KindInfo, nil), false},
		{"text note from a service key", list, event(t, serviceA, 1, nil), false},
		{"deletion from a service key", list, event(t, serviceA, 5, nil), false},
		{"addressable from a service key", list, event(t, serviceA, 30078, nil), false},
		{"other ephemeral from a service key", list, event(t, serviceA, 20001, nil), false},
		{"request before the first list", nil, event(t, clientA, KindRequest, p(serviceA)), false},
		{"info before the first list", nil, event(t, serviceA, KindInfo, nil), false},
	}
	for _, c := range cases {
		reject, msg := Write(c.list, c.event)
		if reject == c.accept {
			t.Errorf("%s: reject %v (%s)", c.name, reject, msg)
		}
	}
	// A stranger's request is refused as a stranger, not as a wrong pair.
	if _, msg := Write(list, event(t, stranger, KindRequest, p(serviceA))); msg != NotAccepted {
		t.Errorf("stranger's request: %q", msg)
	}
}

func TestRead(t *testing.T) {
	control, clientA, serviceA, clientB, serviceB, stranger := newKeys(), newKeys(), newKeys(), newKeys(), newKeys(), newKeys()
	list := listOf(t, control.secret, clientA, serviceA, clientB, serviceB)
	nwc := []nostr.Kind{KindInfo, KindResponse, KindNotification, KindNotifyNIP44}
	cases := []struct {
		name   string
		list   *allowlist.List
		filter nostr.Filter
		accept bool
	}{
		{"client: replies by its service key", list, nostr.Filter{Kinds: nwc, Authors: []nostr.PubKey{serviceA.pub}, Tags: nostr.TagMap{"p": {clientA.hex}}}, true},
		{"client: info by two service keys", list, nostr.Filter{Kinds: []nostr.Kind{KindInfo}, Authors: []nostr.PubKey{serviceA.pub, serviceB.pub}}, true},
		{"wallet: requests by #p", list, nostr.Filter{Kinds: []nostr.Kind{KindRequest}, Tags: nostr.TagMap{"p": {serviceA.hex, serviceB.hex}}, Since: 1}, true},
		{"no authors", list, nostr.Filter{Kinds: nwc}, false},
		{"no kinds", list, nostr.Filter{Authors: []nostr.PubKey{serviceA.pub}}, false},
		{"an unlisted author", list, nostr.Filter{Kinds: nwc, Authors: []nostr.PubKey{serviceA.pub, stranger.pub}}, false},
		{"a client key as author", list, nostr.Filter{Kinds: nwc, Authors: []nostr.PubKey{clientA.pub}}, false},
		{"the allow-list kind", list, nostr.Filter{Kinds: []nostr.Kind{allowlist.Kind}, Authors: []nostr.PubKey{serviceA.pub}}, false},
		{"the allow-list kind by the control key", list, nostr.Filter{Kinds: []nostr.Kind{allowlist.Kind}, Authors: []nostr.PubKey{control.pub}}, false},
		{"text notes", list, nostr.Filter{Kinds: []nostr.Kind{1}, Authors: []nostr.PubKey{serviceA.pub}}, false},
		{"search", list, nostr.Filter{Kinds: nwc, Authors: []nostr.PubKey{serviceA.pub}, Search: "x"}, false},
		{"#p with an unlisted key", list, nostr.Filter{Kinds: []nostr.Kind{KindRequest}, Tags: nostr.TagMap{"p": {serviceA.hex, stranger.hex}}}, false},
		{"#p with a client key", list, nostr.Filter{Kinds: []nostr.Kind{KindRequest}, Tags: nostr.TagMap{"p": {clientA.hex}}}, false},
		{"#p for responses", list, nostr.Filter{Kinds: []nostr.Kind{KindResponse}, Tags: nostr.TagMap{"p": {serviceA.hex}}}, false},
		{"#p for info events", list, nostr.Filter{Kinds: []nostr.Kind{KindRequest, KindInfo}, Tags: nostr.TagMap{"p": {serviceA.hex}}}, false},
		{"#p with another tag", list, nostr.Filter{Kinds: []nostr.Kind{KindRequest}, Tags: nostr.TagMap{"p": {serviceA.hex}, "e": {"x"}}}, false},
		{"#p empty", list, nostr.Filter{Kinds: []nostr.Kind{KindRequest}, Tags: nostr.TagMap{"p": {}}}, false},
		{"#p in uppercase", list, nostr.Filter{Kinds: []nostr.Kind{KindRequest}, Tags: nostr.TagMap{"p": {upper(serviceA.hex)}}}, false},
		{"before the first list", nil, nostr.Filter{Kinds: nwc, Authors: []nostr.PubKey{serviceA.pub}}, false},
	}
	for _, c := range cases {
		reject, msg := Read(c.list, c.filter)
		if reject == c.accept {
			t.Errorf("%s: reject %v (%s)", c.name, reject, msg)
		}
	}
}

func upper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'f' {
			b[i] = c - 'a' + 'A'
		}
	}
	return string(b)
}
