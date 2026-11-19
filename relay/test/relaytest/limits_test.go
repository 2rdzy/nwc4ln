//go:build relay

package relaytest

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"fiatjaf.com/nostr"
)

// The largest wallet reply fits; a message over the limit closes the connection.
func TestEventSize(t *testing.T) {
	s := setup(t, "")
	reply := sign(t, s.serviceA.secret, 23195, nostr.Now(), strings.Repeat("a", 87472), p(s.clientA))
	s.wallet.accept("88 KB reply", reply)
	c := s.r.connect()
	huge := sign(t, s.serviceA.secret, 23195, nostr.Now(), strings.Repeat("a", 128*1024), p(s.clientA))
	data, _ := huge.MarshalJSON()
	_ = c.sendRaw(`["EVENT",` + string(data) + `]`)
	if !c.isClosed(5 * time.Second) {
		t.Fatal("a message over 128 KiB did not close the connection")
	}
}

// A flood of events on one connection is rate-limited, and a connection that
// keeps sending refused events is closed.
func TestEventFlood(t *testing.T) {
	s := setup(t, "[limits]\nmessages_per_second = 1\nmessage_burst = 5\n")
	limited := 0
	for i := range 10 {
		ok, reason := s.wallet.publish(sign(t, s.serviceA.secret, 23195, nostr.Now(), fmt.Sprint(i), p(s.clientA)))
		if !ok && strings.HasPrefix(reason, "rate-limited") {
			limited++
		}
	}
	if limited == 0 {
		t.Fatal("10 events against a burst of 5 were all accepted")
	}

	stranger := newKeys()
	c := s.r.connect()
	for i := range 25 {
		if err := c.sendRaw(eventText(t, sign(t, stranger.secret, 1, nostr.Now(), fmt.Sprint(i), nil))); err != nil {
			break
		}
	}
	if !c.isClosed(5 * time.Second) {
		t.Fatal("25 refused events did not close the connection")
	}
}

func eventText(t *testing.T, e nostr.Event) string {
	data, err := e.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return `["EVENT",` + string(data) + `]`
}

// Subscriptions per connection are capped.
func TestSubscriptionFlood(t *testing.T) {
	s := setup(t, "")
	c := s.r.connect()
	filter := nostr.Filter{Kinds: []nostr.Kind{23195}, Authors: []nostr.PubKey{s.serviceA.pub}}
	for i := range 10 {
		if _, closed := c.subscribe(fmt.Sprint("s", i), filter); closed != "" {
			t.Fatalf("subscription %d refused: %s", i, closed)
		}
	}
	if _, closed := c.subscribe("s10", filter); !strings.Contains(closed, "too many") {
		t.Fatalf("the 11th subscription: %q", closed)
	}
}

// A flood of connections that never send anything accepted fills only the
// unproven pool: the wallet still connects, and idle ones are closed in time.
func TestConnectionFlood(t *testing.T) {
	s := setup(t, "[limits]\nunproven_connections = 8\nunproven_timeout = \"2s\"\n")
	var flood []*client
	for range 30 {
		flood = append(flood, s.r.connect())
	}
	// Only 8 may stay open: the oldest go at once, long before the 2 s timeout.
	time.Sleep(500 * time.Millisecond)
	early := 0
	for _, c := range flood {
		if c.isClosed(0) {
			early++
		}
	}
	if early < len(flood)-8 {
		t.Fatalf("%d of %d flood connections closed at once, want at least %d", early, len(flood), len(flood)-8)
	}
	w := s.r.connect()
	ok, reason := w.publish(allowList(t, s.r.control, nostr.Now()+1, s.clientA, s.serviceA))
	mustOK(t, "list from a wallet that connects during a flood", ok, reason)
	closed := 0
	for _, c := range flood {
		if c.isClosed(3 * time.Second) {
			closed++
		}
	}
	if closed != len(flood) {
		t.Fatalf("%d of %d idle connections closed", closed, len(flood))
	}
	if w.isClosed(100 * time.Millisecond) {
		t.Fatal("the proven wallet connection was closed")
	}
	// The connection proven before the flood still works.
	ok, reason = s.wallet.publish(sign(t, s.serviceA.secret, 23195, nostr.Now(), "x", p(s.clientA)))
	mustOK(t, "response after the flood", ok, reason)
}

// The side doors are shut: COUNT, negentropy and the NIP-86 management API.
func TestSideDoors(t *testing.T) {
	s := setup(t, "")
	c := s.r.connect()
	c.send(`["COUNT","c",{"kinds":[23195],"authors":["` + s.serviceA.hex + `"]}]`)
	c.send(`["NEG-OPEN","n",{"kinds":[23195]},"6100000200"]`)
	deadline := time.After(3 * time.Second)
	answers := 0
	for answers < 2 {
		select {
		case env := <-c.in:
			switch e := env.(type) {
			case *nostr.CountEnvelope:
				if e.Count != nil && *e.Count != 0 {
					t.Fatalf("COUNT answered %d", *e.Count)
				}
				answers++
			case *nostr.ClosedEnvelope, *nostr.NoticeEnvelope:
				answers++
			}
		case <-deadline:
			t.Fatalf("got %d of 2 answers", answers)
		}
	}
	resp, err := http.Post("http://"+s.r.addr, "application/nostr+json+rpc",
		strings.NewReader(`{"method":"supportedmethods","params":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("NIP-86 POST: %s", resp.Status)
	}
}

// A connection that is not proven may send only a little: khatru checks each
// event's signature before the relay's rules see it, so this bounds that work.
func TestUnprovenByteRate(t *testing.T) {
	s := setup(t, "[limits]\nmessage_bytes = 98304\nunproven_bytes_per_second = 1024\nrefusals = 100000\n")
	c := s.r.connect()
	stranger := newKeys()
	sent := 0
	for i := range 300 { // About 330 KB, over the 192 KiB burst.
		e := sign(t, stranger.secret, 1, nostr.Now(), strings.Repeat("x", 1000)+fmt.Sprint(i), nil)
		e.Sig[0] ^= 1 // An invalid signature: khatru refuses it before any hook.
		if c.sendRaw(eventText(t, e)) != nil {
			break
		}
		sent++
	}
	deadline := time.After(3 * time.Second)
	answered := 0
	for done := false; !done; {
		select {
		case env := <-c.in:
			if _, ok := env.(*nostr.OKEnvelope); ok {
				answered++
			}
		case <-deadline:
			done = true
		}
	}
	if answered >= sent || answered > 220 {
		t.Fatalf("%d of %d events answered in 3 s at 1 KiB/s after a 192 KiB burst", answered, sent)
	}
	t.Logf("%d of %d events answered in 3 s", answered, sent)
}
