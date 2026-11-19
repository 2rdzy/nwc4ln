//go:build relay

package relaytest

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fiatjaf.com/nostr"
)

// setup starts a relay and gives it a list of two pairs.
type pairSetup struct {
	r                                    *relay
	wallet                               *client
	clientA, serviceA, clientB, serviceB keys
}

func setup(t *testing.T, extra string) pairSetup {
	t.Helper()
	s := pairSetup{r: startRelay(t, extra), clientA: newKeys(), serviceA: newKeys(), clientB: newKeys(), serviceB: newKeys()}
	s.wallet = s.r.connect()
	ok, reason := s.wallet.publish(allowList(t, s.r.control, nostr.Now(), s.clientA, s.serviceA, s.clientB, s.serviceB))
	mustOK(t, "allow-list", ok, reason)
	return s
}

// Before the first list, the relay accepts only a list.
func TestBeforeFirstList(t *testing.T) {
	r := startRelay(t, "")
	service, client := newKeys(), newKeys()
	c := r.connect()
	ok, reason := c.publish(sign(t, service.secret, 13194, nostr.Now(), "pay_invoice", nil))
	mustRefuse(t, "info event before the list", ok, reason)
	_, closed := c.subscribe("a", nostr.Filter{Kinds: []nostr.Kind{13194}, Authors: []nostr.PubKey{service.pub}})
	if closed == "" {
		t.Fatal("filter served before the list")
	}
	ok, reason = c.publish(allowList(t, r.control, nostr.Now(), client, service))
	mustOK(t, "first list", ok, reason)
	ok, reason = c.publish(sign(t, service.secret, 13194, nostr.Now(), "pay_invoice", nil))
	mustOK(t, "info event after the list", ok, reason)
}

// A stranger's key gets nothing in, by any of khatru's paths: regular,
// replaceable, addressable, ephemeral and deletion events.
func TestStrangerRefused(t *testing.T) {
	s := setup(t, "")
	stranger := newKeys()
	c := s.r.connect()
	for _, kind := range []nostr.Kind{1, 13194, 30078, 20001, 23194, 23195, 23196, 23197, 5} {
		ok, reason := c.publish(sign(t, stranger.secret, kind, nostr.Now(), "x", p(s.serviceA)))
		mustRefuse(t, "stranger kind "+kind.Name(), ok, reason)
	}
	// A listed service key cannot use the relay for other kinds either.
	ok, reason := s.wallet.publish(sign(t, s.serviceA.secret, 1, nostr.Now(), "x", nil))
	mustRefuse(t, "text note by a service key", ok, reason)
}

// A listed client may send requests only to its own service key.
func TestPairsEnforced(t *testing.T) {
	s := setup(t, "")
	c := s.r.connect()
	ok, reason := c.publish(sign(t, s.clientA.secret, 23194, nostr.Now(), "x", p(s.serviceB)))
	mustRefuse(t, "request to another pair's service key", ok, reason)
	ok, reason = c.publish(sign(t, s.clientA.secret, 23194, nostr.Now(), "x", p(s.serviceA)))
	mustOK(t, "request to its own service key", ok, reason)
}

// Responses and info events must be signed by a listed service key.
func TestWrongSigner(t *testing.T) {
	s := setup(t, "")
	c := s.r.connect()
	for name, event := range map[string]nostr.Event{
		"response by a client key":   sign(t, s.clientA.secret, 23195, nostr.Now(), "x", p(s.clientA)),
		"info by a client key":       sign(t, s.clientA.secret, 13194, nostr.Now(), "x", nil),
		"info by the control key":    sign(t, s.r.control, 13194, nostr.Now(), "x", nil),
		"notification by a stranger": sign(t, newKeys().secret, 23197, nostr.Now(), "x", p(s.clientA)),
	} {
		ok, reason := c.publish(event)
		mustRefuse(t, name, ok, reason)
	}
	ok, reason := s.wallet.publish(sign(t, s.serviceA.secret, 23195, nostr.Now(), "x", p(s.clientA)))
	mustOK(t, "response by its service key", ok, reason)
}

// Filters must name listed service keys, and the allow-list is never served.
func TestFiltersRefused(t *testing.T) {
	s := setup(t, "")
	c := s.r.connect()
	nwc := []nostr.Kind{13194, 23195, 23196, 23197}
	for name, filter := range map[string]nostr.Filter{
		"no authors":         {Kinds: nwc},
		"unlisted author":    {Kinds: nwc, Authors: []nostr.PubKey{newKeys().pub}},
		"client as author":   {Kinds: nwc, Authors: []nostr.PubKey{s.clientA.pub}},
		"allow-list kind":    {Kinds: []nostr.Kind{29194}, Authors: []nostr.PubKey{s.r.control.Public()}},
		"no kinds":           {Authors: []nostr.PubKey{s.serviceA.pub}},
		"#p with a stranger": {Kinds: []nostr.Kind{23194}, Tags: nostr.TagMap{"p": {newKeys().hex}}},
	} {
		events, closed := c.subscribe("s", filter)
		if closed == "" || len(events) > 0 {
			t.Fatalf("%s: served %d events", name, len(events))
		}
	}
	// A client reads its own service key's info event and live responses.
	ok, reason := s.wallet.publish(sign(t, s.serviceA.secret, 13194, nostr.Now(), "pay_invoice", nil))
	mustOK(t, "info", ok, reason)
	events, closed := c.subscribe("info", nostr.Filter{Kinds: []nostr.Kind{13194}, Authors: []nostr.PubKey{s.serviceA.pub}})
	if closed != "" || len(events) != 1 {
		t.Fatalf("info: %d events, %q", len(events), closed)
	}
	_, closed = c.subscribe("live", nostr.Filter{Kinds: []nostr.Kind{23195}, Authors: []nostr.PubKey{s.serviceA.pub}})
	if closed != "" {
		t.Fatalf("response filter refused: %s", closed)
	}
	reply := sign(t, s.serviceA.secret, 23195, nostr.Now(), "x", p(s.clientA))
	ok, reason = s.wallet.publish(reply)
	mustOK(t, "response", ok, reason)
	if got, ok := c.next("live", 3*time.Second); !ok || got.ID != reply.ID {
		t.Fatal("the response did not reach its client")
	}
	// A new list is not sent to anyone, even a listener on the relay.
	ok, reason = s.wallet.publish(allowList(t, s.r.control, nostr.Now()+1, s.clientA, s.serviceA))
	mustOK(t, "second list", ok, reason)
	if got, ok := c.next("live", 500*time.Millisecond); ok {
		t.Fatalf("a listener got event kind %d", got.Kind)
	}
}

// Only the control key can change the list, and old or far-future lists fail.
func TestListSignerAndTime(t *testing.T) {
	s := setup(t, "")
	now := nostr.Now()
	c := s.r.connect()
	ok, reason := c.publish(allowList(t, newKeys().secret, now+1, s.clientB, s.serviceB))
	mustRefuse(t, "list by another key", ok, reason)
	ok, reason = s.wallet.publish(allowList(t, s.r.control, now+300, s.clientB, s.serviceB))
	mustRefuse(t, "list 5 minutes ahead", ok, reason)
	// The far-future list did not lock out the next good one.
	ok, reason = s.wallet.publish(allowList(t, s.r.control, now+1, s.clientB, s.serviceB))
	mustOK(t, "next list", ok, reason)
	ok, reason = s.wallet.publish(sign(t, s.clientA.secret, 23194, nostr.Now(), "x", p(s.serviceA)))
	mustRefuse(t, "request from a pair no longer listed", ok, reason)
}

// An older list is refused, also after a restart, and after the saved file is
// lost a list older than the window is refused.
func TestListReplay(t *testing.T) {
	r := startRelay(t, "")
	a, sa, b, sb := newKeys(), newKeys(), newKeys(), newKeys()
	now := nostr.Now()
	old := allowList(t, r.control, now-60, a, sa)
	newer := allowList(t, r.control, now, b, sb)
	w := r.connect()
	w.accept("first list", old)
	w.accept("newer list", newer)
	ok, reason := w.publish(old)
	mustRefuse(t, "older list sent again", ok, reason)
	if !strings.Contains(reason, "created_at") {
		t.Fatalf("the refusal does not name the saved time: %s", reason)
	}
	r.restart()
	ok, reason = r.connect().publish(old)
	mustRefuse(t, "older list after a restart", ok, reason)

	r.stop()
	if err := os.Remove(filepath.Join(r.dir, "allowlist.json")); err != nil {
		t.Fatal(err)
	}
	r.start()
	stale := allowList(t, r.control, now-11*60, a, sa)
	ok, reason = r.connect().publish(stale)
	mustRefuse(t, "list 11 minutes old, file lost", ok, reason)
}

// A restart keeps the list: a client's request is accepted before the wallet
// connects again.
func TestRestartKeepsList(t *testing.T) {
	s := setup(t, "")
	s.r.restart()
	ok, reason := s.r.connect().publish(sign(t, s.clientA.secret, 23194, nostr.Now(), "x", p(s.serviceA)))
	mustOK(t, "request after restart", ok, reason)
}

// Requests sent while no wallet listens are kept for the buffer time.
func TestRequestBuffer(t *testing.T) {
	s := setup(t, "request_buffer = \"2s\"\n")
	c := s.r.connect()
	early := sign(t, s.clientA.secret, 23194, nostr.Now(), "early", p(s.serviceA))
	c.accept("request with no listener", early)
	filter := nostr.Filter{Kinds: []nostr.Kind{23194}, Tags: nostr.TagMap{"p": {s.serviceA.hex, s.serviceB.hex}}}
	events, closed := s.wallet.subscribe("w1", filter)
	if closed != "" || len(events) != 1 || events[0].ID != early.ID {
		t.Fatalf("inside the buffer time: %d events, %q", len(events), closed)
	}
	time.Sleep(3 * time.Second)
	events, closed = s.wallet.subscribe("w2", filter)
	if closed != "" || len(events) != 0 {
		t.Fatalf("after the buffer time: %d events, %q", len(events), closed)
	}
}

// The wallet holds three subscriptions while its connections change; each gets
// the request.
func TestWalletChangeSubscriptions(t *testing.T) {
	s := setup(t, "")
	filter := nostr.Filter{Kinds: []nostr.Kind{23194}, Tags: nostr.TagMap{"p": {s.serviceA.hex}}, Since: nostr.Now() + 1}
	for _, id := range []string{"w1", "w2", "w3"} {
		if _, closed := s.wallet.subscribe(id, filter); closed != "" {
			t.Fatalf("%s refused: %s", id, closed)
		}
	}
	req := sign(t, s.clientA.secret, 23194, nostr.Now()+1, "x", p(s.serviceA))
	s.r.connect().accept("request", req)
	seen := map[string]bool{}
	for range 3 {
		select {
		case env := <-s.wallet.in:
			if e, ok := env.(*nostr.EventEnvelope); ok && e.ID == req.ID {
				seen[*e.SubscriptionID] = true
			}
		case <-time.After(3 * time.Second):
		}
	}
	if len(seen) != 3 {
		t.Fatalf("request reached %d of 3 subscriptions", len(seen))
	}
}

// The relay refuses to start in a folder other users can change: they could
// replace the allow-list or the control key.
func TestRefusesSharedFolder(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o775); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "nwc4ln-relay.toml")
	text := "data_dir = \"" + dir + "\"\nlisten = \"127.0.0.1:0\"\ncontrol_key = \"" + newKeys().hex + "\"\n"
	if err := os.WriteFile(config, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, binary, "start", "--config", config).CombinedOutput()
	if err == nil || !strings.Contains(string(out), "others can change it") {
		t.Fatalf("started in a shared folder: %v\n%s", err, out)
	}
}
