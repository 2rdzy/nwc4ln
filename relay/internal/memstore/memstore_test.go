package memstore

import (
	"slices"
	"testing"
	"time"

	"fiatjaf.com/nostr"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func request(t *testing.T, secret nostr.SecretKey, created int64, content string) nostr.Event {
	t.Helper()
	e := nostr.Event{Kind: 23194, CreatedAt: nostr.Timestamp(created), Content: content}
	if err := e.Sign(secret); err != nil {
		t.Fatal(err)
	}
	return e
}

func all(s *Store) []nostr.Event {
	return slices.Collect(s.Query(nostr.Filter{Kinds: []nostr.Kind{23194, 13194}}))
}

func TestRequestsExpire(t *testing.T) {
	c := &clock{time.Unix(1000, 0)}
	s := New(Limits{Keep: 10 * time.Minute, Count: 100, PerClient: 100, Bytes: 1 << 20}, c.now)
	client := nostr.Generate()
	s.AddRequest(request(t, client, 1000, "a"))
	c.t = c.t.Add(5 * time.Minute)
	s.AddRequest(request(t, client, 1300, "b"))
	if got := all(s); len(got) != 2 || got[0].Content != "b" {
		t.Fatalf("inside the buffer time: %v", got)
	}
	c.t = c.t.Add(6 * time.Minute) // The first is now 11 minutes old.
	if got := all(s); len(got) != 1 || got[0].Content != "b" {
		t.Fatalf("after the buffer time: %v", got)
	}
	since := nostr.Filter{Kinds: []nostr.Kind{23194}, Since: 1301}
	if got := slices.Collect(s.Query(since)); len(got) != 0 {
		t.Fatalf("since is not applied: %v", got)
	}
}

func TestRequestLimits(t *testing.T) {
	c := &clock{time.Unix(1000, 0)}
	s := New(Limits{Keep: time.Hour, Count: 5, PerClient: 3, Bytes: 1 << 20}, c.now)
	loud, quiet := nostr.Generate(), nostr.Generate()
	for i := range 10 {
		s.AddRequest(request(t, loud, 1000+int64(i), "loud"))
	}
	if _, n, _ := s.Counts(); n != 3 {
		t.Fatalf("one client kept %d requests, limit 3", n)
	}
	for i := range 4 {
		s.AddRequest(request(t, quiet, 2000+int64(i), "quiet"))
	}
	got := all(s)
	if len(got) != 5 {
		t.Fatalf("kept %d requests, limit 5", len(got))
	}
	quietKept := 0
	for _, e := range got {
		if e.PubKey == quiet.Public() {
			quietKept++
		}
	}
	if quietKept != 3 {
		t.Fatalf("a client at its own limit pushed out others: quiet kept %d", quietKept)
	}

	small := New(Limits{Keep: time.Hour, Count: 100, PerClient: 100, Bytes: 3 * Size(request(t, loud, 1, "x"))}, c.now)
	for i := range 5 {
		small.AddRequest(request(t, loud, int64(i), "x"))
	}
	if _, n, b := small.Counts(); n != 3 || b > small.limits.Bytes {
		t.Fatalf("byte limit: %d requests, %d bytes", n, b)
	}
}

func TestInfoAndKeep(t *testing.T) {
	c := &clock{time.Unix(1000, 0)}
	s := New(Limits{Keep: time.Hour, Count: 10, PerClient: 10, Bytes: 1 << 20}, c.now)
	service, client := nostr.Generate(), nostr.Generate()
	info := func(created int64, content string) nostr.Event {
		e := nostr.Event{Kind: 13194, CreatedAt: nostr.Timestamp(created), Content: content}
		if err := e.Sign(service); err != nil {
			t.Fatal(err)
		}
		return e
	}
	s.PutInfo(info(10, "new"))
	s.PutInfo(info(9, "old"))
	if got := all(s); len(got) != 1 || got[0].Content != "new" {
		t.Fatalf("an older info event replaced a newer one: %v", got)
	}
	s.AddRequest(request(t, client, 1000, "r"))
	s.Keep(func(nostr.PubKey) bool { return false }, func(nostr.PubKey) bool { return true })
	if i, r, _ := s.Counts(); i != 0 || r != 1 {
		t.Fatalf("after dropping the service key: %d info, %d requests", i, r)
	}
	s.Keep(func(nostr.PubKey) bool { return false }, func(nostr.PubKey) bool { return false })
	if _, r, b := s.Counts(); r != 0 || b != 0 {
		t.Fatalf("after dropping the client key: %d requests, %d bytes", r, b)
	}
}

func TestQueryLimit(t *testing.T) {
	c := &clock{time.Unix(1000, 0)}
	s := New(Limits{Keep: time.Hour, Count: 100, PerClient: 100, Bytes: 1 << 20}, c.now)
	client := nostr.Generate()
	for i := range 10 {
		s.AddRequest(request(t, client, 1000+int64(i), ""))
	}
	got := slices.Collect(s.Query(nostr.Filter{Kinds: []nostr.Kind{23194}, Limit: 3}))
	if len(got) != 3 || got[0].CreatedAt != 1009 {
		t.Fatalf("limit 3 gave %d, newest %d", len(got), got[0].CreatedAt)
	}
}
