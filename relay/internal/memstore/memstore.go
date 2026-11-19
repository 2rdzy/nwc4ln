// Package memstore keeps what the relay serves from memory: one info event per
// service key, and recent requests so that a wallet catches up after a short
// outage (spec section 16). There is no database.
package memstore

import (
	"iter"
	"slices"
	"sync"
	"time"

	"fiatjaf.com/nostr"
)

// Limits bound the request buffer.
type Limits struct {
	Keep      time.Duration // How long a request is kept.
	Count     int           // Requests kept in all.
	PerClient int           // Requests kept for one client key.
	Bytes     int64         // Size of the requests kept, roughly.
}

// Store is safe for use by many goroutines.
type Store struct {
	limits Limits
	now    func() time.Time

	mu        sync.Mutex
	info      map[nostr.PubKey]nostr.Event
	requests  []stored // Oldest first, by arrival, from index head on.
	head      int
	perClient map[nostr.PubKey]int
	bytes     int64
}

type stored struct {
	event   nostr.Event
	arrived time.Time
	size    int64
}

// New returns an empty store. now is the clock, for tests.
func New(limits Limits, now func() time.Time) *Store {
	return &Store{
		limits:    limits,
		now:       now,
		info:      make(map[nostr.PubKey]nostr.Event),
		perClient: make(map[nostr.PubKey]int),
	}
}

// PutInfo keeps an info event if it is newer than the one held for its key. Of
// two with the same time, the lower id wins, as NIP-01 says.
func (s *Store) PutInfo(event nostr.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.info[event.PubKey]
	if ok && (old.CreatedAt > event.CreatedAt ||
		(old.CreatedAt == event.CreatedAt && slices.Compare(old.ID[:], event.ID[:]) <= 0)) {
		return
	}
	s.info[event.PubKey] = event
}

// AddRequest keeps a request. A client at its own limit gets no more kept, so
// one client cannot push out the others. When the total count or size is
// reached, the oldest requests go first. A request that is not kept is still
// sent to every listener: only the catch-up after an outage misses it.
func (s *Store) AddRequest(event nostr.Event) {
	if s.limits.Keep <= 0 || s.limits.Count <= 0 || s.limits.PerClient <= 0 {
		return
	}
	size := Size(event)
	if size > s.limits.Bytes {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.expire(now)
	if s.perClient[event.PubKey] >= s.limits.PerClient {
		return
	}
	for s.len() > 0 && (s.len() >= s.limits.Count || s.bytes+size > s.limits.Bytes) {
		s.dropOldest()
	}
	s.requests = append(s.requests, stored{event: event, arrived: now, size: size})
	s.perClient[event.PubKey]++
	s.bytes += size
}

// Query returns the kept events the filter matches, newest first, up to the
// filter's limit.
func (s *Store) Query(filter nostr.Filter) iter.Seq[nostr.Event] {
	s.mu.Lock()
	s.expire(s.now())
	var found []nostr.Event
	for _, event := range s.info {
		if filter.Matches(event) {
			found = append(found, event)
		}
	}
	for _, r := range s.requests[s.head:] {
		if filter.Matches(r.event) {
			found = append(found, r.event)
		}
	}
	s.mu.Unlock()
	slices.SortFunc(found, func(a, b nostr.Event) int { return int(b.CreatedAt - a.CreatedAt) })
	if filter.Limit > 0 && len(found) > filter.Limit {
		found = found[:filter.Limit]
	}
	return slices.Values(found)
}

// Keep drops what belongs to keys no longer listed: info events of a service key,
// and requests of a client key.
func (s *Store) Keep(isService, isClient func(nostr.PubKey) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.info {
		if !isService(key) {
			delete(s.info, key)
		}
	}
	kept := make([]stored, 0, s.len())
	for _, r := range s.requests[s.head:] {
		if isClient(r.event.PubKey) {
			kept = append(kept, r)
			continue
		}
		s.forget(r)
	}
	s.requests, s.head = kept, 0
}

// Counts reports what is kept, for tests and logs.
func (s *Store) Counts() (info, requests int, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.info), s.len(), s.bytes
}

func (s *Store) expire(now time.Time) {
	for s.len() > 0 && now.Sub(s.requests[s.head].arrived) > s.limits.Keep {
		s.dropOldest()
	}
}

func (s *Store) len() int { return len(s.requests) - s.head }

// dropOldest removes the oldest request. The slice is compacted once half of it
// is unused, so that each removal costs little.
func (s *Store) dropOldest() {
	s.forget(s.requests[s.head])
	s.requests[s.head] = stored{}
	s.head++
	if s.head > len(s.requests)/2 {
		s.requests = slices.Clone(s.requests[s.head:])
		s.head = 0
	}
}

func (s *Store) forget(r stored) {
	s.bytes -= r.size
	if s.perClient[r.event.PubKey]--; s.perClient[r.event.PubKey] <= 0 {
		delete(s.perClient, r.event.PubKey)
	}
}

// Size is an event's rough size in memory.
func Size(event nostr.Event) int64 {
	size := int64(len(event.Content)) + 200
	for _, tag := range event.Tags {
		for _, value := range tag {
			size += int64(len(value)) + 16
		}
	}
	return size
}
