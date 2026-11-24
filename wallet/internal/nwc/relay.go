package nwc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/getAlby/go-nostr"
)

// Run keeps the service on the relay until ctx ends, and waits for requests still
// running. The relay going away is normal, so it never returns an error for that.
func (s *Service) Run(ctx context.Context) {
	defer s.work.Wait()
	s.work.Go(func() { s.notifyLoop(ctx) })
	s.work.Go(func() { s.maintain(ctx) })
	s.work.Go(func() { s.infoLoop(ctx) })
	for range publishers {
		s.work.Go(func() { s.publishLoop(ctx) })
	}
	// With relay control on, the service connects even with no connection, because
	// the relay must hear that the list is empty.
	if s.control == nil && len(s.connectionList()) == 0 {
		s.log.Warn("no connections to serve yet")
		select {
		case <-ctx.Done():
			return
		case <-s.changed:
		}
	}
	backoff := backoffFirst
	for ctx.Err() == nil {
		began := time.Now()
		if err := s.serve(ctx); err != nil && ctx.Err() == nil {
			s.log.Warn("relay connection lost", "relay", s.relay, "error", err)
		}
		// A connection that lasted was healthy. One that ends at once must not
		// turn into a tight loop.
		if time.Since(began) >= backoffMax {
			backoff = backoffFirst
		}
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, backoffMax)
	}
}

// serve runs one relay connection, until it ends. It subscribes before publishing
// the info events, so a client that reads one finds the service listening. With
// relay control on, the allow-list goes first, on connect and on each change: the
// relay refuses a subscription or an info event for a key it does not list.
func (s *Service) serve(ctx context.Context) error {
	since, err := s.since(ctx)
	if err != nil {
		return err
	}
	relay, err := nostr.RelayConnect(ctx, s.relay, nostr.WithNoticeHandler(s.notice))
	if err != nil {
		return err
	}
	defer func() {
		s.setRelay(nil)
		_ = relay.Close()
	}()
	list := s.connectionList()
	if s.control != nil {
		if list, err = s.sendList(ctx, relay); err != nil {
			return err
		}
	}
	// With relay control on and no connection, there is nothing to listen for: the
	// relay refuses a filter that names no key. Off, nothing changes.
	var subscription *nostr.Subscription
	if s.control == nil || len(list) > 0 {
		if subscription, err = relay.Subscribe(ctx, nostr.Filters{filterFor(list, since)}); err != nil {
			return err
		}
	}
	// While connections change, the old subscription is still read until the relay
	// confirms the new one, so that no request falls between them. Closing a
	// subscription drops what it received and nobody read yet, so it is closed only
	// once the new one has had time to take over.
	var (
		old       *nostr.Subscription
		oldEvents chan *nostr.Event
		ready     chan struct{} // The new subscription's end of stored events.
		retire    <-chan time.Time
		// With relay control on, a change first sends the list, apart from this loop,
		// which must keep reading requests while the relay answers. listing is true
		// while one is out, and again while another change waits for it.
		listDone       = make(chan allowListResult, 1)
		listing, again bool
	)
	defer func() {
		if subscription != nil {
			subscription.Unsub() // The one in use at the end.
		}
		if old != nil {
			old.Unsub()
		}
	}()
	// switchTo makes next the subscription in use, and keeps reading the one before
	// for a while. next is nil when there is nothing left to listen for.
	switchTo := func(next *nostr.Subscription) {
		if old != nil { // A change during a change: the oldest has overlapped long enough.
			old.Unsub()
			old, oldEvents = nil, nil
		}
		if subscription != nil {
			old, oldEvents = subscription, subscription.Events
		}
		subscription, ready = next, nil
		if next != nil {
			ready = next.EndOfStoredEvents
		}
		retire = time.After(overlapLimit) // In case the relay never confirms it.
		s.requestInfo()
	}
	sendList := func() {
		listing = true
		go func() {
			list, err := s.sendList(ctx, relay)
			listDone <- allowListResult{list: list, err: err}
		}()
	}
	s.setRelay(relay)
	s.requestInfo()
	s.log.Info("listening on relay", "relay", s.relay, "connections", len(list))
	for {
		var (
			closed <-chan string
			events chan *nostr.Event
		)
		if subscription != nil {
			closed, events = subscription.ClosedReason, subscription.Events
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-relay.Context().Done():
			return context.Cause(relay.Context())
		case reason := <-closed:
			return fmt.Errorf("relay closed the subscription: %s", reason)
		case <-s.changed:
			if s.control != nil {
				if listing {
					again = true
				} else {
					sendList()
				}
				continue
			}
			// A request that reaches both subscriptions is dropped as a repeat.
			next, err := relay.Subscribe(ctx, nostr.Filters{s.filter(since)})
			if err != nil {
				return err
			}
			switchTo(next)
		case result := <-listDone:
			listing = false
			if result.err != nil {
				return result.err
			}
			var next *nostr.Subscription
			if len(result.list) > 0 {
				if next, err = relay.Subscribe(ctx, nostr.Filters{filterFor(result.list, since)}); err != nil {
					return err
				}
			}
			switchTo(next)
			if again {
				again = false
				sendList()
			}
		case <-ready:
			ready = nil
			retire = time.After(overlapAfterReady)
		case <-retire:
			retire = nil
			if old != nil {
				old.Unsub()
				old, oldEvents = nil, nil
			}
		case event, open := <-oldEvents:
			if !open {
				oldEvents = nil
				continue
			}
			s.receive(ctx, event)
		case event, open := <-events:
			if !open {
				return errors.New("subscription ended")
			}
			s.receive(ctx, event)
		}
	}
}

// allowListResult is the outcome of sending the allow-list: the connections on it.
type allowListResult struct {
	list []connection
	err  error
}

// filter asks for requests to every connection on one subscription, because
// relays limit subscriptions per connection. It does not name client keys: a
// revoked client must still reach the service to get UNAUTHORIZED.
func (s *Service) filter(since nostr.Timestamp) nostr.Filter {
	return filterFor(s.connectionList(), since)
}

// filterFor is the filter for the given connections.
func filterFor(list []connection, since nostr.Timestamp) nostr.Filter {
	keys := make([]string, 0, len(list))
	for _, c := range list {
		keys = append(keys, c.servicePub)
	}
	return nostr.Filter{Kinds: []int{kindRequest}, Tags: nostr.TagMap{"p": keys}, Since: &since}
}

// notice logs what the relay says, at a bounded rate.
func (s *Service) notice(notice string) {
	if ok, skipped := s.notices.allow(s.now()); ok {
		s.log.Info("relay notice", "notice", notice, "skipped", skipped)
	}
}

// receive stores a request in order, then runs it apart from the others, because
// a payment can take a minute. Nothing here waits for the relay, which this loop
// reads: what waits on the relay piles up in the library.
func (s *Service) receive(ctx context.Context, event *nostr.Event) {
	request := s.accept(ctx, event)
	if request == nil {
		return
	}
	done := s.releaseReply
	if request.reserved {
		done = func() { s.release(request.connection.id) }
	} else if !s.takeReply() {
		s.log.Debug("request dropped: too many refusals at once", "event", event.ID)
		return
	}
	s.work.Go(func() {
		defer done()
		if reply := s.run(ctx, request); reply != nil {
			s.queue(*reply, event.ID)
		}
	})
}

// outgoing is a reply waiting to be published.
type outgoing struct {
	event   nostr.Event
	request string // The request's event ID, for the log.
}

// queue hands a reply to the publishers. It never waits.
func (s *Service) queue(event nostr.Event, request string) {
	select {
	case s.outbox <- outgoing{event: event, request: request}:
	default:
		if ok, skipped := s.dropped.allow(s.now()); ok {
			s.log.Warn("response dropped: too many waiting for the relay", "event", request, "skipped", skipped)
		}
	}
}

func (s *Service) publishLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-s.outbox:
			if err := s.send(ctx, item.event); err != nil {
				s.log.Error("response lost", "event", item.request, "error", err)
			}
		}
	}
}

// requestInfo asks the info loop to publish the info events, and never waits.
func (s *Service) requestInfo() {
	select {
	case s.infoNeeded <- struct{}{}:
	default: // One is pending already, and it reads the connections afresh.
	}
}

// infoLoop publishes the info events when asked, apart from the relay loop, because
// each publish can wait publishWithin for the relay.
func (s *Service) infoLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.infoNeeded:
			s.publishInfo(ctx)
		}
	}
}

// send publishes a response, and keeps trying through a short relay outage. A
// client that hears nothing can use lookup_invoice, so the wait is bounded.
func (s *Service) send(ctx context.Context, event nostr.Event) error {
	deadline := time.Now().Add(s.sendFor)
	for {
		err := s.publish(ctx, event)
		if err == nil {
			return nil
		}
		if !time.Now().Before(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(s.sendRetry):
		}
	}
}

func (s *Service) publishToRelay(ctx context.Context, event nostr.Event) error {
	s.mu.Lock()
	relay := s.conn
	s.mu.Unlock()
	if relay == nil {
		return errors.New("not connected to the relay")
	}
	ctx, cancel := context.WithTimeout(ctx, publishWithin)
	defer cancel()
	return relay.Publish(ctx, event)
}

func (s *Service) setRelay(relay *nostr.Relay) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conn = relay
}

// publishInfo publishes each live connection's info event. A relay may have lost
// them, so this runs on every connect. A failure is only logged: requests can
// still be served without it.
func (s *Service) publishInfo(ctx context.Context) {
	for _, c := range s.connectionList() {
		// With relay control on, the relay refuses an info event for a key it does
		// not list yet. The list that names it asks for the info events again.
		if c.revoked || (s.control != nil && !s.control.isListed(c.servicePub)) {
			continue
		}
		event, err := s.infoEvent(c)
		if err == nil {
			err = s.publish(ctx, *event)
		}
		if err != nil {
			s.log.Warn("cannot publish info event", "connection", c.id, "error", err)
		}
	}
}

// infoEvent lists only what this connection may call and be told about, because a client will call
// whatever it finds here. A relay keeps one info event per key, and of two with the
// same time it may keep either, so each is stamped later than the last.
func (s *Service) infoEvent(c connection) (*nostr.Event, error) {
	created := max(s.now().Unix(), s.lastInfo[c.id]+1)
	s.lastInfo[c.id] = created
	event := nostr.Event{
		Kind: kindInfo, CreatedAt: nostr.Timestamp(created),
		Content: strings.Join(s.methodNames(c.canPay, c.canInvoice), " "),
		Tags: nostr.Tags{
			{"encryption", s.encryptionTag()},
			{"notifications", strings.Join(notificationNames(c.canPay), " ")},
			{"extensions", strings.Join(extensions, " ")},
		},
	}
	if err := event.Sign(c.serviceKey); err != nil {
		return nil, fmt.Errorf("sign info event: %w", err)
	}
	return &event, nil
}
