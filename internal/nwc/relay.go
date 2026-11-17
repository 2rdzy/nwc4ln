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
	if len(s.connectionList()) == 0 {
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
// the info events, so a client that reads one finds the service listening.
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
	subscription, err := relay.Subscribe(ctx, nostr.Filters{s.filter(since)})
	if err != nil {
		return err
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
	)
	defer func() {
		subscription.Unsub() // The one in use at the end.
		if old != nil {
			old.Unsub()
		}
	}()
	s.setRelay(relay)
	s.requestInfo()
	s.log.Info("listening on relay", "relay", s.relay, "connections", len(s.connectionList()))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-relay.Context().Done():
			return context.Cause(relay.Context())
		case reason := <-subscription.ClosedReason:
			return fmt.Errorf("relay closed the subscription: %s", reason)
		case <-s.changed:
			// A request that reaches both subscriptions is dropped as a repeat.
			next, err := relay.Subscribe(ctx, nostr.Filters{s.filter(since)})
			if err != nil {
				return err
			}
			if old != nil { // A change during a change: the oldest has overlapped long enough.
				old.Unsub()
			}
			old, oldEvents = subscription, subscription.Events
			subscription, ready = next, next.EndOfStoredEvents
			retire = time.After(overlapLimit) // In case the relay never confirms it.
			s.requestInfo()
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
		case event, open := <-subscription.Events:
			if !open {
				return errors.New("subscription ended")
			}
			s.receive(ctx, event)
		}
	}
}

// filter asks for requests to every connection on one subscription, because
// relays limit subscriptions per connection. It does not name client keys: a
// revoked client must still reach the service to get UNAUTHORIZED.
func (s *Service) filter(since nostr.Timestamp) nostr.Filter {
	list := s.connectionList()
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
		if c.revoked {
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
