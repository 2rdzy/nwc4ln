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
	if len(s.connections) == 0 {
		s.log.Warn("no connections to serve; one added later is picked up at the next start")
		<-ctx.Done()
		return
	}
	s.work.Go(func() { s.notifyLoop(ctx) })
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
	relay, err := nostr.RelayConnect(ctx, s.relay, nostr.WithNoticeHandler(func(notice string) {
		s.log.Info("relay notice", "notice", notice)
	}))
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
	defer subscription.Unsub()
	s.setRelay(relay)
	s.publishInfo(ctx)
	s.log.Info("listening on relay", "relay", s.relay, "connections", len(s.connections))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-relay.Context().Done():
			return context.Cause(relay.Context())
		case reason := <-subscription.ClosedReason:
			return fmt.Errorf("relay closed the subscription: %s", reason)
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
	keys := make([]string, 0, len(s.connections))
	for key := range s.connections {
		keys = append(keys, key)
	}
	return nostr.Filter{Kinds: []int{kindRequest}, Tags: nostr.TagMap{"p": keys}, Since: &since}
}

// receive stores a request in order, then runs it apart from the others, because
// a payment can take a minute.
func (s *Service) receive(ctx context.Context, event *nostr.Event) {
	request := s.accept(ctx, event)
	if request == nil {
		return
	}
	s.work.Go(func() {
		reply := s.run(ctx, request)
		if reply == nil {
			return
		}
		if err := s.send(ctx, *reply); err != nil {
			s.log.Error("response lost", "event", event.ID, "error", err)
		}
	})
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
	for _, c := range s.connections {
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

// infoEvent lists only what works today, because a client will call whatever it
// finds here.
func (s *Service) infoEvent(c connection) (*nostr.Event, error) {
	event := nostr.Event{
		Kind: kindInfo, CreatedAt: nostr.Timestamp(s.now().Unix()),
		Content: strings.Join(s.methodNames(), " "),
		Tags: nostr.Tags{
			{"encryption", encryptionTag},
			{"notifications", strings.Join(notificationTypes, " ")},
			{"extensions", strings.Join(extensions, " ")},
		},
	}
	if err := event.Sign(c.serviceKey); err != nil {
		return nil, fmt.Errorf("sign info event: %w", err)
	}
	return &event, nil
}
