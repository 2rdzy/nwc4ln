// Package server runs the relay on khatru. It is the only package that imports
// khatru: the rules are in policy, allowlist and memstore, and this package wires
// them to khatru's hooks and adds the connection limits.
package server

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log"
	"log/slog"
	"net"
	"net/http"
	"time"

	"fiatjaf.com/nostr"
	"fiatjaf.com/nostr/khatru"
	"fiatjaf.com/nostr/nip11"

	"github.com/2rdzy/nwc4ln/relay/internal/allowlist"
	"github.com/2rdzy/nwc4ln/relay/internal/config"
	"github.com/2rdzy/nwc4ln/relay/internal/memstore"
	"github.com/2rdzy/nwc4ln/relay/internal/policy"
)

// Refusal messages added here.
const (
	rateLimited  = "rate-limited: slow down"
	tooManySubs  = "rate-limited: too many open subscriptions"
	poolFull     = "rate-limited: the relay has too many connections"
	notSupported = "unsupported: this relay does not count events"
)

// Server is one relay.
type Server struct {
	settings config.Settings
	lists    *allowlist.Store
	mem      *memstore.Store
	log      *slog.Logger
	now      func() time.Time
	relay    *khatru.Relay
	tracker  *tracker
}

// New builds a relay. It does not listen yet.
func New(settings config.Settings, lists *allowlist.Store, logger *slog.Logger) *Server {
	s := &Server{settings: settings, lists: lists, log: logger, now: time.Now}
	s.mem = memstore.New(memstore.Limits{
		Keep:      settings.RequestBuffer,
		Count:     settings.BufferRequests,
		PerClient: settings.BufferPerClient,
		Bytes:     settings.BufferBytes,
	}, s.now)
	if list := lists.Current(); list != nil {
		s.mem.Keep(list.IsService, list.IsClient)
	}

	rl := khatru.NewRelay()
	rl.Log = log.New(&slogWriter{logger}, "", 0)
	rl.Info = &nip11.RelayInformationDocument{
		Name:          "nwc4ln-relay",
		Description:   "A Nostr relay locked to one wallet's Nostr Wallet Connect connections.",
		Software:      "https://github.com/2rdzy/nwc4ln",
		SupportedNIPs: []any{1, 11, 47},
	}
	rl.MaxMessageSize = settings.MessageBytes
	rl.OnEvent = s.onEvent
	rl.OnEphemeralEvent = s.onEphemeral
	rl.ReplaceEvent = s.replaceEvent
	rl.StoreEvent = func(context.Context, nostr.Event) error { return errors.New("blocked: not stored") }
	rl.DeleteEvent = func(context.Context, nostr.ID) error { return errors.New("blocked: nothing to delete") }
	rl.OnRequest = s.onRequest
	rl.OnCount = func(context.Context, nostr.Filter) (bool, string) { return true, notSupported }
	rl.QueryStored = func(_ context.Context, filter nostr.Filter) iter.Seq[nostr.Event] { return s.mem.Query(filter) }
	rl.PreventBroadcast = func(_ *khatru.WebSocket, _ nostr.Filter, event nostr.Event) bool {
		return event.Kind == allowlist.Kind
	}
	rl.OnListenerAdded = func(ws *khatru.WebSocket, _ int, _ string, _ nostr.Filter) {
		if c := s.connOf(ws); c != nil {
			c.addListener(1)
		}
	}
	rl.OnListenerRemoved = func(ws *khatru.WebSocket, _ int, _ string, _ nostr.Filter) {
		if c := s.connOf(ws); c != nil {
			c.addListener(-1)
		}
	}
	s.relay = rl
	return s
}

// Serve answers on the listener until the context ends.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	st := s.settings
	s.tracker = newTracker(ln, connLimits{
		proven:          st.Connections,
		unproven:        st.UnprovenConns,
		unprovenTimeout: st.UnprovenTimeout,
		refusals:        st.Refusals,
		messagesPerSec:  st.MessagesPerSec,
		messageBurst:    st.MessageBurst,
		subscriptions:   st.Subscriptions,
		provenBytes:     st.ProvenBytesPerSec,
		unprovenBps:     st.UnprovenBytesPerSec,
		byteBurst:       int(2 * st.MessageBytes),
	}, func(reason string, c *conn) {
		s.log.Debug("closed a connection", "reason", reason, "remote", c.addr)
	})
	server := &http.Server{
		Handler:           s.handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 * 1024,
		ErrorLog:          log.New(&slogWriter{s.log}, "", 0),
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(s.tracker) }()
	s.log.Info("relay listening", "address", ln.Addr().String(), "allow_list_pairs", s.pairs())
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := server.Shutdown(shutdown)
	s.relay.DisconnectAll()
	if errors.Is(err, context.DeadlineExceeded) {
		err = nil
	}
	return err
}

// handler passes only websocket upgrades and the NIP-11 document to khatru.
// Everything else, the NIP-86 management API included, gets a plain answer.
func (s *Server) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Upgrade") == "websocket" || r.Header.Get("Accept") == "application/nostr+json" {
			s.relay.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = fmt.Fprintln(w, "nwc4ln-relay: a Nostr relay for one wallet's NWC connections.")
	})
}

func (s *Server) pairs() int {
	if list := s.lists.Current(); list != nil {
		return list.Len()
	}
	return 0
}

func (s *Server) connOf(ws *khatru.WebSocket) *conn {
	if ws == nil || ws.Request == nil || s.tracker == nil {
		return nil
	}
	return s.tracker.lookup(ws.Request.RemoteAddr)
}

// check runs the per-connection checks common to events and filters. It returns
// a refusal, or "" to go on.
func (s *Server) check(c *conn) string {
	if c == nil {
		return "error: unknown connection"
	}
	if !c.allow() {
		return rateLimited
	}
	return ""
}

// result counts a refusal against the connection, or proves it on success.
func (s *Server) result(c *conn, reject bool, msg string) (bool, string) {
	if reject {
		if c != nil {
			c.refuse()
		}
		return true, msg
	}
	if c != nil && !c.prove() {
		c.refuse()
		return true, poolFull
	}
	return false, ""
}

func (s *Server) onEvent(ctx context.Context, event nostr.Event) (bool, string) {
	c := s.connOf(khatru.GetConnection(ctx))
	if msg := s.check(c); msg != "" {
		return s.result(c, true, msg)
	}
	if event.Kind == allowlist.Kind {
		res := s.lists.Offer(event, s.now())
		if !res.Accepted {
			if res.FromControl {
				s.log.Warn("refused an allow-list from the control key", "reason", res.Reason,
					"created_at", int64(event.CreatedAt), "pairs", len(event.Tags))
			} else {
				s.log.Debug("refused an allow-list", "reason", res.Reason)
			}
			return s.result(c, true, res.Reason)
		}
		list := s.lists.Current()
		s.mem.Keep(list.IsService, list.IsClient)
		s.log.Info("allow-list accepted", "created_at", int64(list.Event.CreatedAt), "pairs", list.Len())
		return s.result(c, false, "")
	}
	reject, msg := policy.Write(s.lists.Current(), event)
	if reject {
		s.log.Debug("refused an event", "kind", int(event.Kind), "reason", msg)
	}
	return s.result(c, reject, msg)
}

// onEphemeral keeps requests for a wallet that reconnects. It runs only for
// events onEvent accepted.
func (s *Server) onEphemeral(_ context.Context, event nostr.Event) {
	if event.Kind == policy.KindRequest {
		s.mem.AddRequest(event)
	}
}

// replaceEvent keeps info events. Other replaceable kinds never get here,
// because onEvent refuses them first.
func (s *Server) replaceEvent(_ context.Context, event nostr.Event) error {
	if event.Kind != policy.KindInfo {
		return errors.New("blocked: not stored")
	}
	s.mem.PutInfo(event)
	return nil
}

func (s *Server) onRequest(ctx context.Context, filter nostr.Filter) (bool, string) {
	c := s.connOf(khatru.GetConnection(ctx))
	if msg := s.check(c); msg != "" {
		return s.result(c, true, msg)
	}
	if c.listenerCount() >= s.settings.Subscriptions {
		return s.result(c, true, tooManySubs)
	}
	reject, msg := policy.Read(s.lists.Current(), filter)
	if reject {
		s.log.Debug("refused a filter", "reason", msg)
	}
	return s.result(c, reject, msg)
}

// slogWriter sends khatru's and net/http's log lines to slog at debug level:
// they report what clients do wrong, which strangers control.
type slogWriter struct{ log *slog.Logger }

func (w *slogWriter) Write(p []byte) (int, error) {
	w.log.Debug(string(trimNewline(p)))
	return len(p), nil
}

func trimNewline(p []byte) []byte {
	for len(p) > 0 && (p[len(p)-1] == '\n' || p[len(p)-1] == '\r') {
		p = p[:len(p)-1]
	}
	return p
}
