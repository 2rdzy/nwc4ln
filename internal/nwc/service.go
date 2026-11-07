// Package nwc is the Nostr Wallet Connect side of nwc4ln: keys, encryption, the
// relay connection and request handling. It is the only code that imports
// go-nostr.
//
// Each connection has its own service key. A request is signed by the client
// key and encrypted to the service key, so the signature is what proves who
// asks, and every request is stored by event ID before any work, so that a relay
// repeating it cannot make it run twice.
package nwc

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/getAlby/go-nostr"

	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/store"
)

// Event kinds from NIP-47.
const (
	kindInfo     = 13194
	kindRequest  = 23194
	kindResponse = 23195
)

// Limits on when a request that moves money or makes an invoice may run. After
// downtime, the client may have paid another way (R13).
const (
	maxAge    = 6 * time.Hour
	maxFuture = 10 * time.Minute
)

// How the service catches up, and how long an answer waits for the relay.
const (
	// sinceMargin is how far before the last seen request to subscribe from.
	sinceMargin = 5 * time.Minute

	backoffFirst = time.Second
	backoffMax   = 30 * time.Second

	sendFor       = 30 * time.Second
	sendRetry     = 2 * time.Second
	publishWithin = 7 * time.Second

	lastSeenKey = "nwc_last_seen"
)

// connection is what the service keeps in memory for one connection. Revoked
// connections are kept, so that their requests can be answered with
// UNAUTHORIZED. Whether one is revoked is read from the database per request.
type connection struct {
	id         int64
	clientPub  string
	serviceKey string
	servicePub string
	revoked    bool // At load. Only decides whether an info event is published.
}

// method answers one command. It returns a result, or an error for the client.
type method func(ctx context.Context, params json.RawMessage) (any, *Error)

// Service answers NWC requests from one relay.
type Service struct {
	db    *sql.DB
	node  node.Node
	relay string
	log   *slog.Logger

	// Changed only by tests.
	now       func() time.Time
	publish   func(ctx context.Context, event nostr.Event) error
	sendFor   time.Duration
	sendRetry time.Duration

	methods     map[string]method
	connections map[string]connection // By service public key.

	mu   sync.Mutex
	conn *nostr.Relay // The current relay connection, or nil while there is none.
	work sync.WaitGroup
}

// New loads the connections. One added later is picked up at the next start.
func New(ctx context.Context, db *store.Store, n node.Node, relay string, log *slog.Logger) (*Service, error) {
	if err := CheckRelay(relay); err != nil {
		return nil, err
	}
	s := &Service{
		db: db.DB, node: n, relay: relay, log: log,
		now: time.Now, sendFor: sendFor, sendRetry: sendRetry,
		connections: map[string]connection{},
	}
	s.publish = s.publishToRelay
	s.methods = map[string]method{"get_info": s.getInfo}
	if err := s.load(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Service) load(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT id, client_pubkey, service_key, revoked_at IS NOT NULL FROM connections`)
	if err != nil {
		return fmt.Errorf("load connections: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var c connection
		if err := rows.Scan(&c.id, &c.clientPub, &c.serviceKey, &c.revoked); err != nil {
			return fmt.Errorf("load connections: %w", err)
		}
		if c.servicePub, err = nostr.GetPublicKey(c.serviceKey); err != nil {
			return fmt.Errorf("connection %d: bad service key: %w", c.id, err)
		}
		s.connections[c.servicePub] = c
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("load connections: %w", err)
	}
	return nil
}

// methodNames lists what this service answers, for clients to read.
func (s *Service) methodNames() []string {
	return slices.Sorted(maps.Keys(s.methods))
}

// since returns where a new subscription starts. The time comes from this
// machine's clock, never from a client, so a client's wrong clock cannot make the
// service skip requests. Old requests are refused anyway, so it looks back no
// further than maxAge.
func (s *Service) since(ctx context.Context) (nostr.Timestamp, error) {
	now := s.now()
	seen := now
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key = ?`, lastSeenKey).Scan(&value)
	switch {
	case err == nil:
		unix, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("read %s: %w", lastSeenKey, err)
		}
		if stored := time.Unix(unix, 0); stored.Before(now) {
			seen = stored
		}
	case !errors.Is(err, sql.ErrNoRows):
		return 0, fmt.Errorf("read %s: %w", lastSeenKey, err)
	}
	start := seen.Add(-sinceMargin)
	if oldest := now.Add(-maxAge); start.Before(oldest) {
		start = oldest
	}
	return nostr.Timestamp(start.Unix()), nil
}
