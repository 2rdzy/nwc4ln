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

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/store"
)

// Event kinds from NIP-47.
const (
	kindInfo     = 13194
	kindRequest  = 23194
	kindResponse = 23195
	// Notifications. Each one goes out as both, because a client listens to the
	// kind of the encryption it uses.
	kindNotification      = 23197
	kindNotificationNIP04 = 23196
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

	// notifyQueue is how many notifications can wait for the relay. Past that they
	// are dropped, because they are best effort and the ledger must never wait.
	notifyQueue = 64

	lastSeenKey = "nwc_last_seen"
)

// connection is what the service keeps in memory for one connection. Revoked
// connections are kept, so that their requests can be answered with
// UNAUTHORIZED. Whether one is revoked is read from the database per request.
type connection struct {
	id         int64
	accountID  int64
	clientPub  string
	serviceKey string
	servicePub string
	revoked    bool // At load. Only decides whether an info event is published.
}

// method answers one command. It returns a result, or an error for the client.
type method func(ctx context.Context, a *accepted, params json.RawMessage) (any, *Error)

// Service answers NWC requests from one relay.
type Service struct {
	db     *sql.DB
	node   node.Node
	ledger *ledger.Ledger
	relay  string
	log    *slog.Logger

	// Changed only by tests.
	now       func() time.Time
	publish   func(ctx context.Context, event nostr.Event) error
	sendFor   time.Duration
	sendRetry time.Duration
	payWait   time.Duration

	methods     map[string]method
	connections map[string]connection // By service public key.

	notifications chan ledger.Transaction

	mu   sync.Mutex
	conn *nostr.Relay // The current relay connection, or nil while there is none.
	work sync.WaitGroup
}

// New loads the connections. One added later is picked up at the next start. The
// ledger must have started before Run, so that no request is answered before
// recovery is done.
func New(
	ctx context.Context, db *store.Store, n node.Node, l *ledger.Ledger, relay string, log *slog.Logger,
) (*Service, error) {
	if err := CheckRelay(relay); err != nil {
		return nil, err
	}
	s := &Service{
		db: db.DB, node: n, ledger: l, relay: relay, log: log,
		now: time.Now, sendFor: sendFor, sendRetry: sendRetry, payWait: payWait,
		connections:   map[string]connection{},
		notifications: make(chan ledger.Transaction, notifyQueue),
	}
	s.publish = s.publishToRelay
	s.methods = map[string]method{
		"get_info":          s.getInfo,
		"get_balance":       s.getBalance,
		"pay_invoice":       s.payInvoice,
		"make_invoice":      s.makeInvoice,
		"lookup_invoice":    s.lookupInvoice,
		"list_transactions": s.listTransactions,
	}
	if err := s.load(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Service) load(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT id, account_id, client_pubkey, service_key, revoked_at IS NOT NULL
		FROM connections`)
	if err != nil {
		return fmt.Errorf("load connections: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var c connection
		if err := rows.Scan(&c.id, &c.accountID, &c.clientPub, &c.serviceKey, &c.revoked); err != nil {
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
