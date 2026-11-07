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
	"golang.org/x/time/rate"

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

	// A connection may send rateBurst requests at once, and rateRefill per second
	// after that. A person's wallet stays far below this.
	rateBurst  = 50
	rateRefill = 5

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
	// What the account allowed at load. They only decide what the info event lists:
	// each request reads the account again.
	canPay     bool
	canInvoice bool
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
	burst     int

	methods map[string]method

	connectionsMu sync.RWMutex
	connections   map[string]connection // By service public key.
	// changed tells the relay loop that connections changed. It holds one signal,
	// which is enough: the loop reads the map afresh.
	changed chan struct{}
	// lastInfo is when each connection's info event was last stamped, by connection
	// ID. Only the relay loop uses it.
	lastInfo map[int64]int64

	notifications chan ledger.Transaction

	limitersMu sync.Mutex
	limiters   map[int64]*rate.Limiter // By connection ID. Lost at a restart, which is fine.

	mu   sync.Mutex
	conn *nostr.Relay // The current relay connection, or nil while there is none.
	work sync.WaitGroup
}

// New loads the connections. The ledger must have started before Run, so that no request is answered before
// recovery is done.
func New(
	ctx context.Context, db *store.Store, n node.Node, l *ledger.Ledger, relay string, log *slog.Logger,
) (*Service, error) {
	if err := CheckRelay(relay); err != nil {
		return nil, err
	}
	s := &Service{
		db: db.DB, node: n, ledger: l, relay: relay, log: log,
		now: time.Now, sendFor: sendFor, sendRetry: sendRetry, payWait: payWait, burst: rateBurst,
		connections:   map[string]connection{},
		limiters:      map[int64]*rate.Limiter{},
		changed:       make(chan struct{}, 1),
		lastInfo:      map[int64]int64{},
		notifications: make(chan ledger.Transaction, notifyQueue),
	}
	s.publish = s.publishToRelay
	s.methods = map[string]method{
		"get_info":          s.getInfo,
		"get_balance":       s.getBalance,
		"get_budget":        s.getBudget,
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
	rows, err := s.db.QueryContext(ctx, `SELECT c.id, c.account_id, c.client_pubkey, c.service_key,
		c.revoked_at IS NOT NULL, a.can_pay, a.can_invoice
		FROM connections c JOIN accounts a ON a.id = c.account_id`)
	if err != nil {
		return fmt.Errorf("load connections: %w", err)
	}
	defer func() { _ = rows.Close() }()
	loaded := map[string]connection{}
	for rows.Next() {
		var c connection
		err := rows.Scan(&c.id, &c.accountID, &c.clientPub, &c.serviceKey, &c.revoked, &c.canPay, &c.canInvoice)
		if err != nil {
			return fmt.Errorf("load connections: %w", err)
		}
		if c.servicePub, err = nostr.GetPublicKey(c.serviceKey); err != nil {
			return fmt.Errorf("connection %d: bad service key: %w", c.id, err)
		}
		loaded[c.servicePub] = c
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("load connections: %w", err)
	}
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	s.connections = loaded
	return nil
}

// connectionList returns the connections as they are now.
func (s *Service) connectionList() []connection {
	s.connectionsMu.RLock()
	defer s.connectionsMu.RUnlock()
	return slices.Collect(maps.Values(s.connections))
}

// reload picks up a change to the connections, and tells the relay loop. The loop
// listens for new keys, and publishes the info events again.
func (s *Service) reload(ctx context.Context) error {
	if err := s.load(ctx); err != nil {
		return err
	}
	select {
	case s.changed <- struct{}{}:
	default:
	}
	return nil
}

// AddApp creates an app and starts serving it. See the AddApp function.
func (s *Service) AddApp(ctx context.Context, name string, settings ledger.AppSettings) (string, error) {
	connectionString, err := addAccount(ctx, s.db, "app", name, s.relay, settings)
	if err != nil {
		return "", err
	}
	return connectionString, s.reload(ctx)
}

// AddSubWallet creates a sub-wallet with a balance of 0 and starts serving it. The
// connection string is the only copy of the client secret, as with AddApp.
func (s *Service) AddSubWallet(ctx context.Context, name string) (string, error) {
	connectionString, err := addAccount(ctx, s.db, "sub_wallet", name, s.relay, subWalletSettings())
	if err != nil {
		return "", err
	}
	return connectionString, s.reload(ctx)
}

// EditApp changes an app's settings. They apply to the next request.
func (s *Service) EditApp(ctx context.Context, accountID int64, settings ledger.AppSettings) error {
	if err := s.ledger.EditApp(ctx, accountID, settings); err != nil {
		return err
	}
	return s.reload(ctx)
}

// DeleteApp revokes an app at once, and keeps its history.
func (s *Service) DeleteApp(ctx context.Context, accountID int64) error {
	if err := s.ledger.DeleteApp(ctx, accountID); err != nil {
		return err
	}
	return s.reload(ctx)
}

// allowed spends one of a connection's requests, and reports whether it had one.
func (s *Service) allowed(connectionID int64) bool {
	s.limitersMu.Lock()
	limiter, found := s.limiters[connectionID]
	if !found {
		limiter = rate.NewLimiter(rateRefill, s.burst)
		s.limiters[connectionID] = limiter
	}
	s.limitersMu.Unlock()
	return limiter.AllowN(s.now(), 1)
}

// permitted decides whether settings allow a method. A connection with pay off may
// be held by a public service, so it can read nothing about the account. Anything
// not named here needs pay, so that a new method starts out closed.
func permitted(method string, canPay, canInvoice bool) bool {
	switch method {
	case "get_info", "lookup_invoice":
		return true
	case "make_invoice":
		return canInvoice
	default:
		return canPay
	}
}

// methodNames lists what a connection may call, for clients to read.
func (s *Service) methodNames(canPay, canInvoice bool) []string {
	var names []string
	for _, name := range slices.Sorted(maps.Keys(s.methods)) {
		if permitted(name, canPay, canInvoice) {
			names = append(names, name)
		}
	}
	return names
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
