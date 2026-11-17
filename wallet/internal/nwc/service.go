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

	"github.com/2rdzy/nwc4ln/wallet/internal/ledger"
	"github.com/2rdzy/nwc4ln/wallet/internal/node"
	"github.com/2rdzy/nwc4ln/wallet/internal/store"
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
	// maxPayAge is the limit for a pay_invoice with no expiration tag: about how long a
	// client waits for the answer.
	maxPayAge = time.Minute
)

// How the service catches up, and how long an answer waits for the relay.
const (
	// sinceMargin is how far before the last seen request to subscribe from.
	sinceMargin = 5 * time.Minute

	backoffFirst = time.Second
	backoffMax   = 30 * time.Second

	// overlapAfterReady is how long the old subscription is still read after the relay
	// confirms the new one, for events it received just before. overlapLimit closes it
	// if no confirmation comes.
	overlapAfterReady = 2 * time.Second
	overlapLimit      = 10 * time.Second

	sendFor       = 30 * time.Second
	sendRetry     = 2 * time.Second
	publishWithin = 7 * time.Second

	// notifyQueue is how many notifications can wait for the relay. Past that they
	// are dropped, because they are best effort and the ledger must never wait.
	notifyQueue = 64

	// Replies wait in a queue of publishQueue for one of publishers, so that a relay
	// that acknowledges nothing holds up the queue, never the requests. Past that a
	// reply is dropped: the client can use lookup_invoice.
	publishQueue = 256
	publishers   = 4

	// maxRunning requests run at once, and at most maxRunningEach per connection, so
	// that one holder's slow payments cannot take every place: rateBurst is 50 and a
	// payment can wait payWait. A wallet sends three or four at once when it connects.
	// A request past either counts as over the rate.
	maxRunning     = 64
	maxRunningEach = 8

	// RATE_LIMITED and UNAUTHORIZED replies have their own budget: replyBudget at
	// once, and replyRate a second. Past that the event is dropped before it is
	// decrypted, so a flood costs no keys, encryption or signatures.
	replyBudget = 8
	replyRate   = 10

	// Limits on a request, checked before any hash. An NWC request with a long invoice
	// is about 10 KB. Larger events are dropped, at debug level like any other.
	maxContent  = 64 << 10
	maxTags     = 16
	maxTagItems = 8
	maxTagItem  = 1 << 10

	// Relay notices are logged at most noticeBurst at once, then one every
	// noticeEvery, because a relay can send any number.
	noticeBurst = 5
	noticeEvery = 10 * time.Second

	// A connection may send rateBurst requests at once, and rateRefill per second
	// after that. A person's wallet stays far below this.
	rateBurst  = 50
	rateRefill = 5

	// make_invoice has its own, lower rate: invoiceBurst at once, then one every
	// invoiceEvery. A person makes a few a day.
	invoiceBurst = 10
	invoiceEvery = 6 * time.Second

	// refusalEvery is how often a revoked, deleted or expired connection hears
	// UNAUTHORIZED. revokedKeep is how long after a revoke the service still listens
	// for the key at all.
	refusalEvery = time.Minute
	revokedKeep  = 30 * 24 * time.Hour

	// Requests rows are kept pruneAfter, then removed by a pass every pruneEvery. Only
	// the unique event ID matters, and pay_invoice and make_invoice are refused after
	// maxAge anyway. A row a transaction refers to is kept.
	pruneAfter = 48 * time.Hour
	pruneEvery = 24 * time.Hour

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
	now          func() time.Time
	publish      func(ctx context.Context, event nostr.Event) error
	sendFor      time.Duration
	sendRetry    time.Duration
	payWait      time.Duration
	burst        int
	invoiceBurst int

	// noNIP04 refuses NIP-04 requests and sends no NIP-04 notifications. It is set
	// before Run and never changes after.
	noNIP04 bool

	methods map[string]method

	connectionsMu sync.RWMutex
	connections   map[string]connection // By service public key.
	// changed tells the relay loop that connections changed. It holds one signal,
	// which is enough: the loop reads the map afresh.
	changed chan struct{}
	// lastInfo is when each connection's info event was last stamped, by connection
	// ID. Only the info loop uses it.
	lastInfo map[int64]int64

	notifications chan ledger.Transaction

	// outbox holds replies until a publisher sends them. infoNeeded asks the info loop
	// to publish every info event; it holds one signal, like changed.
	outbox     chan outgoing
	infoNeeded chan struct{}

	// running counts the requests that run now, in all and per connection ID.
	runningMu      sync.Mutex
	running        int
	runningBy      map[int64]int
	maxRunning     int // The constants of the same names. Changed only by tests.
	maxRunningEach int

	// replySlots and replyLimiter are the budget of RATE_LIMITED and UNAUTHORIZED replies.
	replySlots   chan struct{}
	replyLimiter *rate.Limiter

	notices, dropped *quiet // Relay notices, and replies dropped on a full outbox.

	limitersMu      sync.Mutex
	limiters        map[int64]*rate.Limiter // By connection ID. Lost at a restart, which is fine.
	invoiceLimiters map[int64]*rate.Limiter // make_invoice, by connection ID.
	refusals        map[int64]*rate.Limiter // UNAUTHORIZED replies, by connection ID.

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
		invoiceBurst:    invoiceBurst,
		connections:     map[string]connection{},
		limiters:        map[int64]*rate.Limiter{},
		invoiceLimiters: map[int64]*rate.Limiter{},
		refusals:        map[int64]*rate.Limiter{},
		changed:         make(chan struct{}, 1),
		lastInfo:        map[int64]int64{},
		notifications:   make(chan ledger.Transaction, notifyQueue),
		outbox:          make(chan outgoing, publishQueue),
		infoNeeded:      make(chan struct{}, 1),
		runningBy:       map[int64]int{},
		maxRunning:      maxRunning,
		maxRunningEach:  maxRunningEach,
		replySlots:      make(chan struct{}, replyBudget),
		replyLimiter:    rate.NewLimiter(replyRate, replyRate),
		notices:         newQuiet(noticeEvery, noticeBurst),
		dropped:         newQuiet(noticeEvery, noticeBurst),
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

// DisableNIP04 turns the old encryption off: the info event offers NIP-44 only,
// a NIP-04 request, or one with no encryption tag, gets UNSUPPORTED_ENCRYPTION,
// and no kind 23196 notification is sent. Call it before Run.
func (s *Service) DisableNIP04() { s.noNIP04 = true }

// encryptionTag lists the schemes we accept, preferred first.
func (s *Service) encryptionTag() string {
	if s.noNIP04 {
		return schemeNIP44
	}
	return schemeNIP44 + " " + schemeNIP04
}

func (s *Service) load(ctx context.Context) error {
	// A key revoked longer than revokedKeep ago is left out, so its client hears nothing.
	rows, err := s.db.QueryContext(ctx, `SELECT c.id, c.account_id, c.client_pubkey, c.service_key,
		c.revoked_at IS NOT NULL, a.can_pay, a.can_invoice
		FROM connections c JOIN accounts a ON a.id = c.account_id
		WHERE c.revoked_at IS NULL OR c.revoked_at >= ?`, s.now().Add(-revokedKeep).Unix())
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

// DeleteSubWallet revokes a sub-wallet at once, and keeps its history. The ledger's
// sweep then moves its balance to the main wallet.
func (s *Service) DeleteSubWallet(ctx context.Context, accountID int64) error {
	if err := s.ledger.DeleteSubWallet(ctx, accountID); err != nil {
		return err
	}
	return s.reload(ctx)
}

// ReplaceConnection gives an app or a sub-wallet a new connection string and kills the
// old one, as after a leak. Settings, usage, balance and history stay. The string is the only copy of the client secret, as
// with AddApp.
func (s *Service) ReplaceConnection(ctx context.Context, accountID int64) (string, error) {
	connectionString, err := replaceConnection(ctx, s.db, accountID, s.relay)
	if err != nil {
		return "", err
	}
	return connectionString, s.reload(ctx)
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

// reserve takes a place for one running request of a connection, and reports
// whether there was one. Each place taken is given back by release.
func (s *Service) reserve(connectionID int64) bool {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()
	if s.running >= s.maxRunning || s.runningBy[connectionID] >= s.maxRunningEach {
		return false
	}
	s.running++
	s.runningBy[connectionID]++
	return true
}

func (s *Service) release(connectionID int64) {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()
	s.running--
	if s.runningBy[connectionID]--; s.runningBy[connectionID] <= 0 {
		delete(s.runningBy, connectionID)
	}
}

// takeReply takes a place in the budget of refusal replies, and reports whether
// there was one. Each place taken is given back by releaseReply.
func (s *Service) takeReply() bool {
	select {
	case s.replySlots <- struct{}{}:
	default:
		return false
	}
	if !s.replyLimiter.AllowN(s.now(), 1) {
		<-s.replySlots
		return false
	}
	return true
}

func (s *Service) releaseReply() { <-s.replySlots }

// quiet limits how often one kind of message is logged, and counts the rest, so
// that the next line logged can say how many were left out.
type quiet struct {
	mu      sync.Mutex
	limiter *rate.Limiter
	skipped int
}

func newQuiet(every time.Duration, burst int) *quiet {
	return &quiet{limiter: rate.NewLimiter(rate.Every(every), burst)}
}

// allow reports whether to log now, and how many were left out before this one.
func (q *quiet) allow(now time.Time) (bool, int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.limiter.AllowN(now, 1) {
		q.skipped++
		return false, 0
	}
	skipped := q.skipped
	q.skipped = 0
	return true, skipped
}

// take spends one event from a connection's limiter in limiters, made on first use
// with one event every interval and the burst.
func (s *Service) take(limiters map[int64]*rate.Limiter, connectionID int64, every time.Duration, burst int) bool {
	s.limitersMu.Lock()
	limiter, found := limiters[connectionID]
	if !found {
		limiter = rate.NewLimiter(rate.Every(every), burst)
		limiters[connectionID] = limiter
	}
	s.limitersMu.Unlock()
	return limiter.AllowN(s.now(), 1)
}

// prune removes requests rows older than pruneAfter that no transaction refers to.
// Payments and invoices live in transactions, which this never touches.
func (s *Service) prune(ctx context.Context) (int64, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM requests WHERE created_at < ? AND id NOT IN
		(SELECT request_id FROM transactions WHERE request_id IS NOT NULL)`, s.now().Add(-pruneAfter).Unix())
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// maintain prunes at start and then every pruneEvery. Each later pass also reloads
// the connections, so that a key revoked longer than revokedKeep ago is dropped.
func (s *Service) maintain(ctx context.Context) {
	ticker := time.NewTicker(pruneEvery)
	defer ticker.Stop()
	for first := true; ; first = false {
		if removed, err := s.prune(ctx); err != nil {
			s.log.Warn("prune requests", "error", err)
		} else if removed > 0 {
			s.log.Info("pruned old requests", "rows", removed)
		}
		if !first {
			if err := s.reload(ctx); err != nil {
				s.log.Warn("reload connections", "error", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
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

// readScope is the one rule for what a connection may be told about the rows of
// its account, in replies and in notifications alike. With pay on it may read every
// row. With pay off it may read only the incoming rows it made itself, because a
// public service may hold such a key (owner, RV5). It returns the connection to
// limit reads to, or 0 for no limit, as Ledger.Lookup takes it.
func readScope(settings ledger.AppSettings, c connection) int64 {
	if settings.CanPay {
		return 0
	}
	return c.id
}

// notificationNames lists what a connection may be told about, for clients to
// read. It follows readScope: with pay off a connection gets no payment_sent.
func notificationNames(canPay bool) []string {
	if canPay {
		return notificationTypes
	}
	return []string{"payment_received"}
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
