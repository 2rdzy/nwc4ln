// Package ledger holds the money rules. It is the only code that writes the
// transactions table, and every amount in it is an integer in millisatoshis.
//
// A hold is a pending outgoing row: amount plus reserve. Nothing here stores a
// balance. Later figures are computed from the rows.
//
// The rules that keep money safe:
//
//   - A row leaves pending once. Each change is an update that only matches a
//     pending row, so running it twice does nothing.
//   - A row turns failed only when the node says so, or when no node payment can
//     exist. A timeout or a lost connection leaves it pending, and the hold stays.
//   - The database is the queue of work. After a crash, Start finds everything
//     that was unfinished in the rows.
package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/store"
)

const (
	// MaxAmountMsat is all the bitcoin there will ever be. Refusing anything above
	// it keeps amount plus reserve from overflowing. A sum of rows stays safe because
	// rows hold real money, not because of this cap.
	MaxAmountMsat = 21_000_000 * 100_000_000 * 1_000

	minReserveMsat = 10_000

	// payTimeout is how long the node keeps trying new routes. It is below the
	// 60 seconds a client waits, so a payment with no route fails in that time.
	payTimeout = 50 * time.Second
	// payCallMargin ends the wait for the node's answer after payTimeout. The
	// payment can outlive this, and the sweep then follows it.
	payCallMargin = 5 * time.Second

	// noRecordGrace is how long only success is believed, after a Pay error or
	// after a row is made. The request may still be on its way, so "no record" may
	// not be the last word. The second case covers a fast restart.
	noRecordGrace = 2 * time.Minute

	sweepEvery   = 5 * time.Second
	retryEvery   = 5 * time.Second
	catchUpEvery = 10 * time.Minute
)

// Errors a caller can act on. Each means no row was written.
var (
	ErrNotRunning     = errors.New("ledger is not running")
	ErrUnknownAccount = errors.New("unknown account")
	ErrAccountDeleted = errors.New("account is deleted")
	ErrBadAmount      = errors.New("bad amount")
	ErrInvoiceExpired = errors.New("invoice has expired")
	// ErrAlreadyPaid means a pending or settled payment exists for the hash.
	ErrAlreadyPaid = errors.New("invoice is already paid or being paid")
	// ErrInvoiceClosed means the invoice is from this node and can no longer be paid.
	ErrInvoiceClosed = errors.New("invoice is closed")
	// ErrOwnInvoice means the invoice is from this node but not in the ledger. The
	// node refuses self-payments, so asking it would only hold funds for nothing.
	ErrOwnInvoice = errors.New("invoice is from this node and not in the ledger")
	// ErrNodeChanged means the node is not the one this ledger belongs to. Its "no
	// record" answers mean nothing, and believing them would release every hold.
	ErrNodeChanged = errors.New("node public key differs from the one stored in the ledger")
)

// Reserve returns the fee reserve for an amount: 1% rounded up, with a 10 sat
// minimum. It is also the fee limit given to the node, so that the real fee can
// never exceed the hold.
func Reserve(amountMsat int64) int64 {
	reserve := amountMsat / 100
	if amountMsat%100 != 0 {
		reserve++
	}
	return max(reserve, minReserveMsat)
}

// Ledger applies the money rules for one node.
type Ledger struct {
	db   *sql.DB
	node node.Node
	log  *slog.Logger

	// Changed only by tests.
	now          func() time.Time
	sweepEvery   time.Duration
	retryEvery   time.Duration
	catchUpEvery time.Duration
	beforePay    func() // Runs after the hold is stored, before the node call.

	// holdLock covers the check and the insert of a payment, so that two payments
	// at once cannot both pass. Payments to the node run outside it.
	holdLock sync.Mutex

	mu       sync.Mutex
	run      context.Context // Set while running. Node calls for payments use it.
	stop     context.CancelFunc
	attempts map[int64]*attempt
	work     sync.WaitGroup
	pubKey   string    // The node's key, set by Start.
	nextPass time.Time // When a sweep next checks pending invoices.
}

// attempt is what this process knows about a payment it started. A row with no
// attempt was started before this process, or has been decided.
type attempt struct {
	hash string
	// live means a node call for the row is running, so the node may not have
	// seen the payment yet.
	live bool
	// trustFrom is when an answer other than success can be believed.
	trustFrom time.Time
}

// New returns a ledger. Call Start before using it.
func New(db *store.Store, n node.Node, log *slog.Logger) *Ledger {
	return &Ledger{
		db: db.DB, node: n, log: log,
		now: time.Now, sweepEvery: sweepEvery, retryEvery: retryEvery, catchUpEvery: catchUpEvery,
		attempts: map[int64]*attempt{},
	}
}

// Start recovers from any earlier stop, then starts following the node. It
// returns only when recovery is done, so that nothing is answered before it. If
// the node cannot be reached it returns an error, with nothing released, and
// can be called again.
func (l *Ledger) Start(ctx context.Context) error {
	if err := l.checkNode(ctx); err != nil {
		return err
	}
	if err := l.catchUp(ctx); err != nil {
		return fmt.Errorf("catch up on invoices: %w", err)
	}
	if err := l.trackPending(ctx); err != nil {
		return fmt.Errorf("check pending payments: %w", err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.run != nil {
		return errors.New("ledger already started")
	}
	l.run, l.stop = context.WithCancel(context.Background())
	l.nextPass = l.now().Add(l.catchUpEvery)
	l.work.Go(func() { l.subscribe(l.run) })
	l.work.Go(func() { l.sweepLoop(l.run) })
	return nil
}

// checkNode ties the ledger to one node. The first start stores the node's key,
// and every later start must see the same key.
func (l *Ledger) checkNode(ctx context.Context) error {
	info, err := l.node.Info(ctx)
	if err != nil {
		return fmt.Errorf("read node identity: %w", err)
	}
	stored, err := bindNodeKey(ctx, l.db, info.PubKey)
	if err != nil {
		return fmt.Errorf("store node identity: %w", err)
	}
	if stored != info.PubKey {
		return fmt.Errorf("%w: stored %s, node has %s", ErrNodeChanged, stored, info.PubKey)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pubKey = info.PubKey
	return nil
}

// Close stops following the node and waits for running calls to end. Payments in
// flight stay pending, and the next Start picks them up.
func (l *Ledger) Close() {
	l.mu.Lock()
	if l.stop != nil { // Under the lock, so that Pay's check and Add stay before Wait.
		l.stop()
	}
	l.mu.Unlock()
	l.work.Wait()
}

func (l *Ledger) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(l.sweepEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := l.sweep(ctx); err != nil && ctx.Err() == nil {
			l.log.Warn("ledger sweep", "error", err)
		}
	}
}

// sweep is one pass over the unfinished work. See catchUp for why invoices are
// checked while the settle index is 0. After that they are checked now and then,
// which closes rows the stream never reports: expired, canceled or removed.
func (l *Ledger) sweep(ctx context.Context) error {
	index, err := settleIndex(ctx, l.db)
	if err != nil {
		return err
	}
	var catchUpErr error
	if index == 0 || l.passDue() {
		catchUpErr = l.catchUp(ctx)
	}
	return errors.Join(catchUpErr, l.trackPending(ctx))
}

func (l *Ledger) passDue() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if now.Before(l.nextPass) {
		return false
	}
	l.nextPass = now.Add(l.catchUpEvery)
	return true
}

// isLive reports whether a node call is running for this hash.
func (l *Ledger) isLive(hash string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, attempt := range l.attempts {
		if attempt.hash == hash && attempt.live {
			return true
		}
	}
	return false
}

// checkAccount decides whether an account may use the ledger. Limits and
// balances will be added here.
func checkAccount(ctx context.Context, tx *sql.Tx, accountID int64) error {
	var deletedAt sql.NullInt64
	err := tx.QueryRowContext(ctx, "SELECT deleted_at FROM accounts WHERE id = ?", accountID).Scan(&deletedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return ErrUnknownAccount
	case err != nil:
		return err
	case deletedAt.Valid:
		return ErrAccountDeleted
	}
	return nil
}
