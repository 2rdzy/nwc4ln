package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
)

// InvoiceRequest asks for an invoice for an account.
type InvoiceRequest struct {
	AccountID  int64
	AmountMsat int64
	// Description goes in the invoice, unless DescriptionHash is set.
	Description     string
	DescriptionHash string
	// ExpirySeconds of 0 uses the node's default.
	ExpirySeconds int64
	// RequestID is the requests row, or 0.
	RequestID int64
	// ConnectionID is the connection that asked. It must be a connection of the
	// account, not revoked, when the row is stored: a connection replaced while the
	// node makes the invoice must not get it. Any role may ask. 0 is refused.
	ConnectionID int64
}

// MakeInvoice creates an invoice at the node and records it in the account's
// history. If the process stops between the two, the invoice has no row, and any
// income on it counts as main wallet.
//
// The checks run before the node call, so that a refused request costs no node
// work, and again in the write transaction that stores the row, so that a
// connection replaced or a cap reached during the call is seen. An invoice refused
// at that point stays at the node, is shown to nobody, and expires.
func (l *Ledger) MakeInvoice(ctx context.Context, request InvoiceRequest) (Transaction, error) {
	if request.AmountMsat <= 0 || request.AmountMsat > MaxAmountMsat {
		return Transaction{}, fmt.Errorf("%w: %d", ErrBadAmount, request.AmountMsat)
	}
	if len(request.Description) > MaxDescriptionBytes {
		return Transaction{}, ErrDescriptionTooLong
	}
	// Compare seconds with seconds: a large number of seconds wraps when turned into
	// a time.Duration, and could land back inside the range.
	if seconds := request.ExpirySeconds; seconds != 0 &&
		(seconds < int64(minInvoiceExpiry/time.Second) || seconds > int64(maxInvoiceExpiry/time.Second)) {
		return Transaction{}, ErrBadExpiry
	}
	if err := l.checkInvoice(ctx, l.db, request); err != nil {
		return Transaction{}, err
	}
	invoice, err := l.node.MakeInvoice(ctx, node.InvoiceRequest{
		AmountMsat: request.AmountMsat, Description: request.Description,
		DescriptionHash: request.DescriptionHash, ExpirySeconds: request.ExpirySeconds,
	})
	if err != nil {
		return Transaction{}, fmt.Errorf("make invoice: %w", err)
	}
	row := Transaction{
		AccountID: request.AccountID, RequestID: request.RequestID, Type: TypeIncoming, State: StatePending,
		AmountMsat: request.AmountMsat, Invoice: invoice.PaymentRequest, Description: invoice.Description,
		PaymentHash: invoice.Hash, Preimage: invoice.Preimage, CreatedAt: l.now(), ExpiresAt: invoice.ExpiresAt,
	}
	// Write transactions take the lock when they begin, so a replace of the
	// connection, or another invoice, is either seen here or comes after this row.
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return Transaction{}, err
	}
	defer func() { _ = tx.Rollback() }() // Does nothing after a commit.
	if err := l.checkInvoice(ctx, tx, request); err != nil {
		return Transaction{}, err
	}
	if row.ID, err = insert(ctx, tx, row); err != nil {
		return Transaction{}, fmt.Errorf("store invoice: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Transaction{}, fmt.Errorf("store invoice: %w", err)
	}
	return row, nil
}

// checkInvoice decides whether an account may get a new invoice now, through
// this connection.
func (l *Ledger) checkInvoice(ctx context.Context, db querier, request InvoiceRequest) error {
	settings, err := l.checkAccount(ctx, db, request.AccountID)
	if err != nil {
		return err
	}
	if !settings.CanInvoice {
		return ErrRestricted
	}
	if err := checkAnyConnection(ctx, db, request.ConnectionID, request.AccountID); err != nil {
		return err
	}
	return l.checkOpenInvoices(ctx, db, request.AccountID)
}

// checkAnyConnection refuses a connection that is revoked or not the account's.
// Unlike checkConnection, any role passes: a receive-only connection may make
// invoices.
func checkAnyConnection(ctx context.Context, db querier, connectionID, accountID int64) error {
	var usable bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM connections
		WHERE id = ? AND account_id = ? AND revoked_at IS NULL)`, connectionID, accountID).Scan(&usable)
	if err != nil {
		return err
	}
	if !usable {
		return ErrConnectionRevoked
	}
	return nil
}

// checkOpenInvoices refuses a new invoice if the account has too many open, or made
// too many in the last 24 hours. An expired invoice is not open, even while its row
// is pending, so the daily count is what bounds invoices with a short expiry.
func (l *Ledger) checkOpenInvoices(ctx context.Context, db querier, accountID int64) error {
	now := l.now()
	var open, today int
	err := db.QueryRowContext(ctx, `SELECT
			count(CASE WHEN state = 'pending' AND expires_at > ? THEN 1 END),
			count(CASE WHEN created_at > ? THEN 1 END)
		FROM transactions WHERE account_id = ? AND type = 'incoming'`,
		now.Unix(), now.Add(-24*time.Hour).Unix(), accountID).Scan(&open, &today)
	if err != nil {
		return err
	}
	if open >= l.maxOpenInvoices || today >= l.maxDailyInvoices {
		return ErrTooManyInvoices
	}
	return nil
}

// catchUp asks the node about every pending invoice. It works the same at any
// settle index, and it ends when it is done, which the subscription never says.
// It never writes the index: see onSettled.
//
// It runs at each start, and now and then on the sweep. It also runs on each sweep
// while the index is 0, because the subscription replays nothing from 0: an invoice
// that settles after a pass and before the stream opens would otherwise never be seen.
//
// budget bounds a pass on the sweep, which then carries on where it stopped: see
// budgeted. 0 means no limit, as at the start.
func (l *Ledger) catchUp(ctx context.Context, budget time.Duration) error {
	rows, err := pendingRows(ctx, l.db, TypeIncoming)
	if err != nil {
		return err
	}
	rows, next := l.budgeted(rows, &l.incomingAfter, budget)
	var errs []error
	for _, row := range rows {
		if !next(row) {
			break
		}
		invoice, err := l.node.LookupInvoice(ctx, row.PaymentHash)
		switch {
		case errors.Is(err, node.ErrNotFound):
			// The node removes canceled invoices if told to, but never settled ones.
			// checkNode has shown this is the right node, so this one cannot settle.
			l.log.Warn("pending invoice not found on the node", "hash", row.PaymentHash)
			if !l.isLive(row.PaymentHash) {
				err = failRow(ctx, l.db, row.ID)
			} else {
				err = nil
			}
			if err != nil {
				errs = append(errs, err)
			}
			continue
		case err != nil:
			errs = append(errs, err)
			continue
		}
		switch invoice.State {
		case node.InvoiceSettled:
			err = l.credit(ctx, invoice, false)
		case node.InvoiceCanceled:
			// Checked after the lookup, on purpose. An internal payment is live
			// before it cancels, and it settles this row itself.
			if !l.isLive(row.PaymentHash) {
				err = failRow(ctx, l.db, row.ID)
			}
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// subscribe follows settled invoices for as long as ctx lasts, reconnecting from
// the stored index.
func (l *Ledger) subscribe(ctx context.Context) {
	for {
		index, err := settleIndex(ctx, l.db)
		if err == nil {
			err = l.node.SubscribeInvoices(ctx, index, func(invoice node.Invoice) error {
				return l.onSettled(ctx, invoice)
			})
		}
		if ctx.Err() != nil {
			return
		}
		l.log.Warn("invoice stream ended", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(l.retryEvery):
		}
	}
}

// onSettled credits one invoice from the stream, and stores its index in the
// same database transaction. If this fails, the stream stops and the invoice is
// sent again.
//
// Before the first index is stored, catchUp runs once more. The stream is open at
// that point, so anything it missed has settled already and catchUp finds it.
// Once an index is stored there is no other way to find a missed invoice. If this
// catchUp fails, or the process stops, nothing is stored and the next try repeats it.
func (l *Ledger) onSettled(ctx context.Context, invoice node.Invoice) error {
	index, err := settleIndex(ctx, l.db)
	if err != nil {
		return err
	}
	if index == 0 {
		if err := l.catchUp(ctx, 0); err != nil {
			return err
		}
	}
	return l.credit(ctx, invoice, true)
}

// credit marks an invoice settled. Only a pending row changes, so an invoice
// sent twice credits once. An invoice with no row was not made here, and counts
// as main wallet by itself.
func (l *Ledger) credit(ctx context.Context, invoice node.Invoice, storeIndex bool) error {
	l.holdLock.Lock()
	credited, err := l.storeCredit(ctx, invoice, storeIndex)
	l.holdLock.Unlock()
	if err != nil {
		return err
	}
	if credited {
		l.settledInvoice(ctx, invoice.Hash)
	}
	return nil
}

// storeCredit is the database part of credit. It reports whether a row changed.
func (l *Ledger) storeCredit(ctx context.Context, invoice node.Invoice, storeIndex bool) (bool, error) {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }() // Does nothing after a commit.
	settledAt := invoice.SettledAt
	if settledAt.IsZero() {
		settledAt = l.now()
	}
	credited, err := settleIncoming(ctx, tx, invoice, settledAt)
	if err != nil {
		return false, err
	}
	if !credited {
		// Settled inside the ledger or failed, and yet paid at the node. The money
		// is in the node and falls into the main wallet.
		var count int
		err := tx.QueryRowContext(ctx, `SELECT count(*) FROM transactions
			WHERE payment_hash = ? AND type = 'incoming' AND (internal = 1 OR state = 'failed')`,
			invoice.Hash).Scan(&count)
		if err != nil {
			return false, err
		}
		if count > 0 {
			l.raise(ctx, tx, AlertIncomeOnClosed, invoice.Hash, fmt.Sprintf(
				"%d msat arrived at the node on %s, an invoice already closed in the ledger. It counts as main wallet.",
				invoice.PaidMsat, invoice.Hash))
		}
	}
	if storeIndex {
		if err := storeSettleIndex(ctx, tx, invoice.SettleIndex); err != nil {
			return false, err
		}
	}
	return credited, tx.Commit()
}
