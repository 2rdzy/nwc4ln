package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/2rdzy/nwc4ln/internal/node"
)

// PayRequest asks for a payment from an account.
type PayRequest struct {
	AccountID int64
	Invoice   string
	// AmountMsat is only for an invoice with no amount. Otherwise leave it 0, or
	// give the invoice's own amount.
	AmountMsat int64
	// RequestID is the requests row, or 0.
	RequestID int64
}

// Pay pays an invoice and returns its transaction. An error means nothing was
// paid and no hold exists, so nothing after the hold returns one. With no error, the state says what happened: settled,
// failed, or pending if the node has no final answer yet or ctx ended first.
// Pending is not a failure: the payment may still settle, the hold stays, and
// the sweep finishes it. The end of ctx never stops the payment.
func (l *Ledger) Pay(ctx context.Context, request PayRequest) (Transaction, error) {
	invoice := strings.ToLower(request.Invoice)
	decoded, err := l.node.DecodeInvoice(ctx, invoice)
	if err != nil {
		return Transaction{}, fmt.Errorf("decode invoice: %w", err)
	}
	amount, err := payAmount(decoded, request.AmountMsat)
	if err != nil {
		return Transaction{}, err
	}
	if !l.now().Before(decoded.ExpiresAt) {
		return Transaction{}, ErrInvoiceExpired
	}

	l.mu.Lock()
	run := l.run
	if run == nil || run.Err() != nil {
		l.mu.Unlock()
		return Transaction{}, ErrNotRunning
	}
	done := make(chan struct{})
	l.work.Add(1)
	l.mu.Unlock()

	row, err := l.hold(ctx, request, invoice, decoded, amount)
	if err != nil {
		l.work.Done()
		return Transaction{}, err
	}
	nodeAmount := int64(0) // The node takes an amount only if the invoice has none.
	if decoded.AmountMsat == 0 {
		nodeAmount = amount
	}
	go func() {
		defer l.work.Done()
		defer close(done)
		l.send(run, row, nodeAmount)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
	tx, err := l.transaction(context.WithoutCancel(ctx), row.ID)
	if err != nil {
		// An error here would read as "not paid". Pending is the honest answer.
		l.log.Warn("read payment result", "hash", row.PaymentHash, "error", err)
		return row, nil
	}
	return tx, nil
}

func payAmount(decoded node.DecodedInvoice, requested int64) (int64, error) {
	amount := decoded.AmountMsat
	switch {
	case requested < 0:
		return 0, fmt.Errorf("%w: %d", ErrBadAmount, requested)
	case amount == 0:
		amount = requested
	case requested != 0 && requested != amount:
		return 0, fmt.Errorf("%w: %d differs from the invoice's %d", ErrBadAmount, requested, amount)
	}
	if amount <= 0 || amount > MaxAmountMsat {
		return 0, fmt.Errorf("%w: %d", ErrBadAmount, amount)
	}
	return amount, nil
}

// hold checks the payment and stores it as pending, under the one lock.
func (l *Ledger) hold(
	ctx context.Context, request PayRequest, invoice string, decoded node.DecodedInvoice, amount int64,
) (Transaction, error) {
	l.holdLock.Lock()
	defer l.holdLock.Unlock()
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return Transaction{}, err
	}
	defer func() { _ = tx.Rollback() }() // Does nothing after a commit.

	if err := checkAccount(ctx, tx, request.AccountID); err != nil {
		return Transaction{}, err
	}
	var taken int
	err = tx.QueryRowContext(ctx, `SELECT count(*) FROM transactions
		WHERE payment_hash = ? AND type = 'outgoing' AND state IN ('pending', 'settled')`, decoded.Hash).Scan(&taken)
	if err != nil {
		return Transaction{}, err
	}
	if taken > 0 {
		return Transaction{}, ErrAlreadyPaid
	}
	internal, err := isInternal(ctx, tx, invoice, decoded.Hash)
	if err != nil {
		return Transaction{}, err
	}
	reserve := int64(0)
	if !internal {
		if decoded.Destination == l.nodeKey() {
			return Transaction{}, ErrOwnInvoice
		}
		reserve = Reserve(amount)
	}
	row := Transaction{
		AccountID: request.AccountID, RequestID: request.RequestID, Type: TypeOutgoing, State: StatePending,
		AmountMsat: amount, ReserveMsat: reserve, Invoice: invoice, Description: decoded.Description,
		PaymentHash: decoded.Hash, Internal: internal, CreatedAt: l.now(), ExpiresAt: decoded.ExpiresAt,
	}
	if row.ID, err = insert(ctx, tx, row); err != nil {
		return Transaction{}, err
	}
	// Recorded before the commit, so that a sweep can never see this row without
	// knowing a node call is coming, and fail it on "no record".
	l.mu.Lock()
	l.attempts[row.ID] = &attempt{hash: row.PaymentHash, live: true}
	l.mu.Unlock()
	if err := tx.Commit(); err != nil {
		l.forget(row.ID)
		return Transaction{}, err
	}
	return row, nil
}

// isInternal reports whether this invoice was made here and can settle inside
// the ledger. The hash alone is not enough: a different invoice can carry the
// same hash, and that one goes out through the node.
func isInternal(ctx context.Context, tx *sql.Tx, invoice, hash string) (bool, error) {
	var state, own string
	err := tx.QueryRowContext(ctx, `SELECT state, coalesce(invoice, '') FROM transactions
		WHERE payment_hash = ? AND type = 'incoming'`, hash).Scan(&state, &own)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	case own != invoice:
		return false, nil
	case state != StatePending:
		return false, ErrInvoiceClosed
	}
	return true, nil
}

// send does the node's part of a held payment.
func (l *Ledger) send(ctx context.Context, row Transaction, nodeAmount int64) {
	if row.Internal {
		l.settleInternal(ctx, row)
		return
	}
	if l.beforePay != nil {
		l.beforePay()
	}
	call, cancel := context.WithTimeout(ctx, payTimeout+payCallMargin)
	defer cancel()
	payment, err := l.node.Pay(call, node.PayRequest{
		Invoice: row.Invoice, AmountMsat: nodeAmount, FeeLimitMsat: row.ReserveMsat, Timeout: payTimeout,
	})
	if err == nil && (payment.State == node.PaymentSucceeded || payment.State == node.PaymentFailed) {
		l.apply(ctx, row, payment) // The node's own answer to this call: always believed.
		return
	}
	// The error says nothing about the payment. Only the node's record does.
	l.log.Warn("no final answer from pay", "hash", row.PaymentHash, "error", err)
	l.distrust(row.ID)
	payment, err = l.node.TrackPayment(ctx, row.PaymentHash)
	if err != nil {
		l.log.Warn("track payment", "hash", row.PaymentHash, "error", err)
		return
	}
	l.resolve(ctx, row, payment)
}

func (l *Ledger) distrust(id int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if attempt, ok := l.attempts[id]; ok {
		attempt.live = false
		attempt.trustFrom = l.now().Add(noRecordGrace)
	}
}

func (l *Ledger) forget(id int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.attempts, id)
}

func (l *Ledger) nodeKey() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.pubKey
}

// trusted reports whether a non-success answer about this row can be believed. A
// row with no attempt was made before this process, or by one that just stopped,
// so a young row gets the same wait as a Pay error.
func (l *Ledger) trusted(row Transaction) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if attempt, ok := l.attempts[row.ID]; ok {
		return !attempt.live && !l.now().Before(attempt.trustFrom)
	}
	return !l.now().Before(row.CreatedAt.Add(noRecordGrace))
}

// resolve applies a node answer about a pending row, if it can be believed. The
// sweep and restart use it, and so does Pay after an error.
func (l *Ledger) resolve(ctx context.Context, row Transaction, payment node.Payment) {
	if payment.State == node.PaymentSucceeded || l.trusted(row) {
		l.apply(ctx, row, payment)
	}
}

// apply stores a node answer. Anything that is not final leaves the hold in place.
func (l *Ledger) apply(ctx context.Context, row Transaction, payment node.Payment) {
	var err error
	switch payment.State {
	case node.PaymentSucceeded:
		err = l.applySuccess(ctx, row, payment)
	case node.PaymentFailed, node.PaymentNoRecord:
		l.log.Info("payment failed", "hash", row.PaymentHash, "reason", payment.FailureReason)
		err = failRow(ctx, l.db, row.ID)
	default:
		return
	}
	if err != nil {
		l.log.Error("store payment result", "hash", row.PaymentHash, "error", err)
		return
	}
	l.forget(row.ID)
}

// applySuccess charges what the node really sent. It differs from the row only if
// the hash was paid by another tool. A row that has failed already is not changed,
// but this means money left the node with no record of it: see the Late success
// item in the project's list.
func (l *Ledger) applySuccess(ctx context.Context, row Transaction, payment node.Payment) error {
	amount := row.AmountMsat
	if payment.AmountMsat != 0 && payment.AmountMsat != amount {
		l.log.Error("node sent a different amount", "hash", row.PaymentHash, "row", amount, "node", payment.AmountMsat)
		amount = payment.AmountMsat
	}
	if payment.FeeMsat > row.ReserveMsat {
		l.log.Error("fee above the reserve", "hash", row.PaymentHash, "fee", payment.FeeMsat, "reserve", row.ReserveMsat)
	}
	moved, err := settleOutgoing(ctx, l.db, row.ID, amount, payment.FeeMsat, payment.Preimage, l.now())
	if err != nil || moved {
		return err
	}
	current, err := l.transaction(ctx, row.ID)
	if err != nil {
		return err
	}
	if current.State == StateFailed {
		l.log.Error("node paid a payment the ledger failed", "hash", row.PaymentHash, "amount", amount)
	}
	return nil
}

// trackPending asks the node about each pending payment. Reading the rows here,
// and not a list in memory, makes restart and the running sweep the same code. A
// row with a node call running is asked about too: resolve believes only success
// for it, because the node may not have seen the payment yet.
func (l *Ledger) trackPending(ctx context.Context) error {
	rows, err := pendingRows(ctx, l.db, TypeOutgoing)
	if err != nil {
		return err
	}
	var errs []error
	for _, row := range rows {
		payment, err := l.node.TrackPayment(ctx, row.PaymentHash)
		if err != nil {
			errs = append(errs, err) // The payment stays pending.
			continue
		}
		l.resolve(ctx, row, payment)
	}
	return errors.Join(errs...)
}

// settleInternal finishes a payment to an invoice made here. The order matters.
// A cancel that succeeds means the invoice can never settle at the node, so no
// outside payment can land on top of this one. If the cancel fails, the invoice
// may already be paid, so the payment is refused. No node payment exists for the
// row, so failing it is safe.
func (l *Ledger) settleInternal(ctx context.Context, row Transaction) {
	defer l.forget(row.ID)
	if err := l.node.CancelInvoice(ctx, row.PaymentHash); err != nil {
		l.log.Warn("internal payment refused", "hash", row.PaymentHash, "error", err)
		if err := failRow(ctx, l.db, row.ID); err != nil {
			l.log.Error("store payment result", "hash", row.PaymentHash, "error", err)
		}
		return
	}
	// If this fails, or the process stops, neither side changed: the row stays
	// pending, the node has no record of it, and it fails on the next check.
	if err := settleBothSides(ctx, l.db, row, l.now()); err != nil {
		l.log.Error("settle internal payment", "hash", row.PaymentHash, "error", err)
	}
}
