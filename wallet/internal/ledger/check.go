package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
)

// Mismatch is one place where the ledger and the node disagree.
type Mismatch struct {
	Hash    string // Payment hash, or the account for a balance problem.
	Problem string // Short code, such as "not_paid_at_node".
	Detail  string
}

func (m Mismatch) String() string { return fmt.Sprintf("%s %s: %s", m.Problem, m.Hash, m.Detail) }

// Check compares the finished rows created since a time with the node, and lists
// every mismatch. It only reads, from the database and the node, so it can run beside
// the service. Pending rows are left out: the sweep follows them. It fails if the node
// cannot be asked, or is not the ledger's node, rather than report a guess.
func (l *Ledger) Check(ctx context.Context, since time.Time) ([]Mismatch, error) {
	info, err := l.node.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("read node identity: %w", err)
	}
	var stored string
	if err := l.db.QueryRowContext(ctx, `SELECT coalesce((SELECT value FROM settings WHERE key = ?), '')`,
		nodeKeyKey).Scan(&stored); err != nil {
		return nil, err
	}
	if stored != "" && stored != info.PubKey {
		return nil, fmt.Errorf("%w: stored %s, node has %s", ErrNodeChanged, stored, info.PubKey)
	}
	rows, err := listRows(ctx, l.db, `created_at >= ? AND state IN ('settled', 'failed') ORDER BY id`, since.Unix())
	if err != nil {
		return nil, err
	}
	var found []Mismatch
	add := func(hash, problem, format string, args ...any) {
		found = append(found, Mismatch{Hash: hash, Problem: problem, Detail: fmt.Sprintf(format, args...)})
	}
	for _, row := range rows {
		var err error
		switch {
		case row.Type == TypeOutgoing && row.Internal:
			err = l.checkInternal(ctx, row, add)
		case row.Type == TypeOutgoing:
			err = l.checkPayment(ctx, row, add)
		case row.Type == TypeIncoming && row.State == StateSettled && !row.Internal:
			err = l.checkIncome(ctx, row, add)
		}
		if err != nil {
			return nil, err
		}
	}
	negative, err := l.db.QueryContext(ctx, `SELECT account_id, balance FROM (
		SELECT account_id, `+balanceSum+` AS balance FROM transactions
		WHERE account_id IN (SELECT id FROM accounts WHERE type = 'sub_wallet') GROUP BY account_id)
		WHERE balance < 0 ORDER BY account_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = negative.Close() }()
	for negative.Next() {
		var account, balance int64
		if err := negative.Scan(&account, &balance); err != nil {
			return nil, err
		}
		add(fmt.Sprintf("account %d", account), "negative_balance", "sub-wallet balance is %d msat", balance)
	}
	return found, negative.Err()
}

type addFunc func(hash, problem, format string, args ...any)

// checkPayment compares a payment that went to the node.
func (l *Ledger) checkPayment(ctx context.Context, row Transaction, add addFunc) error {
	payment, err := l.node.TrackPayment(ctx, row.PaymentHash)
	if err != nil {
		return fmt.Errorf("track payment %s: %w", row.PaymentHash, err)
	}
	if row.State == StateFailed {
		if payment.State != node.PaymentSucceeded {
			return nil
		}
		// A retry that settled explains the success.
		var retried int
		if err := l.db.QueryRowContext(ctx, `SELECT count(*) FROM transactions
			WHERE payment_hash = ? AND type = 'outgoing' AND state = 'settled'`, row.PaymentHash).Scan(&retried); err != nil {
			return err
		}
		if retried == 0 {
			add(row.PaymentHash, "paid_after_failed", "the ledger marked it failed, the node paid it")
		}
		return nil
	}
	switch {
	case payment.State != node.PaymentSucceeded:
		add(row.PaymentHash, "not_paid_at_node", "settled in the ledger, the node reports state %d", payment.State)
	case payment.AmountMsat != row.AmountMsat || payment.FeeMsat != row.FeeMsat:
		add(row.PaymentHash, "amount_differs", "ledger %d + fee %d msat, node %d + fee %d msat",
			row.AmountMsat, row.FeeMsat, payment.AmountMsat, payment.FeeMsat)
	}
	return nil
}

// checkIncome compares an invoice paid at the node.
func (l *Ledger) checkIncome(ctx context.Context, row Transaction, add addFunc) error {
	invoice, err := l.node.LookupInvoice(ctx, row.PaymentHash)
	switch {
	case errors.Is(err, node.ErrNotFound):
		add(row.PaymentHash, "invoice_missing", "credited in the ledger, unknown to the node")
		return nil
	case err != nil:
		return fmt.Errorf("lookup invoice %s: %w", row.PaymentHash, err)
	case invoice.State != node.InvoiceSettled:
		add(row.PaymentHash, "income_not_settled", "credited in the ledger, not settled at the node")
	case invoice.PaidMsat != row.AmountMsat:
		add(row.PaymentHash, "income_differs", "ledger %d msat, node %d msat", row.AmountMsat, invoice.PaidMsat)
	}
	return nil
}

// checkInternal checks a settled payment between accounts: both sides settled with one
// amount, and the invoice never paid at the node, which would credit it twice.
func (l *Ledger) checkInternal(ctx context.Context, row Transaction, add addFunc) error {
	if row.State != StateSettled {
		return nil
	}
	var state string
	var amount int64
	err := l.db.QueryRowContext(ctx, `SELECT state, amount_msat FROM transactions
		WHERE payment_hash = ? AND type = 'incoming'`, row.PaymentHash).Scan(&state, &amount)
	if err != nil {
		return fmt.Errorf("read the other side of %s: %w", row.PaymentHash, err)
	}
	if state != StateSettled || amount != row.AmountMsat {
		add(row.PaymentHash, "internal_unbalanced", "paid %d msat, credited %d msat (%s)", row.AmountMsat, amount, state)
	}
	invoice, err := l.node.LookupInvoice(ctx, row.PaymentHash)
	switch {
	case errors.Is(err, node.ErrNotFound):
	case err != nil:
		return fmt.Errorf("lookup invoice %s: %w", row.PaymentHash, err)
	case invoice.State == node.InvoiceSettled:
		add(row.PaymentHash, "internal_paid_at_node", "settled inside the ledger and also paid at the node")
	}
	return nil
}
