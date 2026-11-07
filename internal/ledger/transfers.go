package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/2rdzy/nwc4ln/internal/node"
)

// transferExpirySeconds is the life of a fund or withdraw invoice. Nobody is meant to
// pay it from outside. If its cancel fails, it stops being open after this.
const transferExpirySeconds = 600

// deletedSubWallets lists the accounts whose money the sweep moves out.
const deletedSubWallets = `(SELECT id FROM accounts WHERE type = 'sub_wallet' AND deleted_at IS NOT NULL)`

// transfer moves money between one sub-wallet and the main wallet. The main wallet
// side of each row has an empty account, and the two rows share a payment hash.
type transfer struct {
	accountID  int64
	amountMsat int64
	// toSubWallet is a fund. Otherwise it is a withdraw.
	toSubWallet bool
	// deleted allows an account that has been deleted. Only the sweep sets it.
	deleted     bool
	description string
}

// Fund moves money from the main wallet to a sub-wallet, up to the main wallet
// available (R15). Like Pay, an error means nothing moved and no hold exists. With no
// error, read State: a refused cancel gives failed. It needs a reachable node.
func (l *Ledger) Fund(ctx context.Context, accountID, amountMsat int64) (Transaction, error) {
	return l.transfer(ctx, transfer{
		accountID: accountID, amountMsat: amountMsat, toSubWallet: true, description: "Funded from the main wallet",
	})
}

// Withdraw moves money from a sub-wallet to the main wallet, up to its balance, which
// already leaves out what pending payments hold (R15). It reads no node figure, so it
// works during a shortfall. It has the same results as Fund.
func (l *Ledger) Withdraw(ctx context.Context, accountID, amountMsat int64) (Transaction, error) {
	return l.transfer(ctx, transfer{
		accountID: accountID, amountMsat: amountMsat, description: "Withdrawn to the main wallet",
	})
}

// transfer settles an ordinary invoice under R10, so there is no second money path.
func (l *Ledger) transfer(ctx context.Context, move transfer) (Transaction, error) {
	if move.amountMsat <= 0 || move.amountMsat > MaxAmountMsat {
		return Transaction{}, fmt.Errorf("%w: %d", ErrBadAmount, move.amountMsat)
	}
	run, err := l.begin()
	if err != nil {
		return Transaction{}, err
	}
	invoice, err := l.node.MakeInvoice(ctx, node.InvoiceRequest{
		AmountMsat: move.amountMsat, Description: move.description, ExpirySeconds: transferExpirySeconds,
	})
	if err != nil {
		l.work.Done()
		return Transaction{}, fmt.Errorf("make invoice: %w", err)
	}
	row, err := l.holdTransfer(ctx, move, invoice)
	if err != nil {
		l.work.Done()
		// The invoice has no row and nobody has seen it, so a failed cancel does no harm.
		if cancelErr := l.node.CancelInvoice(context.WithoutCancel(ctx), invoice.Hash); cancelErr != nil {
			l.log.Warn("cancel refused transfer", "hash", invoice.Hash, "error", cancelErr)
		}
		return Transaction{}, err
	}
	return l.carryOut(ctx, run, row, 0), nil
}

// holdTransfer checks a transfer and stores both of its sides as pending, under the
// one lock. The rows then follow the same path as an internal payment, in a crash too.
func (l *Ledger) holdTransfer(ctx context.Context, move transfer, invoice node.Invoice) (Transaction, error) {
	l.holdLock.Lock()
	defer l.holdLock.Unlock()
	var spendable node.Spendable
	if move.toSubWallet { // A withdraw takes nothing from the main wallet.
		var err error
		if spendable, err = l.readSpendable(ctx); err != nil {
			return Transaction{}, err
		}
	}
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return Transaction{}, err
	}
	defer func() { _ = tx.Rollback() }() // Does nothing after a commit.
	if err := checkTransferAccount(ctx, tx, move); err != nil {
		return Transaction{}, err
	}
	var fits int64
	if move.toSubWallet {
		fits, err = availableFrom(ctx, tx, spendable.ActiveMsat)
	} else {
		fits, err = subWalletBalance(ctx, tx, move.accountID)
	}
	if err != nil {
		return Transaction{}, err
	}
	if move.amountMsat > fits {
		return Transaction{}, ErrInsufficientBalance
	}
	payer, payee := move.accountID, int64(0)
	if move.toSubWallet {
		payer, payee = 0, move.accountID
	}
	row := Transaction{
		AccountID: payer, Type: TypeOutgoing, State: StatePending, AmountMsat: move.amountMsat,
		Invoice: invoice.PaymentRequest, Description: invoice.Description, PaymentHash: invoice.Hash, Internal: true,
		CreatedAt: l.now(), ExpiresAt: invoice.ExpiresAt,
	}
	incoming := row
	incoming.AccountID, incoming.Type, incoming.Preimage, incoming.Internal = payee, TypeIncoming, invoice.Preimage, false
	if _, err := insert(ctx, tx, incoming); err != nil {
		return Transaction{}, err
	}
	if row.ID, err = insert(ctx, tx, row); err != nil {
		return Transaction{}, err
	}
	return row, l.commitHold(tx, row)
}

// checkTransferAccount allows only a sub-wallet. A deleted one is allowed only for the
// sweep, which must still be able to empty it.
func checkTransferAccount(ctx context.Context, db querier, move transfer) error {
	var (
		subWallet bool
		deleted   sql.Null[int64]
	)
	err := db.QueryRowContext(ctx, `SELECT type = 'sub_wallet', deleted_at FROM accounts WHERE id = ?`,
		move.accountID).Scan(&subWallet, &deleted)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return ErrUnknownAccount
	case err != nil:
		return err
	case !subWallet:
		return ErrUnknownAccount
	case deleted.Valid && !move.deleted:
		return ErrAccountDeleted
	}
	return nil
}

// DeleteSubWallet revokes a sub-wallet's connections at once, and keeps its history
// (R16). It needs no node, so it works at any time. The sweep does the rest: see
// finishDeleted.
func (l *Ledger) DeleteSubWallet(ctx context.Context, accountID int64) error {
	return l.deleteAccount(ctx, accountID, "sub_wallet")
}

// finishDeleted is the part of a delete that needs the node. There is no list of work
// and no hook on a payment's end. The rows say what is left, so one path covers the
// balance at delete, what pending payments release later, income on an invoice that
// could not be canceled, and a crash at any point. A move that fails is tried again by
// the next sweep. The sweep is one goroutine, so two moves never race.
func (l *Ledger) finishDeleted(ctx context.Context) error {
	cancelErr := l.cancelDeleted(ctx)
	balances, err := deletedBalances(ctx, l.db)
	if err != nil {
		return errors.Join(cancelErr, err)
	}
	errs := []error{cancelErr}
	for _, balance := range balances {
		move := transfer{
			accountID: balance.accountID, amountMsat: balance.amountMsat, deleted: true,
			description: "Moved to the main wallet: sub-wallet deleted",
		}
		if _, err := l.transfer(ctx, move); err != nil {
			errs = append(errs, fmt.Errorf("move balance of deleted account %d: %w", balance.accountID, err))
		}
	}
	return errors.Join(errs...)
}

// cancelDeleted closes the open invoices of deleted sub-wallets. A row with a live
// internal payment is left alone, because that payment settles it. A cancel that
// succeeds means the invoice can never settle, so the row can fail. One that does not
// may mean the invoice is paid: the row stays pending, and the income is credited and
// then moved, so it is recorded.
func (l *Ledger) cancelDeleted(ctx context.Context) error {
	rows, err := listRows(ctx, l.db, `type = 'incoming' AND state = 'pending' AND account_id IN `+deletedSubWallets+
		` ORDER BY id`)
	if err != nil {
		return err
	}
	var errs []error
	for _, row := range rows {
		if l.isLive(row.PaymentHash) {
			continue
		}
		if err := l.node.CancelInvoice(ctx, row.PaymentHash); err != nil {
			errs = append(errs, fmt.Errorf("cancel invoice of deleted account %d: %w", row.AccountID, err))
			continue
		}
		if err := failRow(ctx, l.db, row.ID); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type accountBalance struct{ accountID, amountMsat int64 }

// deletedBalances finds deleted sub-wallets that still hold money that is free to move.
// What pending payments hold is left out by R3, and comes back here once released.
func deletedBalances(ctx context.Context, db querier) ([]accountBalance, error) {
	rows, err := db.QueryContext(ctx, `SELECT account_id, balance FROM (
		SELECT account_id, `+balanceSum+` AS balance FROM transactions
		WHERE account_id IN `+deletedSubWallets+` GROUP BY account_id)
		WHERE balance > 0 ORDER BY account_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var balances []accountBalance
	for rows.Next() {
		var balance accountBalance
		if err := rows.Scan(&balance.accountID, &balance.amountMsat); err != nil {
			return nil, err
		}
		balances = append(balances, balance)
	}
	return balances, rows.Err()
}

// readSpendable reads the node's figure for a check under holdLock, with a short
// timeout, because every payment waits for that lock.
func (l *Ledger) readSpendable(ctx context.Context) (node.Spendable, error) {
	read, cancel := context.WithTimeout(ctx, nodeReadTimeout)
	defer cancel()
	spendable, err := l.node.Spendable(read)
	if err != nil {
		return node.Spendable{}, fmt.Errorf("read spendable: %w", err)
	}
	if l.afterNodeRead != nil {
		l.afterNodeRead()
	}
	return spendable, nil
}
