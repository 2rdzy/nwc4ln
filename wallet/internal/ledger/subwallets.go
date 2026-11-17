package ledger

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// balanceSum is R3 over a set of rows: settled income, less what settled payments
// cost, less what pending payments hold.
const balanceSum = `sum(CASE
		WHEN type = 'incoming' AND state = 'settled' THEN amount_msat
		WHEN type = 'outgoing' AND state = 'settled' THEN -(amount_msat + fee_msat)
		WHEN type = 'outgoing' AND state = 'pending' THEN -(amount_msat + reserve_msat)
		ELSE 0 END)`

// SubWallet is one sub-wallet as the admin sees it.
type SubWallet struct {
	ID          int64
	Name        string
	BalanceMsat int64
	CreatedAt   time.Time
	LastUsedAt  time.Time // Zero if never used.
}

// isSubWallet reports the account's type. An unknown account reads as false, and
// the check of the account then refuses it.
func isSubWallet(ctx context.Context, db querier, accountID int64) (bool, error) {
	var subWallet bool
	err := db.QueryRowContext(ctx, `SELECT type = 'sub_wallet' FROM accounts WHERE id = ?`, accountID).Scan(&subWallet)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return subWallet, err
}

// subWalletBalance is R3 for one sub-wallet. To decide a payment, it must run inside
// the transaction that stores the hold, under holdLock, so that no row changes between
// the check and the insert.
func subWalletBalance(ctx context.Context, db querier, accountID int64) (int64, error) {
	var balance int64
	err := db.QueryRowContext(ctx, `SELECT coalesce(`+balanceSum+`, 0) FROM transactions WHERE account_id = ?`,
		accountID).Scan(&balance)
	return balance, err
}

// subWalletTotal is the money that belongs to sub-wallets, which R5 keeps away from
// apps. Each one counts for at least 0: a balance below 0 is a bug, and it must not
// free another holder's money. Deleted sub-wallets count too, so money that a delete
// failed to move stays locked rather than becoming spendable.
func subWalletTotal(ctx context.Context, db querier) (int64, error) {
	var total int64
	err := db.QueryRowContext(ctx, `SELECT coalesce(sum(max(balance, 0)), 0) FROM (
		SELECT `+balanceSum+` AS balance FROM transactions
		WHERE account_id IN (SELECT id FROM accounts WHERE type = 'sub_wallet')
		GROUP BY account_id)`).Scan(&total)
	return total, err
}

// Balance is what a client is told it holds: R3 for a sub-wallet, which needs no
// node, and AppBalance for an app. It can be below 0.
func (l *Ledger) Balance(ctx context.Context, accountID int64) (int64, error) {
	subWallet, err := isSubWallet(ctx, l.db, accountID)
	if err != nil {
		return 0, err
	}
	if subWallet {
		return subWalletBalance(ctx, l.db, accountID)
	}
	return l.AppBalance(ctx, accountID)
}

// SubWallets lists the sub-wallets that have not been deleted, oldest first.
func (l *Ledger) SubWallets(ctx context.Context) ([]SubWallet, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT a.id, a.name, a.created_at, max(c.last_used_at)
		FROM accounts a LEFT JOIN connections c ON c.account_id = a.id
		WHERE a.type = 'sub_wallet' AND a.deleted_at IS NULL GROUP BY a.id ORDER BY a.id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var wallets []SubWallet
	for rows.Next() {
		var (
			wallet  SubWallet
			created int64
			used    sql.Null[int64]
		)
		if err := rows.Scan(&wallet.ID, &wallet.Name, &created, &used); err != nil {
			return nil, err
		}
		wallet.CreatedAt, wallet.LastUsedAt = time.Unix(created, 0).UTC(), timeOf(used)
		wallets = append(wallets, wallet)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range wallets {
		if wallets[i].BalanceMsat, err = subWalletBalance(ctx, l.db, wallets[i].ID); err != nil {
			return nil, err
		}
	}
	return wallets, nil
}
