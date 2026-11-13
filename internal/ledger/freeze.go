package ledger

import (
	"context"
	"database/sql"
	"errors"
)

// pausedKey is the settings row that pauses sub-wallet spending (R14). Its presence
// is the pause, so a restart keeps it.
const pausedKey = "subwallet_spending_paused"

// ErrSpendingPaused means the admin, or a lasting shortfall, paused sub-wallet spending.
var ErrSpendingPaused = errors.New("sub-wallet spending is paused by the operator")

// SetSpendingPaused pauses or resumes every payment from a sub-wallet (R14). A payment
// reads the switch inside the write transaction that stores its hold, and SQLite runs
// write transactions one at a time, so a payment is either held before the change or
// checked after it: none passes in the moment the switch turns. Receiving, pending
// payments, apps, withdraw and the sweep of deleted sub-wallets are not affected.
func (l *Ledger) SetSpendingPaused(ctx context.Context, paused bool) error {
	var err error
	if paused {
		_, err = l.db.ExecContext(ctx, `INSERT INTO settings (key, value) VALUES (?, '1')
			ON CONFLICT (key) DO NOTHING`, pausedKey)
	} else {
		_, err = l.db.ExecContext(ctx, `DELETE FROM settings WHERE key = ?`, pausedKey)
	}
	if err == nil {
		l.log.Warn("sub-wallet spending changed", "paused", paused)
	}
	return err
}

// SpendingPaused reports whether sub-wallet spending is paused.
func (l *Ledger) SpendingPaused(ctx context.Context) (bool, error) {
	return spendingPaused(ctx, l.db)
}

func spendingPaused(ctx context.Context, db querier) (bool, error) {
	var paused bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM settings WHERE key = ?)`, pausedKey).Scan(&paused)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return paused, err
}
