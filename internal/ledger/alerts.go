package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Alert kinds. Each is a place where the ledger and the node disagree about money,
// or a shortfall that lasts. Nothing is corrected by itself: the admin decides.
const (
	AlertAmountDiffers   = "amount_differs"
	AlertFeeAboveReserve = "fee_above_reserve"
	AlertPaidAfterFailed = "paid_after_failed"
	AlertIncomeOnClosed  = "income_on_closed_invoice"
	AlertCheck           = "check"
	AlertShortfall       = "shortfall"
	shortfallAlertAfter  = 30 * time.Minute
	shortfallCheckEvery  = 10 * time.Minute
	regularCheckEvery    = 24 * time.Hour
	regularCheckLookBack = 7 * 24 * time.Hour
)

// Alert is one alert as the admin sees it.
type Alert struct {
	ID        int64
	Kind      string
	Message   string
	CreatedAt time.Time
}

// raise stores an alert once per kind and key, and logs it. A repeat, even of one
// the admin closed, adds nothing. It reports a failure to store only in the log, so
// that an alert never stops the money path.
func (l *Ledger) raise(ctx context.Context, db executor, kind, key, message string) {
	l.log.Error("alert: "+message, "kind", kind, "key", key)
	_, err := db.ExecContext(context.WithoutCancel(ctx), `INSERT INTO alerts (kind, key, message, created_at)
		VALUES (?, ?, ?, ?) ON CONFLICT (kind, key) DO NOTHING`, kind, key, message, l.now().Unix())
	if err != nil {
		l.log.Error("store alert", "kind", kind, "error", err)
	}
}

// Alerts lists the open alerts, newest first.
func (l *Ledger) Alerts(ctx context.Context) ([]Alert, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT id, kind, message, created_at FROM alerts
		WHERE closed_at IS NULL ORDER BY created_at DESC, id DESC`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var alerts []Alert
	for rows.Next() {
		var (
			alert   Alert
			created int64
		)
		if err := rows.Scan(&alert.ID, &alert.Kind, &alert.Message, &created); err != nil {
			return nil, err
		}
		alert.CreatedAt = time.Unix(created, 0).UTC()
		alerts = append(alerts, alert)
	}
	return alerts, rows.Err()
}

// CloseAlert hides an open alert. It reports ErrNotFound for an unknown or closed one.
func (l *Ledger) CloseAlert(ctx context.Context, id int64) error {
	changed, err := changedOne(l.db.ExecContext(ctx, `UPDATE alerts SET closed_at = ?
		WHERE id = ? AND closed_at IS NULL`, l.now().Unix(), id))
	if err != nil {
		return err
	}
	if !changed {
		return ErrNotFound
	}
	return nil
}

// watchShortfall raises an alert once a shortfall has lasted shortfallAlertAfter, and
// closes it when the shortfall ends. Offline peers never cause one (R14). The start of
// the shortfall is its key, so a new one gets a new alert.
func (l *Ledger) watchShortfall(ctx context.Context) error {
	overview, err := l.Overview(ctx)
	if err != nil {
		return err
	}
	now := l.now()
	l.mu.Lock()
	if overview.ShortfallMsat <= 0 {
		l.shortfallSince = time.Time{}
	} else if l.shortfallSince.IsZero() {
		l.shortfallSince = now
	}
	since := l.shortfallSince
	l.mu.Unlock()
	if since.IsZero() {
		_, err := l.db.ExecContext(ctx, `UPDATE alerts SET closed_at = ? WHERE kind = ? AND closed_at IS NULL`,
			now.Unix(), AlertShortfall)
		return err
	}
	if now.Sub(since) >= shortfallAlertAfter {
		l.raise(ctx, l.db, AlertShortfall, fmt.Sprint(since.Unix()), fmt.Sprintf(
			"Shortfall of %d msat since %s: sub-wallets and pending payments are owed more than the node can send. "+
				"An offline peer does not cause this. A closing channel, or a new channel below its reserve, can.",
			overview.ShortfallMsat, since.UTC().Format(time.DateTime+" UTC")))
	}
	return nil
}

// due reports whether a timer has passed, and sets it again.
func (l *Ledger) due(next *time.Time, every time.Duration) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if now.Before(*next) {
		return false
	}
	*next = now.Add(every)
	return true
}

// regularCheck runs Check over the last week, and raises an alert for each mismatch.
func (l *Ledger) regularCheck(ctx context.Context) error {
	mismatches, err := l.Check(ctx, l.now().Add(-regularCheckLookBack))
	if err != nil {
		return err
	}
	for _, m := range mismatches {
		l.raise(ctx, l.db, AlertCheck, m.Hash+":"+m.Problem, "Check: "+m.String())
	}
	return nil
}

// executor for raise inside or outside a database transaction.
var _ executor = (*sql.DB)(nil)
