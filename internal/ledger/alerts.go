package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Alert kinds. Each is a place where the ledger and the node disagree about money,
// or a shortfall that lasts. Nothing is corrected by itself: the admin decides.
const (
	AlertAmountDiffers   = "amount_differs"
	AlertFeeAboveReserve = "fee_above_reserve"
	AlertPaidAfterFailed = "paid_after_failed"
	AlertIncomeOnClosed  = "income_on_closed_invoice"
	AlertImpossibleReply = "impossible_reply"
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
// It reports whether the alert is new.
func (l *Ledger) raise(ctx context.Context, db executor, kind, key, message string) bool {
	l.log.Error("alert: "+message, "kind", kind, "key", key)
	added, err := changedOne(db.ExecContext(context.WithoutCancel(ctx), `INSERT INTO alerts (kind, key, message, created_at)
		VALUES (?, ?, ?, ?) ON CONFLICT (kind, key) DO NOTHING`, kind, key, message, l.now().Unix()))
	if err != nil {
		l.log.Error("store alert", "kind", kind, "error", err)
	}
	return added
}

// Alerts lists the open alerts, newest first.
func (l *Ledger) Alerts(ctx context.Context) ([]Alert, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT id, kind, message, created_at FROM alerts
		WHERE closed_at IS NULL ORDER BY created_at DESC, id DESC`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var (
		alerts   []Alert
		shortage bool
	)
	for rows.Next() {
		var (
			alert   Alert
			created int64
		)
		if err := rows.Scan(&alert.ID, &alert.Kind, &alert.Message, &created); err != nil {
			return nil, err
		}
		alert.CreatedAt = time.Unix(created, 0).UTC()
		shortage = shortage || alert.Kind == AlertShortfall
		alerts = append(alerts, alert)
	}
	if err := rows.Err(); err != nil || !shortage {
		return alerts, err
	}
	// The pause can change after the alert, so its text comes from the switch now.
	paused, err := spendingPaused(ctx, l.db)
	if err != nil {
		return nil, err
	}
	note := " Sub-wallet spending was resumed by the admin."
	if paused {
		note = " Sub-wallet spending is paused, so that no holder pays out ahead of the others: resume it on the dashboard."
	}
	for i := range alerts {
		if alerts[i].Kind == AlertShortfall {
			alerts[i].Message += note
		}
	}
	return alerts, nil
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
	if overview.ShortfallMsat <= 0 {
		l.mu.Lock()
		l.shortfallSince = time.Time{}
		l.mu.Unlock()
		_, err := l.db.ExecContext(ctx, `UPDATE alerts SET closed_at = ? WHERE kind = ? AND closed_at IS NULL`,
			now.Unix(), AlertShortfall)
		return err
	}
	since, err := l.shortfallStart(ctx, now)
	if err != nil {
		return err
	}
	if now.Sub(since) < shortfallAlertAfter {
		return nil
	}
	return l.raiseShortfall(ctx, fmt.Sprint(since.Unix()), fmt.Sprintf(
		"Shortfall of %d msat since %s: sub-wallets and pending payments are owed more than the node can send. "+
			"An offline peer does not cause this. A closing channel, or a new channel below its reserve, can.",
		overview.ShortfallMsat, since.UTC().Format(time.DateTime+" UTC")))
}

// shortfallStart is when the current shortfall began. After a restart it is the key
// of the open shortfall alert, if there is one: the alert is closed in the pass that
// sees a shortfall end, so an open one means the shortfall has gone on. Without this,
// a restart would start a new shortfall, raise a second alert and pause again over
// the admin's resume.
func (l *Ledger) shortfallStart(ctx context.Context, now time.Time) (time.Time, error) {
	l.mu.Lock()
	since := l.shortfallSince
	l.mu.Unlock()
	if !since.IsZero() {
		return since, nil
	}
	since = now
	var key string
	err := l.db.QueryRowContext(ctx, `SELECT key FROM alerts WHERE kind = ? AND closed_at IS NULL
		ORDER BY created_at DESC, id DESC LIMIT 1`, AlertShortfall).Scan(&key)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return time.Time{}, err
	default:
		if unix, err := strconv.ParseInt(key, 10, 64); err == nil {
			since = time.Unix(unix, 0)
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.shortfallSince = since
	return since, nil
}

// raiseShortfall stores the shortfall alert and turns on the pause in one transaction,
// once per shortfall: both are stored or neither, and a pass after a failed write
// tries again. So an open shortfall alert with no pause means the admin resumed, and
// that resume stands for the rest of the shortfall (owner, SR5).
func (l *Ledger) raiseShortfall(ctx context.Context, key, message string) error {
	l.log.Error("alert: "+message, "kind", AlertShortfall, "key", key)
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }() // Does nothing after a commit.
	added, err := changedOne(tx.ExecContext(ctx, `INSERT INTO alerts (kind, key, message, created_at)
		VALUES (?, ?, ?, ?) ON CONFLICT (kind, key) DO NOTHING`, AlertShortfall, key, message, l.now().Unix()))
	if err != nil {
		return fmt.Errorf("store shortfall alert: %w", err)
	}
	if !added {
		return nil
	}
	if err := setPaused(ctx, tx, true); err != nil {
		return fmt.Errorf("pause sub-wallet spending: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store shortfall alert: %w", err)
	}
	l.log.Warn("sub-wallet spending changed", "paused", true)
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
