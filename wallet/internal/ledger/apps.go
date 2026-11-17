package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
)

// Renewal periods of a limit.
const (
	RenewDaily   = "daily"
	RenewWeekly  = "weekly"
	RenewMonthly = "monthly"
	RenewYearly  = "yearly"
	RenewNever   = "never"
)

// AppSettings are what the admin chooses for an app. A change applies to the next
// request. Payments already pending finish, and usage is worked out again from the
// rows, so nothing else needs handling. A limit cut below usage leaves used above
// the limit, and nothing more can be spent.
type AppSettings struct {
	// LimitMsat of nil means no limit. A limit of 0 is real, and allows nothing:
	// clients must be able to tell the two apart (NWC-13).
	LimitMsat *int64
	Renewal   string
	// ExpiresAt of zero means the app does not expire.
	ExpiresAt  time.Time
	CanPay     bool
	CanInvoice bool
}

// DefaultApp is a new app: pay on, invoice off, no limit.
func DefaultApp() AppSettings {
	return AppSettings{Renewal: RenewNever, CanPay: true}
}

// Check reports settings that cannot be stored.
func (s AppSettings) Check() error {
	switch s.Renewal {
	case RenewDaily, RenewWeekly, RenewMonthly, RenewYearly, RenewNever:
	default:
		return fmt.Errorf("%w: %q", ErrBadRenewal, s.Renewal)
	}
	if s.LimitMsat != nil && (*s.LimitMsat < 0 || *s.LimitMsat > MaxAmountMsat) {
		return fmt.Errorf("%w: %d", ErrBadAmount, *s.LimitMsat)
	}
	return nil
}

// LimitColumn gives the limit in the form the accounts table stores it.
func (s AppSettings) LimitColumn() sql.Null[int64] {
	if s.LimitMsat == nil {
		return sql.Null[int64]{}
	}
	return sql.Null[int64]{V: *s.LimitMsat, Valid: true}
}

// ExpiryColumn gives the expiry in the form the accounts table stores it.
func (s AppSettings) ExpiryColumn() sql.Null[int64] {
	return unixOrNull(s.ExpiresAt)
}

// App is one app as the admin sees it.
type App struct {
	ID   int64
	Name string
	AppSettings
	CreatedAt  time.Time
	LastUsedAt time.Time // Zero if never used.
	// UsedMsat is limit usage in the current period (R6).
	UsedMsat int64
}

// Budget is an app's limit in the current period.
type Budget struct {
	TotalMsat int64
	UsedMsat  int64
	Renewal   string
	RenewsAt  time.Time // Zero if the limit does not renew.
}

// periodStart returns the start of the period that holds the time. Periods follow
// the calendar in UTC, and weeks start on Monday.
func periodStart(renewal string, at time.Time) time.Time {
	at = at.UTC()
	year, month, day := at.Date()
	switch renewal {
	case RenewDaily:
		return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	case RenewWeekly:
		sinceMonday := (int(at.Weekday()) + 6) % 7
		return time.Date(year, month, day-sinceMonday, 0, 0, 0, 0, time.UTC)
	case RenewMonthly:
		return time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	case RenewYearly:
		return time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC)
	default:
		return time.Unix(0, 0).UTC()
	}
}

// periodEnd returns when the period that holds the time ends, or zero if it never
// does.
func periodEnd(renewal string, at time.Time) time.Time {
	start := periodStart(renewal, at)
	switch renewal {
	case RenewDaily:
		return start.AddDate(0, 0, 1)
	case RenewWeekly:
		return start.AddDate(0, 0, 7)
	case RenewMonthly:
		return start.AddDate(0, 1, 0)
	case RenewYearly:
		return start.AddDate(1, 0, 0)
	default:
		return time.Time{}
	}
}

// readAccount reads an account that has not been deleted.
func readAccount(ctx context.Context, db querier, accountID int64) (AppSettings, error) {
	var (
		settings         AppSettings
		limit            sql.Null[int64]
		expires, deleted sql.Null[int64]
	)
	err := db.QueryRowContext(ctx, `SELECT limit_msat, renewal, expires_at, can_pay, can_invoice, deleted_at
		FROM accounts WHERE id = ?`, accountID).Scan(&limit, &settings.Renewal, &expires, &settings.CanPay,
		&settings.CanInvoice, &deleted)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return AppSettings{}, ErrUnknownAccount
	case err != nil:
		return AppSettings{}, err
	case deleted.Valid:
		return AppSettings{}, ErrAccountDeleted
	}
	if limit.Valid {
		settings.LimitMsat = &limit.V
	}
	settings.ExpiresAt = timeOf(expires)
	return settings, nil
}

// checkAccount decides whether an account may use the ledger at all, and returns
// its settings. An expired or deleted account gets no new work, and its pending
// payments still finish.
func (l *Ledger) checkAccount(ctx context.Context, db querier, accountID int64) (AppSettings, error) {
	settings, err := readAccount(ctx, db, accountID)
	if err != nil {
		return AppSettings{}, err
	}
	if !settings.ExpiresAt.IsZero() && !l.now().Before(settings.ExpiresAt) {
		return AppSettings{}, ErrAccountExpired
	}
	return settings, nil
}

// Access returns an account's settings, or the error that says why it may not be
// used. Every request starts here.
func (l *Ledger) Access(ctx context.Context, accountID int64) (AppSettings, error) {
	return l.checkAccount(ctx, l.db, accountID)
}

// usedMsat is limit usage since a time (R6). A settled row counts what really left,
// and a pending one what it holds. Each row keeps its reserve after settling, so the
// two must not be added up. A payment stays in the period of its created_at, even
// if it settles in the next one.
func usedMsat(ctx context.Context, db querier, accountID int64, since time.Time) (int64, error) {
	var used int64
	err := db.QueryRowContext(ctx, `SELECT coalesce(sum(CASE state
			WHEN 'settled' THEN amount_msat + fee_msat
			ELSE amount_msat + reserve_msat END), 0)
		FROM transactions
		WHERE account_id = ? AND type = 'outgoing' AND state IN ('pending', 'settled') AND created_at >= ?`,
		accountID, since.Unix()).Scan(&used)
	return used, err
}

// availableFrom is R5, given the node's figures. It can be below 0. Payments in
// flight on active channels are added back, because their holds count them too.
func availableFrom(ctx context.Context, db querier, spendable node.Spendable) (int64, error) {
	inFlight, err := inFlightMsat(ctx, db, spendable.Outgoing, true)
	if err != nil {
		return 0, err
	}
	promised, err := subWalletTotal(ctx, db)
	if err != nil {
		return 0, err
	}
	holds, err := pendingHolds(ctx, db)
	if err != nil {
		return 0, err
	}
	return spendable.ActiveMsat + inFlight - promised - holds, nil
}

// pendingHolds is what every pending payment holds, apps and sub-wallets alike.
func pendingHolds(ctx context.Context, db querier) (int64, error) {
	var holds int64
	err := db.QueryRowContext(ctx, `SELECT coalesce(sum(amount_msat + reserve_msat), 0) FROM transactions
		WHERE type = 'outgoing' AND state = 'pending'`).Scan(&holds)
	return holds, err
}

// inFlightMsat is what the node's HTLCs for the ledger's own pending payments took
// from its figures. Those payments are held, so without this they would count twice.
// Any other HTLC, such as a payment made outside nwc4ln or a forward, is not added
// back: nothing holds it, and adding it would overstate the node during a drain. With
// activeOnly, only HTLCs on active channels count, as for ActiveMsat.
func inFlightMsat(ctx context.Context, db querier, htlcs []node.HTLC, activeOnly bool) (int64, error) {
	if len(htlcs) == 0 {
		return 0, nil
	}
	rows, err := db.QueryContext(ctx, `SELECT DISTINCT payment_hash FROM transactions
		WHERE type = 'outgoing' AND state = 'pending' AND internal = 0`)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	held := map[string]bool{}
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return 0, err
		}
		held[hash] = true
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	var total int64
	for _, htlc := range htlcs {
		if held[htlc.Hash] && (htlc.Active || !activeOnly) {
			total += htlc.CostMsat
		}
	}
	return total, nil
}

// checkFunds is R7. A sub-wallet needs only its own balance. An app needs its limit,
// then the main wallet. Exact msat comparisons.
func (l *Ledger) checkFunds(
	ctx context.Context, db querier, accountID int64, subWallet bool, settings AppSettings, spendable node.Spendable,
	costMsat int64,
) error {
	if subWallet {
		balance, err := subWalletBalance(ctx, db, accountID)
		if err != nil {
			return err
		}
		if costMsat > balance {
			return ErrInsufficientBalance
		}
		return nil
	}
	if settings.LimitMsat != nil {
		used, err := usedMsat(ctx, db, accountID, periodStart(settings.Renewal, l.now()))
		if err != nil {
			return err
		}
		if costMsat > *settings.LimitMsat-used {
			return ErrQuotaExceeded
		}
	}
	available, err := availableFrom(ctx, db, spendable)
	if err != nil {
		return err
	}
	if costMsat > available {
		// The usual sign of a shortfall (R14), so the admin needs to see it.
		l.log.Warn("app refused: main wallet too low", "account", accountID, "cost_msat", costMsat,
			"available_msat", available)
		return ErrInsufficientBalance
	}
	return nil
}

// MainAvailable is what the node can send now, less every hold and all sub-wallet
// money. It can be below 0. It fails if the node cannot be asked, so nobody is
// shown a guess.
func (l *Ledger) MainAvailable(ctx context.Context) (int64, error) {
	spendable, err := l.node.Spendable(ctx)
	if err != nil {
		return 0, fmt.Errorf("read spendable: %w", err)
	}
	return availableFrom(ctx, l.db, spendable)
}

// Shortfall is R14: what the ledger owes to sub-wallets and pending payments, less
// what the node can send over all open channels, so that an offline peer raises no
// alarm. Above 0, the node holds less than the ledger promises. It is read under the
// one lock, like a payment check, so that no payment can settle between the node's
// figure and the rows. It fails if the node cannot be asked.
func (l *Ledger) Shortfall(ctx context.Context) (int64, error) {
	overview, err := l.Overview(ctx)
	if err != nil {
		return 0, err
	}
	return overview.ShortfallMsat, nil
}

// Overview is the admin's view of the money, all from one node read.
type Overview struct {
	// NodeSpendableMsat is the node's figure on active channels, with the ledger's own
	// payments in flight added back, as R5 reads it. It equals SubWalletMsat plus
	// HoldsMsat plus MainAvailableMsat.
	NodeSpendableMsat int64
	// SubWalletMsat is all sub-wallet money, deleted ones not yet moved included.
	SubWalletMsat int64
	// HoldsMsat is what pending payments hold.
	HoldsMsat int64
	// MainAvailableMsat is R5. It can be below 0.
	MainAvailableMsat int64
	// ShortfallMsat is R14. Above 0 is a shortfall.
	ShortfallMsat int64
}

// Overview reads the node once, under the one lock, and gives R5 and R14 with the
// figures they come from, so that they agree with each other. It holds the lock
// through a node read, so do not call it often. It fails if the node cannot be asked:
// a figure is never guessed.
func (l *Ledger) Overview(ctx context.Context) (Overview, error) {
	l.holdLock.Lock()
	defer l.holdLock.Unlock()
	spendable, err := l.readSpendable(ctx)
	if err != nil {
		return Overview{}, err
	}
	available, err := availableFrom(ctx, l.db, spendable)
	if err != nil {
		return Overview{}, err
	}
	activeFlight, err := inFlightMsat(ctx, l.db, spendable.Outgoing, true)
	if err != nil {
		return Overview{}, err
	}
	allFlight, err := inFlightMsat(ctx, l.db, spendable.Outgoing, false)
	if err != nil {
		return Overview{}, err
	}
	promised, err := subWalletTotal(ctx, l.db)
	if err != nil {
		return Overview{}, err
	}
	holds, err := pendingHolds(ctx, l.db)
	if err != nil {
		return Overview{}, err
	}
	return Overview{
		NodeSpendableMsat: spendable.ActiveMsat + activeFlight,
		SubWalletMsat:     promised,
		HoldsMsat:         holds,
		MainAvailableMsat: available,
		ShortfallMsat:     promised + holds - spendable.AllMsat - allFlight,
	}, nil
}

// AppBudget returns an app's limit in the current period, or nil if it has none. A
// sub-wallet never has one.
func (l *Ledger) AppBudget(ctx context.Context, accountID int64) (*Budget, error) {
	settings, err := readAccount(ctx, l.db, accountID)
	if err != nil {
		return nil, err
	}
	subWallet, err := isSubWallet(ctx, l.db, accountID)
	if err != nil {
		return nil, err
	}
	if subWallet || settings.LimitMsat == nil {
		return nil, nil
	}
	now := l.now()
	used, err := usedMsat(ctx, l.db, accountID, periodStart(settings.Renewal, now))
	if err != nil {
		return nil, err
	}
	return &Budget{
		TotalMsat: *settings.LimitMsat, UsedMsat: used, Renewal: settings.Renewal,
		RenewsAt: periodEnd(settings.Renewal, now),
	}, nil
}

// AppBalance is what an app may be told it can spend: the lower of its remaining
// limit and the main wallet available. It can be below 0.
func (l *Ledger) AppBalance(ctx context.Context, accountID int64) (int64, error) {
	available, err := l.MainAvailable(ctx)
	if err != nil {
		return 0, err
	}
	budget, err := l.AppBudget(ctx, accountID)
	if err != nil || budget == nil {
		return available, err
	}
	return min(available, budget.TotalMsat-budget.UsedMsat), nil
}

// Apps lists the apps that have not been deleted, oldest first.
func (l *Ledger) Apps(ctx context.Context) ([]App, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT a.id, a.name, a.created_at, max(c.last_used_at)
		FROM accounts a LEFT JOIN connections c ON c.account_id = a.id
		WHERE a.type = 'app' AND a.deleted_at IS NULL GROUP BY a.id ORDER BY a.id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var apps []App
	for rows.Next() {
		var (
			app     App
			created int64
			used    sql.Null[int64]
		)
		if err := rows.Scan(&app.ID, &app.Name, &created, &used); err != nil {
			return nil, err
		}
		app.CreatedAt, app.LastUsedAt = time.Unix(created, 0).UTC(), timeOf(used)
		apps = append(apps, app)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range apps {
		if apps[i].AppSettings, err = readAccount(ctx, l.db, apps[i].ID); err != nil {
			return nil, err
		}
		since := periodStart(apps[i].Renewal, l.now())
		if apps[i].UsedMsat, err = usedMsat(ctx, l.db, apps[i].ID, since); err != nil {
			return nil, err
		}
	}
	return apps, nil
}

// EditApp replaces an app's settings. See AppSettings for what a change means.
func (l *Ledger) EditApp(ctx context.Context, accountID int64, settings AppSettings) error {
	if err := settings.Check(); err != nil {
		return err
	}
	changed, err := changedOne(l.db.ExecContext(ctx, `UPDATE accounts
		SET limit_msat = ?, renewal = ?, expires_at = ?, can_pay = ?, can_invoice = ?
		WHERE id = ? AND type = 'app' AND deleted_at IS NULL`,
		settings.LimitColumn(), settings.Renewal, settings.ExpiryColumn(), settings.CanPay, settings.CanInvoice, accountID))
	if err != nil {
		return err
	}
	if !changed {
		return ErrUnknownAccount
	}
	return nil
}

// DeleteApp revokes an app's connections at once, and keeps its history. Its
// pending payments finish, and income that still arrives counts as main wallet.
func (l *Ledger) DeleteApp(ctx context.Context, accountID int64) error {
	return l.deleteAccount(ctx, accountID, "app")
}

// deleteAccount marks an account deleted and revokes its connections in one step. Any
// request or payment that comes later is refused.
func (l *Ledger) deleteAccount(ctx context.Context, accountID int64, kind string) error {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }() // Does nothing after a commit.
	now := l.now().Unix()
	changed, err := changedOne(tx.ExecContext(ctx, `UPDATE accounts SET deleted_at = ?
		WHERE id = ? AND type = ? AND deleted_at IS NULL`, now, accountID, kind))
	if err != nil {
		return err
	}
	if !changed {
		return ErrUnknownAccount
	}
	if _, err := tx.ExecContext(ctx, `UPDATE connections SET revoked_at = ?
		WHERE account_id = ? AND revoked_at IS NULL`, now, accountID); err != nil {
		return err
	}
	return tx.Commit()
}
