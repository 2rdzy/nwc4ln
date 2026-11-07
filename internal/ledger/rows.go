package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/2rdzy/nwc4ln/internal/node"
)

// Transaction types and states.
const (
	TypeIncoming = "incoming"
	TypeOutgoing = "outgoing"

	StatePending = "pending"
	StateSettled = "settled"
	StateFailed  = "failed"
)

const (
	settleIndexKey = "settle_index"
	nodeKeyKey     = "node_pubkey"
)

// Transaction is one row of the transactions table.
type Transaction struct {
	ID          int64
	AccountID   int64 // 0 means none.
	RequestID   int64 // 0 means none.
	Type        string
	State       string
	AmountMsat  int64
	FeeMsat     int64
	ReserveMsat int64
	Invoice     string
	Description string
	PaymentHash string
	Preimage    string
	Internal    bool
	CreatedAt   time.Time
	SettledAt   time.Time // Zero until settled.
	ExpiresAt   time.Time
}

type executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

const columns = `id, account_id, request_id, type, state, amount_msat, fee_msat, reserve_msat, invoice, description,
	payment_hash, preimage, internal, created_at, settled_at, expires_at`

func nullable[T comparable](value T) sql.Null[T] {
	var zero T
	return sql.Null[T]{V: value, Valid: value != zero}
}

func unixOrNull(t time.Time) sql.Null[int64] {
	if t.IsZero() {
		return sql.Null[int64]{}
	}
	return sql.Null[int64]{V: t.Unix(), Valid: true}
}

func timeOf(unix sql.Null[int64]) time.Time {
	if !unix.Valid {
		return time.Time{}
	}
	return time.Unix(unix.V, 0).UTC()
}

func insert(ctx context.Context, db executor, row Transaction) (int64, error) {
	result, err := db.ExecContext(ctx, `INSERT INTO transactions
		(account_id, request_id, type, state, amount_msat, reserve_msat, invoice, description,
		 payment_hash, preimage, internal, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		nullable(row.AccountID), nullable(row.RequestID), row.Type, row.State, row.AmountMsat, row.ReserveMsat,
		nullable(row.Invoice), nullable(row.Description), row.PaymentHash, nullable(row.Preimage), row.Internal,
		row.CreatedAt.Unix(), unixOrNull(row.ExpiresAt))
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func scanRow(scan func(...any) error) (Transaction, error) {
	var (
		row                            Transaction
		account, request               sql.Null[int64]
		invoice, description, preimage sql.Null[string]
		created                        int64
		settled, expires               sql.Null[int64]
	)
	err := scan(&row.ID, &account, &request, &row.Type, &row.State, &row.AmountMsat, &row.FeeMsat, &row.ReserveMsat,
		&invoice, &description, &row.PaymentHash, &preimage, &row.Internal, &created, &settled, &expires)
	if err != nil {
		return Transaction{}, err
	}
	row.AccountID, row.RequestID = account.V, request.V
	row.Invoice, row.Description, row.Preimage = invoice.V, description.V, preimage.V
	row.CreatedAt = time.Unix(created, 0).UTC()
	row.SettledAt, row.ExpiresAt = timeOf(settled), timeOf(expires)
	return row, nil
}

func listRows(ctx context.Context, db querier, where string, args ...any) ([]Transaction, error) {
	rows, err := db.QueryContext(ctx, "SELECT "+columns+" FROM transactions WHERE "+where, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var found []Transaction
	for rows.Next() {
		row, err := scanRow(rows.Scan)
		if err != nil {
			return nil, err
		}
		found = append(found, row)
	}
	return found, rows.Err()
}

func pendingRows(ctx context.Context, db querier, kind string) ([]Transaction, error) {
	return listRows(ctx, db, "type = ? AND state = 'pending' ORDER BY id", kind)
}

func (l *Ledger) transaction(ctx context.Context, id int64) (Transaction, error) {
	row := l.db.QueryRowContext(ctx, "SELECT "+columns+" FROM transactions WHERE id = ?", id)
	return scanRow(row.Scan)
}

// HistoryQuery chooses transactions of one account.
type HistoryQuery struct {
	AccountID int64
	// From and Until are Unix seconds, both inclusive, on created_at. Until 0
	// means no end.
	From, Until int64
	// Limit 0 means no limit.
	Limit, Offset int64
	// Unpaid includes rows that are not settled.
	Unpaid bool
	// Type is TypeIncoming, TypeOutgoing, or empty for both.
	Type string
}

// History returns one page of an account's transactions, newest first, and how
// many match without the paging.
func (l *Ledger) History(ctx context.Context, query HistoryQuery) ([]Transaction, int64, error) {
	where := "account_id = ? AND created_at >= ?"
	args := []any{query.AccountID, query.From}
	if query.Until != 0 {
		where += " AND created_at <= ?"
		args = append(args, query.Until)
	}
	if !query.Unpaid {
		where += " AND state = 'settled'"
	}
	if query.Type != "" {
		where += " AND type = ?"
		args = append(args, query.Type)
	}
	var total int64
	err := l.db.QueryRowContext(ctx, "SELECT count(*) FROM transactions WHERE "+where, args...).Scan(&total)
	if err != nil {
		return nil, 0, err
	}
	page := where + " ORDER BY created_at DESC, id DESC"
	if query.Limit != 0 {
		page += " LIMIT ? OFFSET ?"
		args = append(args, query.Limit, query.Offset)
	}
	rows, err := listRows(ctx, l.db, page, args...)
	return rows, total, err
}

// Lookup finds an account's transaction by payment hash, or by invoice text if
// the hash is empty. Two rows can match, after a retry or when an account pays its
// own invoice: the outgoing row comes first, then the newest. It needs no node.
func (l *Ledger) Lookup(ctx context.Context, accountID int64, hash, invoice string) (Transaction, error) {
	column, value := "payment_hash", strings.ToLower(hash)
	if hash == "" {
		column, value = "invoice", strings.ToLower(invoice)
	}
	rows, err := listRows(ctx, l.db, "account_id = ? AND "+column+" = ? ORDER BY type = 'outgoing' DESC, id DESC LIMIT 1",
		accountID, value)
	if err != nil {
		return Transaction{}, err
	}
	if len(rows) == 0 {
		return Transaction{}, ErrNotFound
	}
	return rows[0], nil
}

// MainAvailable is what the node can send now, less every hold. It can be below
// 0. It fails if the node cannot be asked, so nobody is shown a guess.
func (l *Ledger) MainAvailable(ctx context.Context) (int64, error) {
	spendable, err := l.node.Spendable(ctx)
	if err != nil {
		return 0, fmt.Errorf("read spendable: %w", err)
	}
	var holds int64
	err = l.db.QueryRowContext(ctx, `SELECT coalesce(sum(amount_msat + reserve_msat), 0) FROM transactions
		WHERE type = 'outgoing' AND state = 'pending'`).Scan(&holds)
	if err != nil {
		return 0, err
	}
	return spendable.ActiveMsat - holds, nil
}

func changedOne(result sql.Result, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count == 1, err
}

// settleOutgoing reports whether a row changed.
func settleOutgoing(
	ctx context.Context, db executor, id, amountMsat, feeMsat int64, preimage string, at time.Time,
) (bool, error) {
	return changedOne(db.ExecContext(ctx, `UPDATE transactions
		SET state = 'settled', amount_msat = ?, fee_msat = ?, preimage = ?, settled_at = ?
		WHERE id = ? AND state = 'pending'`, amountMsat, feeMsat, preimage, at.Unix(), id))
}

func failRow(ctx context.Context, db executor, id int64) error {
	_, err := db.ExecContext(ctx, `UPDATE transactions SET state = 'failed' WHERE id = ? AND state = 'pending'`, id)
	return err
}

// settleIncoming credits what was really paid, which can differ from the invoice
// amount. It reports whether a row changed.
func settleIncoming(ctx context.Context, db executor, invoice node.Invoice, at time.Time) (bool, error) {
	return changedOne(db.ExecContext(ctx, `UPDATE transactions SET state = 'settled', amount_msat = ?, settled_at = ?
		WHERE payment_hash = ? AND type = 'incoming' AND state = 'pending'`,
		invoice.PaidMsat, at.Unix(), invoice.Hash))
}

// settleBothSides moves an internal payment in one database transaction, with
// fee 0. The payer gets the preimage stored with the invoice.
func settleBothSides(ctx context.Context, db *sql.DB, row Transaction, at time.Time) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }() // Does nothing after a commit.
	var preimage string
	err = tx.QueryRowContext(ctx, `SELECT preimage FROM transactions
		WHERE payment_hash = ? AND type = 'incoming' AND state = 'pending'`, row.PaymentHash).Scan(&preimage)
	if err != nil {
		return fmt.Errorf("find invoice: %w", err)
	}
	moved, err := changedOne(tx.ExecContext(ctx, `UPDATE transactions
		SET state = 'settled', fee_msat = 0, preimage = ?, settled_at = ?
		WHERE id = ? AND state = 'pending'`, preimage, at.Unix(), row.ID))
	if err != nil {
		return err
	}
	if !moved {
		return errors.New("payment is no longer pending")
	}
	moved, err = changedOne(tx.ExecContext(ctx, `UPDATE transactions SET state = 'settled', internal = 1, settled_at = ?
		WHERE payment_hash = ? AND type = 'incoming' AND state = 'pending'`, at.Unix(), row.PaymentHash))
	if err != nil {
		return err
	}
	if !moved {
		return errors.New("invoice is no longer pending")
	}
	return tx.Commit()
}

// bindNodeKey stores the key if none is stored, and returns the stored one.
func bindNodeKey(ctx context.Context, db *sql.DB, key string) (string, error) {
	_, err := db.ExecContext(ctx, `INSERT INTO settings (key, value) VALUES (?, ?)
		ON CONFLICT (key) DO NOTHING`, nodeKeyKey, key)
	if err != nil {
		return "", err
	}
	var stored string
	err = db.QueryRowContext(ctx, "SELECT value FROM settings WHERE key = ?", nodeKeyKey).Scan(&stored)
	return stored, err
}

func settleIndex(ctx context.Context, db querier) (uint64, error) {
	var value string
	err := db.QueryRowContext(ctx, "SELECT value FROM settings WHERE key = ?", settleIndexKey).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(value, 10, 64)
}

// storeSettleIndex only moves forward, so a replay cannot rewind it.
func storeSettleIndex(ctx context.Context, tx *sql.Tx, index uint64) error {
	current, err := settleIndex(ctx, tx)
	if err != nil || index <= current {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO settings (key, value) VALUES (?, ?)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value`, settleIndexKey, strconv.FormatUint(index, 10))
	return err
}
