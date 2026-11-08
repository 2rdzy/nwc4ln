package ledger

import (
	"context"
	"database/sql"
)

// Account names an account of either type, deleted ones included, so that every row
// of the history can be named.
type Account struct {
	ID      int64
	Type    string // "app" or "sub_wallet".
	Name    string
	Deleted bool
}

// Accounts lists every account, deleted ones included, oldest first.
func (l *Ledger) Accounts(ctx context.Context) ([]Account, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT id, type, name, deleted_at FROM accounts ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var accounts []Account
	for rows.Next() {
		var (
			account Account
			deleted sql.Null[int64]
		)
		if err := rows.Scan(&account.ID, &account.Type, &account.Name, &deleted); err != nil {
			return nil, err
		}
		account.Deleted = deleted.Valid
		accounts = append(accounts, account)
	}
	return accounts, rows.Err()
}

// AllAccounts is the AllHistoryQuery account that matches every row.
const AllAccounts int64 = -1

// AllHistoryQuery chooses rows for the admin, across accounts.
type AllHistoryQuery struct {
	// AccountID is one account, AllAccounts, or 0 for the main wallet side of funds and
	// withdraws, which is stored with no account.
	AccountID int64
	// Limit 0 means no limit.
	Limit, Offset int64
}

// AllHistory returns one page of rows in every state, newest first, and how many match
// without the paging. Unlike History, it can match every account, or the rows that
// have none.
func (l *Ledger) AllHistory(ctx context.Context, query AllHistoryQuery) ([]Transaction, int64, error) {
	where, args := "1", []any{}
	switch query.AccountID {
	case AllAccounts:
	case 0:
		where = "account_id IS NULL"
	default:
		where, args = "account_id = ?", []any{query.AccountID}
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
