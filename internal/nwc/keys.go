package nwc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"

	"github.com/getAlby/go-nostr"

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/store"
)

// CheckRelay reports a relay URL the service could not connect to. The config
// file starts with no relay, and there is no default.
func CheckRelay(relay string) error {
	if relay == "" {
		return errors.New("relay is required in the config file")
	}
	parsed, err := url.Parse(relay)
	if err != nil {
		return fmt.Errorf("relay %q: %w", relay, err)
	}
	if (parsed.Scheme != "ws" && parsed.Scheme != "wss") || parsed.Host == "" {
		return fmt.Errorf("relay must be a ws:// or wss:// URL, got %q", relay)
	}
	return nil
}

// AddApp creates an app with one connection, and returns its connection string.
// That string is the only copy of the client secret: only the public key is stored,
// so this is the one time it can be shown. It is for tests and for a database no
// service has open. A running service must use its own AddApp, which the admin UI
// calls, so that it starts listening for the new connection.
func AddApp(
	ctx context.Context, db *store.Store, name, relay string, settings ledger.AppSettings,
) (connectionString string, err error) {
	return addAccount(ctx, db.DB, "app", name, relay, settings)
}

// subWalletSettings are fixed: a sub-wallet has no limit or expiry, and every
// command stays on. Nothing can edit them.
func subWalletSettings() ledger.AppSettings {
	return ledger.AppSettings{Renewal: ledger.RenewNever, CanPay: true, CanInvoice: true}
}

func addAccount(
	ctx context.Context, db *sql.DB, kind, name, relay string, settings ledger.AppSettings,
) (connectionString string, err error) {
	if name == "" {
		return "", errors.New("name is required")
	}
	if err := settings.Check(); err != nil {
		return "", err
	}
	if err := CheckRelay(relay); err != nil {
		return "", err
	}
	fresh, err := newCredentials(relay)
	if err != nil {
		return "", err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }() // Does nothing after a commit.
	now := nostr.Now()
	result, err := tx.ExecContext(ctx, `INSERT INTO accounts
		(type, name, limit_msat, renewal, expires_at, can_pay, can_invoice, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, kind, name, settings.LimitColumn(), settings.Renewal, settings.ExpiryColumn(),
		settings.CanPay, settings.CanInvoice, int64(now))
	if err != nil {
		return "", fmt.Errorf("add account: %w", err)
	}
	account, err := result.LastInsertId()
	if err != nil {
		return "", fmt.Errorf("add account: %w", err)
	}
	if err := fresh.store(ctx, tx, account, int64(now)); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("add account: %w", err)
	}
	return fresh.connectionString, nil
}

// replaceConnection gives a sub-wallet a new connection and revokes the old one in one
// step. The balance and history belong to the account, so they stay. The service key
// is new too, because the service finds a connection by that key. A receive-only
// connection is not touched.
func replaceConnection(ctx context.Context, db *sql.DB, accountID int64, relay string) (connectionString string, err error) {
	fresh, err := newCredentials(relay)
	if err != nil {
		return "", err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }() // Does nothing after a commit.
	var found bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM accounts
		WHERE id = ? AND type = 'sub_wallet' AND deleted_at IS NULL)`, accountID).Scan(&found)
	if err != nil {
		return "", fmt.Errorf("find account: %w", err)
	}
	if !found {
		return "", ledger.ErrUnknownAccount
	}
	now := int64(nostr.Now())
	if _, err := tx.ExecContext(ctx, `UPDATE connections SET revoked_at = ?
		WHERE account_id = ? AND role = 'full' AND revoked_at IS NULL`, now, accountID); err != nil {
		return "", fmt.Errorf("revoke connection: %w", err)
	}
	if err := fresh.store(ctx, tx, accountID, now); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("replace connection: %w", err)
	}
	return fresh.connectionString, nil
}

// credentials are the keys of a new connection. connectionString is the only copy of
// the client secret.
type credentials struct {
	serviceKey, clientPub, connectionString string
}

func newCredentials(relay string) (credentials, error) {
	serviceKey, servicePub, err := newKey()
	if err != nil {
		return credentials{}, err
	}
	clientSecret, clientPub, err := newKey()
	if err != nil {
		return credentials{}, err
	}
	query := url.Values{"relay": {relay}, "secret": {clientSecret}}
	return credentials{
		serviceKey: serviceKey, clientPub: clientPub,
		connectionString: "nostr+walletconnect://" + servicePub + "?" + query.Encode(),
	}, nil
}

func (c credentials) store(ctx context.Context, tx *sql.Tx, accountID, now int64) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO connections (account_id, client_pubkey, service_key, role, created_at)
		VALUES (?, ?, ?, 'full', ?)`, accountID, c.clientPub, c.serviceKey, now); err != nil {
		return fmt.Errorf("add connection: %w", err)
	}
	return nil
}

func newKey() (secret, public string, err error) {
	secret = nostr.GeneratePrivateKey()
	if secret == "" {
		return "", "", errors.New("generate key: no randomness")
	}
	public, err = nostr.GetPublicKey(secret)
	if err != nil {
		return "", "", fmt.Errorf("generate key: %w", err)
	}
	return secret, public, nil
}
