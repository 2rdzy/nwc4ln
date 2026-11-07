package nwc

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/getAlby/go-nostr"

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

// AddApp creates an app with no limit and one connection, and returns its
// connection string. That string is the only copy of the client secret: only the
// public key is stored, so this is the one time it can be shown.
func AddApp(ctx context.Context, db *store.Store, name, relay string, canInvoice bool) (connectionString string, err error) {
	if name == "" {
		return "", errors.New("app name is required")
	}
	if err := CheckRelay(relay); err != nil {
		return "", err
	}
	serviceKey, servicePub, err := newKey()
	if err != nil {
		return "", err
	}
	clientSecret, clientPub, err := newKey()
	if err != nil {
		return "", err
	}
	tx, err := db.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }() // Does nothing after a commit.
	now := nostr.Now()
	result, err := tx.ExecContext(ctx, `INSERT INTO accounts (type, name, can_pay, can_invoice, created_at)
		VALUES ('app', ?, 1, ?, ?)`, name, canInvoice, int64(now))
	if err != nil {
		return "", fmt.Errorf("add app: %w", err)
	}
	account, err := result.LastInsertId()
	if err != nil {
		return "", fmt.Errorf("add app: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO connections (account_id, client_pubkey, service_key, role, created_at)
		VALUES (?, ?, ?, 'full', ?)`, account, clientPub, serviceKey, int64(now)); err != nil {
		return "", fmt.Errorf("add connection: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("add app: %w", err)
	}
	query := url.Values{"relay": {relay}, "secret": {clientSecret}}
	return "nostr+walletconnect://" + servicePub + "?" + query.Encode(), nil
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
