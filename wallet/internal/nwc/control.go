package nwc

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/getAlby/go-nostr"
	"github.com/getAlby/go-nostr/nip19"

	"github.com/2rdzy/nwc4ln/wallet/internal/store"
)

// Relay control: the wallet tells our own relay which keys may use it, with an
// allow-list signed by the control key. The relay keeps the same format; its
// golden list is copied into testdata/allowlist.json.
//
//	kind:    29194, an ephemeral kind no NIP uses, so a foreign relay does not keep it
//	pubkey:  the control key
//	content: empty
//	tags:    one ["pair", "<client key hex>", "<service key hex>"] per connection, by connection ID
const (
	kindAllowList = 29194

	// controlKeySetting is the settings row with the control key's secret, in hex.
	controlKeySetting = "relay_control_key"

	// maxAllowList is our relay's largest message, 128 KiB: about 900 pairs.
	maxAllowList = 128 << 10

	// listWait is how long creating or replacing a connection waits for the relay
	// to accept the list that names it, before the connection string is shown.
	listWait = 10 * time.Second
)

// notNewer matches our relay's refusal of a list that is not newer than the one
// it saved, and captures the saved list's created_at. The relay's two clock
// refusals name the wallet's own time, so they must not match.
var notNewer = regexp.MustCompile(`not newer than the saved allow-list, created_at (\d+)`)

// EnsureControlKey makes the control key once, and keeps the one there is.
func EnsureControlKey(ctx context.Context, db *store.Store) error {
	_, err := db.DB.ExecContext(ctx, `INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT (key) DO NOTHING`,
		controlKeySetting, nostr.GeneratePrivateKey())
	if err != nil {
		return fmt.Errorf("make the control key: %w", err)
	}
	return nil
}

// controlSecret reads the control key's secret, or "" while there is none.
func controlSecret(ctx context.Context, db *sql.DB) (string, error) {
	var secret string
	err := db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key = ?`, controlKeySetting).Scan(&secret)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read the control key: %w", err)
	}
	return secret, nil
}

// ControlPublicKey returns the control key's public key as hex and npub, for the
// relay's config. It only reads. found is false while no key was made yet.
func ControlPublicKey(ctx context.Context, db *store.Store) (hex, npub string, found bool, err error) {
	secret, err := controlSecret(ctx, db.DB)
	if err != nil || secret == "" {
		return "", "", false, err
	}
	if hex, err = nostr.GetPublicKey(secret); err != nil {
		return "", "", false, fmt.Errorf("control key: %w", err)
	}
	if npub, err = nip19.EncodePublicKey(hex); err != nil {
		return "", "", false, fmt.Errorf("control key: %w", err)
	}
	return hex, npub, true, nil
}

// CheckControlRelay refuses a clear-text relay URL that is not on this machine:
// with relay control on, the allow-list would cross the network in clear text.
func CheckControlRelay(relay string) error {
	parsed, err := url.Parse(relay)
	if err != nil {
		return fmt.Errorf("relay %q: %w", relay, err)
	}
	if parsed.Scheme == "ws" && !isLoopbackHost(parsed.Hostname()) {
		return fmt.Errorf("relay control needs a wss:// relay URL, or ws:// to a loopback address, got %q", relay)
	}
	return nil
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// control is the state of relay control. It exists only while control is on.
type control struct {
	secret, pub string
	wait        time.Duration // listWait. Changed only by tests.

	mu sync.Mutex
	// wanted counts the changes to the connections. accepted is the change the relay
	// last accepted a list for, and acceptedNow is closed and made again on each.
	wanted, accepted uint64
	acceptedNow      chan struct{}
	stamp            int64           // The last list's created_at.
	listed           map[string]bool // Service keys on the accepted list.
	problem          string          // Why the last list was not accepted, or "".
}

// UseRelayControl turns relay control on: the service sends the allow-list on
// every connect and change, before it subscribes and before the info events. The
// control key must exist (EnsureControlKey). Call it before Run.
func (s *Service) UseRelayControl(ctx context.Context) error {
	if err := CheckControlRelay(s.relay); err != nil {
		return err
	}
	secret, err := controlSecret(ctx, s.db)
	if err != nil {
		return err
	}
	if secret == "" {
		return errors.New("relay control is on, but there is no control key")
	}
	pub, err := nostr.GetPublicKey(secret)
	if err != nil {
		return fmt.Errorf("control key: %w", err)
	}
	s.control = &control{secret: secret, pub: pub, wait: listWait, acceptedNow: make(chan struct{})}
	return nil
}

// RelayState is what the admin UI shows about the relay: whether relay control is
// on, whether the URL is clear text to another machine, and why the relay last
// refused the allow-list, or "".
func (s *Service) RelayState() (controlOn, clearText bool, problem string) {
	parsed, err := url.Parse(s.relay)
	clearText = err == nil && parsed.Scheme == "ws" && !isLoopbackHost(parsed.Hostname())
	if s.control == nil {
		return false, clearText, ""
	}
	s.control.mu.Lock()
	defer s.control.mu.Unlock()
	return true, clearText, s.control.problem
}

// WaitListed waits, up to listWait, until the relay has accepted a list with
// every change made so far, and reports whether it has. With relay control off
// there is no list, so it reports true at once.
func (s *Service) WaitListed(ctx context.Context) bool {
	c := s.control
	if c == nil {
		return true
	}
	timeout := time.NewTimer(c.wait)
	defer timeout.Stop()
	c.mu.Lock()
	target := c.wanted
	for c.accepted < target {
		next := c.acceptedNow
		c.mu.Unlock()
		select {
		case <-next:
		case <-timeout.C:
			return false
		case <-ctx.Done():
			return false
		}
		c.mu.Lock()
	}
	c.mu.Unlock()
	return true
}

// changedConnections counts a change, so that WaitListed waits for its list.
func (c *control) changedConnections() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wanted++
}

// isListed reports whether the relay has accepted a list with this service key.
func (c *control) isListed(servicePub string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.listed[servicePub]
}

// allowList signs the full list of pairs, by connection ID, so that the same
// connections always give the same list. Each list is stamped later than the
// last one, so the relay never takes a newer list for a repeat.
func (s *Service) allowList(list []connection, after int64) (nostr.Event, error) {
	slices.SortFunc(list, func(a, b connection) int { return cmp.Compare(a.id, b.id) })
	tags := make(nostr.Tags, 0, len(list))
	for _, c := range list {
		tags = append(tags, nostr.Tag{"pair", c.clientPub, c.servicePub})
	}
	event := nostr.Event{
		Kind: kindAllowList, CreatedAt: nostr.Timestamp(max(s.now().Unix(), after+1)), Tags: tags, Content: "",
	}
	if err := event.Sign(s.control.secret); err != nil {
		return event, fmt.Errorf("sign the allow-list: %w", err)
	}
	return event, nil
}

// sendList publishes the list of the connections served now and waits for the
// relay to accept it. It returns the connections on that list, so that the
// subscription names only keys the relay knows. A list the relay refuses as not
// newer is stamped past the relay's and sent once more; any other refusal is an
// error, and the admin sees it on the dashboard.
func (s *Service) sendList(ctx context.Context, relay *nostr.Relay) ([]connection, error) {
	c := s.control
	c.mu.Lock()
	change, after := c.wanted, c.stamp
	c.mu.Unlock()
	list := s.connectionList()
	for attempt := 0; ; attempt++ {
		event, err := s.allowList(list, after)
		if err != nil {
			return nil, s.listFailed(err)
		}
		c.mu.Lock()
		c.stamp = max(c.stamp, int64(event.CreatedAt))
		c.mu.Unlock()
		if size := listSize(event); size > maxAllowList {
			return nil, s.listFailed(fmt.Errorf("the allow-list of %d connections is %d bytes, over the relay's "+
				"limit of %d: delete apps or sub-wallets (a revoked one counts for 30 days)", len(list), size, maxAllowList))
		}
		publishCtx, cancel := context.WithTimeout(ctx, publishWithin)
		err = relay.Publish(publishCtx, event)
		cancel()
		if err == nil {
			break
		}
		match := notNewer.FindStringSubmatch(err.Error())
		if match == nil || attempt > 0 {
			return nil, s.listFailed(fmt.Errorf("the relay refused the allow-list: %w", err))
		}
		saved, _ := strconv.ParseInt(match[1], 10, 64)
		after = max(after, saved)
	}
	listed := make(map[string]bool, len(list))
	for _, item := range list {
		listed[item.servicePub] = true
	}
	c.mu.Lock()
	c.accepted = max(c.accepted, change)
	c.listed, c.problem = listed, ""
	close(c.acceptedNow)
	c.acceptedNow = make(chan struct{})
	c.mu.Unlock()
	s.log.Info("relay accepted the allow-list", "connections", len(list))
	return list, nil
}

// listFailed keeps the reason for the dashboard and logs it.
func (s *Service) listFailed(err error) error {
	s.control.mu.Lock()
	s.control.problem = err.Error()
	s.control.mu.Unlock()
	s.log.Error("allow-list not accepted", "error", err)
	return err
}

// listSize is the size of the message that carries the list: ["EVENT",{...}].
func listSize(event nostr.Event) int {
	data, _ := json.Marshal(event)
	return len(data) + len(`["EVENT",]`)
}
