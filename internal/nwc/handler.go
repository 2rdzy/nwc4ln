package nwc

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/getAlby/go-nostr"
)

// Error codes from NIP-47.
const (
	codeUnauthorized          = "UNAUTHORIZED"
	codeNotImplemented        = "NOT_IMPLEMENTED"
	codeUnsupportedEncryption = "UNSUPPORTED_ENCRYPTION"
	codeInternal              = "INTERNAL"
	codePaymentFailed         = "PAYMENT_FAILED"
	codeNotFound              = "NOT_FOUND"
	codeOther                 = "OTHER"
)

// States of a row in the requests table. A request stays received if the process
// dies while it runs. It is never run again, because a repeat could pay twice.
const (
	stateReceived = "received"
	stateDone     = "done"
	stateFailed   = "failed"
	stateIgnored  = "ignored"
	// statePending is a payment that had no final state when the reply went out.
	statePending = "pending"
)

// Error is an error sent to a client.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type request struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

// response has error and result always present, because the protocol wants the
// unused one to be null.
type response struct {
	ResultType string `json:"result_type"`
	Error      *Error `json:"error"`
	Result     any    `json:"result"`
}

// accepted is a request stored once and ready to run.
type accepted struct {
	event      *nostr.Event
	connection connection
	rowID      int64
}

// accept decides whether an event is a new request for one of our connections,
// and stores it. Dropped events get no reply and leave no row, so that a stranger
// cannot fill the table: service keys are public.
func (s *Service) accept(ctx context.Context, event *nostr.Event) *accepted {
	if event.Kind != kindRequest {
		return nil
	}
	// The signature covers the event's content, not its id field. Without the ID
	// check, a repeat under a new id would pass the unique index below.
	if !event.CheckID() {
		s.log.Warn("request dropped: id does not match the event")
		return nil
	}
	if valid, err := event.CheckSignature(); err != nil || !valid {
		s.log.Warn("request dropped: bad signature", "event", event.ID)
		return nil
	}
	c, found := s.findConnection(event)
	if !found {
		s.log.Warn("request dropped: no connection for this key", "event", event.ID)
		return nil
	}
	if expired(event, s.now()) {
		s.log.Info("request dropped: expired", "event", event.ID)
		return nil
	}
	rowID, err := s.store(ctx, event, c)
	if err != nil {
		s.log.Error("request dropped: cannot store it", "event", event.ID, "error", err)
		return nil
	}
	if rowID == 0 {
		s.log.Info("request dropped: seen before", "event", event.ID)
		return nil
	}
	return &accepted{event: event, connection: c, rowID: rowID}
}

// findConnection finds the connection a request is for. The p tag names the
// service key, and the event must be signed by that connection's client key.
func (s *Service) findConnection(event *nostr.Event) (connection, bool) {
	for _, tag := range event.Tags {
		if len(tag) < 2 || tag[0] != "p" {
			continue
		}
		if c, found := s.connections[tag[1]]; found && c.clientPub == event.PubKey {
			return c, true
		}
	}
	return connection{}, false
}

func expired(event *nostr.Event, now time.Time) bool {
	value, found := tagValue(event, "expiration")
	if !found {
		return false
	}
	unix, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return true // Unreadable, so it cannot be shown to be still valid.
	}
	return now.Unix() > unix
}

// tagValue returns the first value of a tag. A tag with no value gives "".
func tagValue(event *nostr.Event, name string) (string, bool) {
	for _, tag := range event.Tags {
		if len(tag) == 0 || tag[0] != name {
			continue
		}
		if len(tag) == 1 {
			return "", true
		}
		return tag[1], true
	}
	return "", false
}

// store records the event ID, and returns the row, or 0 if it was seen before.
func (s *Service) store(ctx context.Context, event *nostr.Event, c connection) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }() // Does nothing after a commit.
	now := s.now().Unix()
	var rowID int64
	err = tx.QueryRowContext(ctx, `INSERT INTO requests (nostr_event_id, connection_id, state, created_at)
		VALUES (?, ?, ?, ?) ON CONFLICT (nostr_event_id) DO NOTHING RETURNING id`,
		event.ID, c.id, stateReceived, now).Scan(&rowID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO settings (key, value) VALUES (?, ?)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value`, lastSeenKey, strconv.FormatInt(now, 10)); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE connections SET last_used_at = ? WHERE id = ?`, now, c.id); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return rowID, nil
}

// run does the work of a stored request, and returns the signed response. It
// returns nil if the request gets no reply. The end of ctx stops the wait for a
// payment, not the payment, so the state is stored even then.
func (s *Service) run(ctx context.Context, a *accepted) *nostr.Event {
	reply, scheme, methodName, state := s.answer(ctx, a)
	if _, err := s.db.ExecContext(context.WithoutCancel(ctx), `UPDATE requests SET method = ?, state = ? WHERE id = ?`,
		methodName, state, a.rowID); err != nil {
		s.log.Error("cannot record request state", "event", a.event.ID, "error", err)
	}
	if reply == nil {
		return nil
	}
	event, err := s.seal(a, reply, scheme)
	if err != nil {
		s.log.Error("cannot build response", "event", a.event.ID, "error", err)
		return nil
	}
	return event
}

// answer works out the reply and the encryption it goes out in. The method is
// inside the encrypted content, so the rules that depend on it, and the
// result_type every reply must carry, come after decrypting.
func (s *Service) answer(ctx context.Context, a *accepted) (*response, string, string, string) {
	scheme, tagged := tagValue(a.event, "encryption")
	if !tagged {
		scheme = schemeNIP04 // Clients from before the tag existed.
	}
	c, err := newCipher(scheme, a.connection.clientPub, a.connection.serviceKey)
	if errors.Is(err, errUnsupportedEncryption) {
		reply := failure("", codeUnsupportedEncryption, "this encryption is not supported")
		return reply, schemeNIP44, "", stateFailed
	}
	if err != nil {
		return nil, "", "", stateFailed // The key is bad, so nothing can reach the client.
	}
	plain, err := c.decrypt(a.event.Content)
	if err != nil {
		return failure("", codeOther, "cannot decrypt the request"), scheme, "", stateFailed
	}
	var req request
	if err := json.Unmarshal([]byte(plain), &req); err != nil || req.Method == "" {
		return failure("", codeOther, "cannot read the request"), scheme, "", stateFailed
	}
	if tooOldOrNew(req.Method, a.event, s.now()) {
		s.log.Info("request dropped: outside the time limits", "event", a.event.ID, "method", req.Method)
		return nil, "", req.Method, stateIgnored
	}
	revoked, err := s.isRevoked(ctx, a.connection.id)
	if err != nil {
		s.log.Error("cannot read connection", "event", a.event.ID, "error", err)
		return failure(req.Method, codeInternal, "internal error"), scheme, req.Method, stateFailed
	}
	if revoked {
		return failure(req.Method, codeUnauthorized, "this connection is revoked"), scheme, req.Method, stateFailed
	}
	handle, known := s.methods[req.Method]
	if !known {
		return failure(req.Method, codeNotImplemented, "unknown method"), scheme, req.Method, stateFailed
	}
	result, failed := handle(ctx, a, req.Params)
	if failed == errPaymentPending {
		return &response{ResultType: req.Method, Error: failed}, scheme, req.Method, statePending
	}
	if failed != nil {
		return &response{ResultType: req.Method, Error: failed}, scheme, req.Method, stateFailed
	}
	return &response{ResultType: req.Method, Result: result}, scheme, req.Method, stateDone
}

func failure(method, code, message string) *response {
	return &response{ResultType: method, Error: &Error{Code: code, Message: message}}
}

// tooOldOrNew applies R13's time limits, which cover only what spends or commits
// the wallet. created_at is set by the client.
func tooOldOrNew(method string, event *nostr.Event, now time.Time) bool {
	if method != "pay_invoice" && method != "make_invoice" {
		return false
	}
	age := now.Unix() - int64(event.CreatedAt)
	return age > int64(maxAge/time.Second) || age < -int64(maxFuture/time.Second)
}

func (s *Service) isRevoked(ctx context.Context, id int64) (bool, error) {
	var revoked bool
	err := s.db.QueryRowContext(ctx, `SELECT revoked_at IS NOT NULL FROM connections WHERE id = ?`, id).Scan(&revoked)
	return revoked, err
}

// seal encrypts a reply and signs it with the connection's service key.
func (s *Service) seal(a *accepted, reply *response, scheme string) (*nostr.Event, error) {
	return s.sealTo(a.connection, reply, scheme, kindResponse, nostr.Tag{"e", a.event.ID})
}

// sealTo builds an event for one connection's client.
func (s *Service) sealTo(c connection, body any, scheme string, kind int, tags ...nostr.Tag) (*nostr.Event, error) {
	plain, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	sealer, err := newCipher(scheme, c.clientPub, c.serviceKey)
	if err != nil {
		return nil, err
	}
	content, err := sealer.encrypt(string(plain))
	if err != nil {
		return nil, fmt.Errorf("encrypt: %w", err)
	}
	event := nostr.Event{
		Kind: kind, CreatedAt: nostr.Timestamp(s.now().Unix()), Content: content,
		Tags: append(nostr.Tags{{"p", c.clientPub}}, tags...),
	}
	if err := event.Sign(c.serviceKey); err != nil {
		return nil, fmt.Errorf("sign: %w", err)
	}
	return &event, nil
}
