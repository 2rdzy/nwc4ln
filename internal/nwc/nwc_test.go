package nwc

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getAlby/go-nostr"

	"github.com/2rdzy/nwc4ln/internal/node/fake"
	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
	"github.com/2rdzy/nwc4ln/internal/store"
)

const relayURL = "ws://127.0.0.1:1"

type env struct {
	t       *testing.T
	store   *store.Store
	path    string
	node    *fake.Node
	service *Service
	client  *nwctest.Client
	now     *atomic.Int64
}

func newEnv(t *testing.T) *env {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := store.Create(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	e := &env{t: t, store: db, path: path, node: fake.New(), now: &atomic.Int64{}}
	e.now.Store(time.Now().Unix())
	e.client = e.addApp("first")
	e.restart()
	return e
}

func (e *env) addApp(name string) *nwctest.Client {
	e.t.Helper()
	connectionString, err := AddApp(e.t.Context(), e.store, name, relayURL, false)
	if err != nil {
		e.t.Fatal(err)
	}
	client, err := nwctest.Parse(connectionString)
	if err != nil {
		e.t.Fatal(err)
	}
	return client
}

// restart builds a new service, which loads the connections again.
func (e *env) restart() {
	e.t.Helper()
	service, err := New(e.t.Context(), e.store, e.node, relayURL, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		e.t.Fatal(err)
	}
	service.now = func() time.Time { return time.Unix(e.now.Load(), 0) }
	e.service = service
}

func (e *env) timestamp() nostr.Timestamp { return nostr.Timestamp(e.now.Load()) }

func (e *env) request(scheme, method string, tags ...nostr.Tag) *nostr.Event {
	e.t.Helper()
	event, err := e.client.Request(scheme, method, e.timestamp(), tags...)
	if err != nil {
		e.t.Fatal(err)
	}
	return event
}

// handle does what the relay loop does with one event, and returns the reply.
func (e *env) handle(event *nostr.Event) *nostr.Event {
	e.t.Helper()
	request := e.service.accept(e.t.Context(), event)
	if request == nil {
		return nil
	}
	return e.service.run(e.t.Context(), request)
}

// answer sends a request that must get a reply, and reads the reply.
func (e *env) answer(event *nostr.Event, scheme string) *nwctest.Response {
	e.t.Helper()
	reply := e.handle(event)
	if reply == nil {
		e.t.Fatal("no reply")
	}
	response, err := e.client.Open(scheme, event, reply)
	if err != nil {
		e.t.Fatal(err)
	}
	return response
}

func (e *env) requests() int {
	e.t.Helper()
	var count int
	if err := e.store.DB.QueryRow(`SELECT count(*) FROM requests`).Scan(&count); err != nil {
		e.t.Fatal(err)
	}
	return count
}

func (e *env) requestState(id string) string {
	e.t.Helper()
	var state string
	err := e.store.DB.QueryRow(`SELECT state FROM requests WHERE nostr_event_id = ?`, id).Scan(&state)
	if err != nil {
		e.t.Fatal(err)
	}
	return state
}

func wantCode(t *testing.T, response *nwctest.Response, code string) {
	t.Helper()
	if response.Error == nil || response.Error.Code != code {
		t.Fatalf("got %+v, want error %s", response, code)
	}
	if string(response.Result) != "null" && len(response.Result) != 0 {
		t.Fatalf("error with a result: %s", response.Result)
	}
}

// Catches: double payment.
func TestSameEventRunsOnce(t *testing.T) {
	e := newEnv(t)
	event := e.request(nwctest.NIP44, "get_info")
	stored := e.service.accept(t.Context(), event)
	if stored == nil {
		t.Fatal("first delivery dropped")
	}
	// Stored and not yet run is the state a crash leaves. It must stay that way.
	if e.service.accept(t.Context(), event) != nil {
		t.Fatal("repeat accepted while the first is running")
	}
	if e.service.run(t.Context(), stored) == nil {
		t.Fatal("no reply")
	}
	if e.handle(event) != nil {
		t.Fatal("repeat accepted after the first finished")
	}
	e.restart()
	if e.handle(event) != nil {
		t.Fatal("repeat accepted after a restart")
	}
	if got := e.requests(); got != 1 {
		t.Fatalf("%d rows", got)
	}
}

// Catches: a relay getting a request past the unique index under a new id. The
// signature does not cover the id field, so it stays valid.
func TestChangedIDIsDropped(t *testing.T) {
	e := newEnv(t)
	event := e.request(nwctest.NIP44, "get_info")
	if e.handle(event) == nil {
		t.Fatal("no reply")
	}
	changed := *event
	changed.ID = "00" + event.ID[2:]
	if valid, err := changed.CheckSignature(); err != nil || !valid {
		t.Fatalf("this test needs a signature that still passes: %v %v", valid, err)
	}
	if e.handle(&changed) != nil {
		t.Fatal("changed id ran again")
	}
	if got := e.requests(); got != 1 {
		t.Fatalf("%d rows", got)
	}
}

// Catches: a leaked service key letting someone spend. Such a person can encrypt
// correctly, so only the signature stops them.
func TestBadSignatureIsDropped(t *testing.T) {
	e := newEnv(t)
	event := e.request(nwctest.NIP44, "get_info")
	flipped, err := hex.DecodeString(event.Sig)
	if err != nil {
		t.Fatal(err)
	}
	flipped[63] ^= 1
	event.Sig = hex.EncodeToString(flipped)
	if !event.CheckID() {
		t.Fatal("the id must stay right, so that only the signature can fail")
	}
	if e.handle(event) != nil {
		t.Fatal("bad signature served")
	}
	if got := e.requests(); got != 0 {
		t.Fatalf("%d rows", got)
	}
}

// Catches: a stale payment running.
func TestExpiration(t *testing.T) {
	e := newEnv(t)
	past := strconv.FormatInt(e.now.Load()-1, 10)
	future := strconv.FormatInt(e.now.Load()+60, 10)
	for name, tag := range map[string]nostr.Tag{"past": {"expiration", past}, "unreadable": {"expiration", "soon"}} {
		if e.handle(e.request(nwctest.NIP44, "get_info", tag)) != nil {
			t.Errorf("%s: served", name)
		}
	}
	if got := e.requests(); got != 0 {
		t.Fatalf("%d rows", got)
	}
	if e.handle(e.request(nwctest.NIP44, "get_info", nostr.Tag{"expiration", future})) == nil {
		t.Fatal("request not yet expired was dropped")
	}
}

// Catches: an old payment running after downtime, when the client may have paid
// another way.
func TestTimeLimits(t *testing.T) {
	e := newEnv(t)
	now := e.now.Load()
	tests := []struct {
		name    string
		method  string
		created int64
		served  bool
	}{
		{"pay too old", "pay_invoice", now - 6*3600 - 1, false},
		{"pay at the limit", "pay_invoice", now - 6*3600, true},
		{"pay too new", "pay_invoice", now + 601, false},
		{"pay near the future limit", "pay_invoice", now + 600, true},
		{"invoice too old", "make_invoice", now - 6*3600 - 1, false},
		{"invoice too new", "make_invoice", now + 601, false},
		{"read is not limited", "get_info", now - 7*24*3600, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event, err := e.client.Request(nwctest.NIP44, test.method, nostr.Timestamp(test.created))
			if err != nil {
				t.Fatal(err)
			}
			reply := e.handle(event)
			if served := reply != nil; served != test.served {
				t.Fatalf("served %v", served)
			}
			if !test.served {
				if got := e.requestState(event.ID); got != stateIgnored {
					t.Fatalf("state %q", got)
				}
			}
		})
	}
}

// Catches: a client that cannot talk to us.
func TestEachEncryption(t *testing.T) {
	e := newEnv(t)
	for _, scheme := range []string{nwctest.NIP44, nwctest.NIP04, nwctest.NoTag} {
		response := e.answer(e.request(scheme, "get_info"), scheme)
		if response.Error != nil || response.ResultType != "get_info" {
			t.Errorf("scheme %q: %+v", scheme, response)
		}
	}
}

// Catches: the wrong error, or a reply the client cannot read.
func TestUnsupportedEncryption(t *testing.T) {
	e := newEnv(t)
	content, err := e.client.Encrypt(nwctest.NIP04, `{"method":"get_info"}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, tag := range []nostr.Tag{{"encryption", "nip99"}, {"encryption"}} {
		event, err := e.client.Seal(content, e.timestamp(), tag)
		if err != nil {
			t.Fatal(err)
		}
		wantCode(t, e.answer(event, nwctest.NIP44), codeUnsupportedEncryption)
	}
}

// Catches: a crash or silence on a request we cannot read.
func TestUnreadableRequest(t *testing.T) {
	e := newEnv(t)
	for _, scheme := range []string{nwctest.NIP44, nwctest.NIP04} {
		tag := nostr.Tag{"encryption", scheme}
		broken, err := e.client.Seal("not encrypted", e.timestamp(), tag)
		if err != nil {
			t.Fatal(err)
		}
		wantCode(t, e.answer(broken, scheme), codeOther)
		for _, plain := range []string{"not json", `{}`, `{"method":""}`, `[]`} {
			content, err := e.client.Encrypt(scheme, plain)
			if err != nil {
				t.Fatal(err)
			}
			event, err := e.client.Seal(content, e.timestamp(), tag)
			if err != nil {
				t.Fatal(err)
			}
			wantCode(t, e.answer(event, scheme), codeOther)
		}
	}
}

// Catches: a stranger being served, or filling the table. The service key is
// public, so anyone can address an event to it.
func TestWrongKeyGetsNothing(t *testing.T) {
	e := newEnv(t)
	stranger, err := e.client.Stranger()
	if err != nil {
		t.Fatal(err)
	}
	second := e.addApp("second")
	e.restart()
	// The second client is real, but this is the first one's service key.
	second.ServicePub = e.client.ServicePub
	for name, client := range map[string]*nwctest.Client{"stranger": stranger, "another connection": second} {
		event, err := client.Request(nwctest.NIP44, "get_info", e.timestamp())
		if err != nil {
			t.Fatal(err)
		}
		if e.handle(event) != nil {
			t.Errorf("%s: served", name)
		}
	}
	if got := e.requests(); got != 0 {
		t.Fatalf("%d rows", got)
	}
}

// Catches: a revoked key still being served.
func TestRevokedConnection(t *testing.T) {
	e := newEnv(t)
	if e.answer(e.request(nwctest.NIP44, "get_info"), nwctest.NIP44).Error != nil {
		t.Fatal("not served before revoking")
	}
	// The service does not restart: revoking must work at once.
	if _, err := e.store.DB.Exec(`UPDATE connections SET revoked_at = 1`); err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"get_info", "no_such_method"} {
		response := e.answer(e.request(nwctest.NIP44, method), nwctest.NIP44)
		wantCode(t, response, codeUnauthorized)
		if response.ResultType != method {
			t.Errorf("result_type %q", response.ResultType)
		}
	}
}

// Catches: the wrong error for each method.
func TestUnknownMethod(t *testing.T) {
	e := newEnv(t)
	for _, method := range []string{"no_such_method", "pay_invoice"} {
		response := e.answer(e.request(nwctest.NIP44, method), nwctest.NIP44)
		wantCode(t, response, codeNotImplemented)
		if response.ResultType != method {
			t.Errorf("result_type %q", response.ResultType)
		}
	}
}

// Catches: wrong field names, and a false answer when the node is down.
func TestGetInfo(t *testing.T) {
	e := newEnv(t)
	want, err := e.node.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Alias       string   `json:"alias"`
		Color       string   `json:"color"`
		PubKey      string   `json:"pubkey"`
		Network     string   `json:"network"`
		BlockHeight uint32   `json:"block_height"`
		BlockHash   string   `json:"block_hash"`
		Methods     []string `json:"methods"`
	}
	response := e.answer(e.request(nwctest.NIP44, "get_info"), nwctest.NIP44)
	if err := json.Unmarshal(response.Result, &got); err != nil {
		t.Fatal(err)
	}
	if got.PubKey != want.PubKey || got.Alias != want.Alias || got.Network != want.Network ||
		got.BlockHeight != want.BlockHeight || got.BlockHash != want.BlockHash || got.Color != want.Color {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if !slices.Equal(got.Methods, []string{"get_info"}) {
		t.Errorf("methods %v", got.Methods)
	}
	e.node.SetUnreachable(true)
	wantCode(t, e.answer(e.request(nwctest.NIP44, "get_info"), nwctest.NIP44), codeInternal)
}

// Catches: a client calling what does not work, because we advertised it.
func TestInfoEvent(t *testing.T) {
	e := newEnv(t)
	c := e.service.connections[e.client.ServicePub]
	event, err := e.service.infoEvent(c)
	if err != nil {
		t.Fatal(err)
	}
	if event.Kind != kindInfo || event.PubKey != e.client.ServicePub || event.Content != "get_info" {
		t.Fatalf("got %+v", event)
	}
	if valid, err := event.CheckSignature(); err != nil || !valid {
		t.Fatalf("signature: %v %v", valid, err)
	}
	if got, _ := tagValue(event, "encryption"); got != "nip44_v2 nip04" {
		t.Fatalf("encryption %q", got)
	}
	if len(event.Tags) != 1 {
		t.Fatalf("advertises more than works today: %v", event.Tags)
	}
}

// Catches: a stored client secret leaking through a backup. The raw files are
// read, so that a copy in any column, or in the WAL, is found.
func TestClientSecretNotStored(t *testing.T) {
	e := newEnv(t)
	if e.handle(e.request(nwctest.NIP44, "get_info")) == nil {
		t.Fatal("no reply")
	}
	raw, err := hex.DecodeString(e.client.Secret)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, path := range []string{e.path, e.path + "-wal"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		found += len(data)
		if bytes.Contains(data, []byte(e.client.Secret)) || bytes.Contains(data, raw) {
			t.Errorf("client secret found in %s", path)
		}
		// Proves the search can find something: the public key is stored.
		if path == e.path+"-wal" && !bytes.Contains(data, []byte(e.client.Public)) {
			t.Errorf("client public key not in the WAL, so this test looks in the wrong place")
		}
	}
	if found == 0 {
		t.Fatal("nothing to search")
	}
}

// Catches: an app with rights nobody gave it.
func TestAddAppPermissions(t *testing.T) {
	e := newEnv(t)
	if _, err := AddApp(t.Context(), e.store, "second", relayURL, true); err != nil {
		t.Fatal(err)
	}
	rows, err := e.store.DB.Query(`SELECT can_pay, can_invoice, limit_msat FROM accounts ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var got [][2]bool
	for rows.Next() {
		var pay, invoice bool
		var limit sql.NullInt64
		if err := rows.Scan(&pay, &invoice, &limit); err != nil {
			t.Fatal(err)
		}
		if limit.Valid {
			t.Errorf("limit %d", limit.Int64)
		}
		got = append(got, [2]bool{pay, invoice})
	}
	if !slices.Equal(got, [][2]bool{{true, false}, {true, true}}) {
		t.Fatalf("got %v", got)
	}
}

// Catches: the service skipping requests, because one client's clock moved the
// place it subscribes from.
func TestSince(t *testing.T) {
	e := newEnv(t)
	margin := int64(sinceMargin / time.Second)
	want := func(name string, expected int64) {
		t.Helper()
		got, err := e.service.since(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if int64(got) != expected {
			t.Errorf("%s: got %d, want %d", name, int64(got), expected)
		}
	}
	want("first start", e.now.Load()-margin)

	event, err := e.client.Request(nwctest.NIP44, "get_info", e.timestamp()+3600)
	if err != nil {
		t.Fatal(err)
	}
	if e.handle(event) == nil {
		t.Fatal("no reply")
	}
	seen := e.now.Load()
	want("client clock an hour ahead", seen-margin)

	e.now.Add(120)
	want("two minutes later", seen-margin)

	e.now.Add(7 * 24 * 3600)
	want("a week down", e.now.Load()-int64(maxAge/time.Second))

	e.now.Store(seen - 3600)
	want("our clock moved back", e.now.Load()-margin)
}

// Catches: an answer lost in a short relay outage.
func TestSendRetries(t *testing.T) {
	e := newEnv(t)
	e.service.sendFor, e.service.sendRetry = time.Minute, time.Millisecond
	var calls atomic.Int32
	e.service.publish = func(context.Context, nostr.Event) error {
		if calls.Add(1) < 3 {
			return errors.New("relay is down")
		}
		return nil
	}
	if err := e.service.send(t.Context(), nostr.Event{}); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("%d calls", got)
	}

	e.service.sendFor = 0
	calls.Store(-100)
	if err := e.service.send(t.Context(), nostr.Event{}); err == nil {
		t.Fatal("a send that cannot work must end with an error")
	}
}
