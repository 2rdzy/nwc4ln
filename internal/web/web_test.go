package web

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
)

const host = "127.0.0.1:8080"

// stubLedger returns a fixed overview, or an error.
type stubLedger struct {
	overview ledger.Overview
	err      error
	reads    atomic.Int64 // Overview calls, each a node read.
}

func (s *stubLedger) Overview(context.Context) (ledger.Overview, error) {
	s.reads.Add(1)
	return s.overview, s.err
}
func (s *stubLedger) Apps(context.Context) ([]ledger.App, error) { return nil, nil }
func (s *stubLedger) AppBudget(context.Context, int64) (*ledger.Budget, error) {
	return nil, nil
}

func (s *stubLedger) History(context.Context, ledger.HistoryQuery) ([]ledger.Transaction, int64, error) {
	return nil, 0, nil
}

func (s *stubLedger) SubWallets(context.Context) ([]ledger.SubWallet, error) { return nil, nil }
func (s *stubLedger) Accounts(context.Context) ([]ledger.Account, error)     { return nil, nil }

func (s *stubLedger) AllHistory(context.Context, ledger.AllHistoryQuery) ([]ledger.Transaction, int64, error) {
	return nil, 0, nil
}

func (s *stubLedger) Fund(context.Context, int64, int64) (ledger.Transaction, error) {
	return ledger.Transaction{}, errors.New("stub moves nothing")
}

func (s *stubLedger) Withdraw(context.Context, int64, int64) (ledger.Transaction, error) {
	return ledger.Transaction{}, errors.New("stub moves nothing")
}

type env struct {
	t      *testing.T
	server *Server
	secret string // The admin's secret key.
	now    *atomic.Int64
}

func newEnv(t *testing.T, l Ledger) *env {
	t.Helper()
	return newEnvWith(t, Config{Ledger: l, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
}

// newEnvWith sets the admin key and allowed hosts on the config, and starts the
// clock at a fixed time.
func newEnvWith(t *testing.T, cfg Config) *env {
	t.Helper()
	secret, public := nwctest.NewKey()
	cfg.AdminKey, cfg.AllowedHosts = public, []string{"nwc.example"}
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, server: server, secret: secret, now: &atomic.Int64{}}
	e.now.Store(time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC).Unix())
	server.now = func() time.Time { return time.Unix(e.now.Load(), 0) }
	return e
}

func (e *env) advance(d time.Duration) { e.now.Add(int64(d / time.Second)) }

// do sends a request as a browser on the same origin would.
func (e *env) do(method, target string, change func(*http.Request)) *http.Response {
	e.t.Helper()
	r := httptest.NewRequest(method, "http://"+host+target, nil)
	if method != http.MethodGet {
		r.Header.Set("Origin", "http://"+host)
	}
	if change != nil {
		change(r)
	}
	w := httptest.NewRecorder()
	e.server.ServeHTTP(w, r)
	return w.Result()
}

func body(t *testing.T, response *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

var challengePattern = regexp.MustCompile(`data-challenge="([0-9a-f]{64})"`)

func (e *env) challenge() string {
	e.t.Helper()
	match := challengePattern.FindStringSubmatch(body(e.t, e.do(http.MethodGet, "/login", nil)))
	if match == nil {
		e.t.Fatal("no challenge on the login page")
	}
	return match[1]
}

// authEvent describes a login event. The zero value, with a challenge, is a valid one.
type authEvent struct {
	secret    string // Default: the admin's.
	kind      int    // Default: 27235.
	age       time.Duration
	url       string // Default: the login URL with the challenge.
	method    string // Default: POST.
	extraTags [][]string
}

func (e *env) header(challenge string, event authEvent) string {
	e.t.Helper()
	if event.secret == "" {
		event.secret = e.secret
	}
	if event.kind == 0 {
		event.kind = authKind
	}
	if event.url == "" {
		event.url = "http://" + host + "/login?challenge=" + challenge
	}
	if event.method == "" {
		event.method = http.MethodPost
	}
	tags := append([][]string{{"u", event.url}, {"method", event.method}}, event.extraTags...)
	raw, err := nwctest.SignEvent(event.secret, event.kind, e.now.Load()-int64(event.age/time.Second), tags, "")
	if err != nil {
		e.t.Fatal(err)
	}
	return "Nostr " + base64.StdEncoding.EncodeToString(raw)
}

func (e *env) loginWith(challenge, header string) *http.Response {
	e.t.Helper()
	return e.do(http.MethodPost, "/login?challenge="+challenge, func(r *http.Request) {
		r.Header.Set("Authorization", header)
	})
}

// login logs in as the admin and returns the session cookie.
func (e *env) login() *http.Cookie {
	e.t.Helper()
	challenge := e.challenge()
	response := e.loginWith(challenge, e.header(challenge, authEvent{}))
	if response.StatusCode != http.StatusNoContent {
		e.t.Fatalf("login: %d %s", response.StatusCode, body(e.t, response))
	}
	for _, cookie := range response.Cookies() {
		if cookie.Name == sessionCookie {
			return cookie
		}
	}
	e.t.Fatal("no session cookie")
	return nil
}

func withCookie(cookie *http.Cookie) func(*http.Request) {
	return func(r *http.Request) { r.AddCookie(cookie) }
}

func noSessionCookie(t *testing.T, response *http.Response) {
	t.Helper()
	for _, cookie := range response.Cookies() {
		if cookie.Name == sessionCookie && cookie.Value != "" {
			t.Fatal("a refused login set a session cookie")
		}
	}
}

// Catches: a login with the wrong key, a forged or altered event, or an event made
// for another purpose (kind, URL, method, time, body) that still starts a session.
func TestLoginRefusesBadEvents(t *testing.T) {
	stranger, _ := nwctest.NewKey()
	cases := map[string]func(e *env, challenge string) string{
		"wrong key":    func(e *env, c string) string { return e.header(c, authEvent{secret: stranger}) },
		"wrong kind":   func(e *env, c string) string { return e.header(c, authEvent{kind: 27236}) },
		"wrong method": func(e *env, c string) string { return e.header(c, authEvent{method: http.MethodGet}) },
		"other URL": func(e *env, c string) string {
			return e.header(c, authEvent{url: "http://" + host + "/other?challenge=" + c})
		},
		"other host": func(e *env, c string) string {
			return e.header(c, authEvent{url: "http://nwc.example/login?challenge=" + c})
		},
		"other challenge": func(e *env, c string) string {
			return e.header(c, authEvent{url: "http://" + host + "/login?challenge=" + e.challenge()})
		},
		"two URLs": func(e *env, c string) string {
			return e.header(c, authEvent{extraTags: [][]string{{"u", "http://evil.example/"}}})
		},
		"too old":       func(e *env, c string) string { return e.header(c, authEvent{age: 61 * time.Second}) },
		"in the future": func(e *env, c string) string { return e.header(c, authEvent{age: -61 * time.Second}) },
		"wrong payload": func(e *env, c string) string {
			return e.header(c, authEvent{extraTags: [][]string{{"payload", strings.Repeat("0", 64)}}})
		},
		"bad signature": func(e *env, c string) string {
			return changeFirstDigit(t, e.header(c, authEvent{}), "sig")
		},
		// The signature check hashes the fields itself and never reads the ID, so
		// this event still has a valid signature.
		"ID does not match the content": func(e *env, c string) string {
			return changeFirstDigit(t, e.header(c, authEvent{}), "id")
		},
		"no header":  func(*env, string) string { return "" },
		"not base64": func(*env, string) string { return "Nostr !!!" },
		"other scheme": func(e *env, c string) string {
			return "Bearer " + strings.TrimPrefix(e.header(c, authEvent{}), "Nostr ")
		},
	}
	for name, header := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, &stubLedger{})
			challenge := e.challenge()
			response := e.loginWith(challenge, header(e, challenge))
			if response.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status %d", response.StatusCode)
			}
			noSessionCookie(t, response)
		})
	}
}

// alterEvent edits the base64 event in a header. With drop > 0, it also cuts that
// many characters after the edit, to keep a hex field's length.
// changeFirstDigit changes the first hex digit of a field of the event to another
// digit. Writing a fixed digit would leave the event unchanged, and valid, whenever
// the field already starts with that digit: one time in 16.
func changeFirstDigit(t *testing.T, header, field string) string {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(header, "Nostr "))
	if err != nil {
		t.Fatal(err)
	}
	text := raw
	prefix := `"` + field + `":"`
	at := strings.Index(string(text), prefix)
	if at < 0 {
		t.Fatalf("%q not in %s", prefix, text)
	}
	at += len(prefix)
	if text[at] == '0' {
		text[at] = '1'
	} else {
		text[at] = '0'
	}
	return "Nostr " + base64.StdEncoding.EncodeToString(text)
}

// Catches: a login event or challenge that can be used twice, or after it expires,
// or a challenge the server never gave.
func TestLoginRefusesReplay(t *testing.T) {
	e := newEnv(t, &stubLedger{})
	challenge := e.challenge()
	header := e.header(challenge, authEvent{})
	if response := e.loginWith(challenge, header); response.StatusCode != http.StatusNoContent {
		t.Fatalf("first login: %d", response.StatusCode)
	}
	if response := e.loginWith(challenge, header); response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("replayed login: %d", response.StatusCode)
	}

	// A failed attempt uses the challenge up too.
	challenge = e.challenge()
	stranger, _ := nwctest.NewKey()
	e.loginWith(challenge, e.header(challenge, authEvent{secret: stranger}))
	if response := e.loginWith(challenge, e.header(challenge, authEvent{})); response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("login after a failed attempt on one challenge: %d", response.StatusCode)
	}

	challenge = e.challenge()
	e.advance(challengeLife + time.Second)
	if response := e.loginWith(challenge, e.header(challenge, authEvent{})); response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired challenge: %d", response.StatusCode)
	}

	made := strings.Repeat("ab", 32)
	if response := e.loginWith(made, e.header(made, authEvent{})); response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unknown challenge: %d", response.StatusCode)
	}
}

// Catches: challenges that pile up without limit, since anyone can ask for one.
func TestChallengeCap(t *testing.T) {
	e := newEnv(t, &stubLedger{})
	first := e.challenge()
	for range maxChallenges {
		e.challenge()
	}
	if len(e.server.challenges) != maxChallenges || len(e.server.order) != maxChallenges {
		t.Fatalf("%d challenges kept", len(e.server.challenges))
	}
	if response := e.loginWith(first, e.header(first, authEvent{})); response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("the oldest challenge still works: %d", response.StatusCode)
	}
}

// routes lists every page and action. Each needs a session, except the ones marked
// public. A new route must be added here.
var routes = []struct {
	method, path string
	public       bool
}{
	{http.MethodGet, "/", false},
	{http.MethodPost, "/logout", false},
	{http.MethodGet, "/nothing-here", false},
	{http.MethodGet, "/apps", false},
	{http.MethodGet, "/apps/new", false},
	{http.MethodPost, "/apps", false},
	{http.MethodGet, "/apps/1", false},
	{http.MethodPost, "/apps/1", false},
	{http.MethodGet, "/apps/1/delete", false},
	{http.MethodPost, "/apps/1/delete", false},
	{http.MethodGet, "/subwallets", false},
	{http.MethodGet, "/subwallets/new", false},
	{http.MethodPost, "/subwallets", false},
	{http.MethodGet, "/subwallets/1", false},
	{http.MethodPost, "/subwallets/1/fund", false},
	{http.MethodPost, "/subwallets/1/withdraw", false},
	{http.MethodGet, "/subwallets/1/replace", false},
	{http.MethodPost, "/subwallets/1/replace", false},
	{http.MethodGet, "/subwallets/1/delete", false},
	{http.MethodPost, "/subwallets/1/delete", false},
	{http.MethodGet, "/transactions", false},
	{http.MethodGet, "/login", true},
	{http.MethodGet, "/static/style.css", true},
	{http.MethodGet, "/static/login.js", true},
}

const figures = "Main wallet available"

// Catches: a page that shows data, or an action that runs, without a valid session:
// none, a made-up one, or one past 24 hours.
func TestNoSessionNoData(t *testing.T) {
	e := newEnv(t, &stubLedger{})
	expired := e.login()
	e.advance(sessionLife)
	sessions := map[string]func(*http.Request){
		"no cookie":  nil,
		"made up":    withCookie(&http.Cookie{Name: sessionCookie, Value: strings.Repeat("ab", 32)}),
		"empty":      withCookie(&http.Cookie{Name: sessionCookie, Value: ""}),
		"after 24 h": withCookie(expired),
	}
	for name, session := range sessions {
		for _, route := range routes {
			if route.public {
				continue
			}
			response := e.do(route.method, route.path, session)
			text := body(t, response)
			want := http.StatusUnauthorized
			if route.method == http.MethodGet {
				want = http.StatusSeeOther
			}
			if response.StatusCode != want || strings.Contains(text, figures) {
				t.Errorf("%s: %s %s gave %d", name, route.method, route.path, response.StatusCode)
			}
			if want == http.StatusSeeOther && response.Header.Get("Location") != "/login" {
				t.Errorf("%s: %s sent to %q", name, route.path, response.Header.Get("Location"))
			}
		}
	}

	// A session works for a day, and not a second more.
	e.advance(-sessionLife)
	fresh := e.login()
	e.advance(sessionLife - time.Second)
	if response := e.do(http.MethodGet, "/", withCookie(fresh)); response.StatusCode != http.StatusOK {
		t.Fatalf("session ended early: %d", response.StatusCode)
	}
	e.advance(time.Second)
	if response := e.do(http.MethodGet, "/", withCookie(fresh)); response.StatusCode != http.StatusSeeOther {
		t.Fatalf("session outlived 24 hours: %d", response.StatusCode)
	}
}

// Catches: a public route that needs a session, which would lock the admin out.
func TestPublicRoutes(t *testing.T) {
	e := newEnv(t, &stubLedger{})
	for _, route := range routes {
		if !route.public {
			continue
		}
		if response := e.do(route.method, route.path, nil); response.StatusCode != http.StatusOK {
			t.Errorf("%s %s: %d", route.method, route.path, response.StatusCode)
		}
	}
}

// Catches: DNS rebinding, where a web page under another name reaches the UI on
// loopback; and a request from another site, which the browser sends with the
// admin's cookie unless SameSite stops it.
func TestHostAndOrigin(t *testing.T) {
	e := newEnv(t, &stubLedger{})
	cookie := e.login()
	for _, bad := range []string{"evil.example", "evil.example:8080", "127.0.0.1.evil.example", "", "localhost.:8080"} {
		for _, route := range routes {
			response := e.do(route.method, route.path, func(r *http.Request) {
				r.Host = bad
				r.Header.Set("Origin", "http://"+bad)
				r.AddCookie(cookie)
			})
			if response.StatusCode != http.StatusForbidden {
				t.Errorf("host %q: %s %s gave %d", bad, route.method, route.path, response.StatusCode)
			}
		}
	}
	for _, good := range []string{"localhost:9000", "[::1]:8080", "127.0.0.1", "NWC.example"} {
		response := e.do(http.MethodGet, "/", func(r *http.Request) { r.Host = good; r.AddCookie(cookie) })
		if response.StatusCode != http.StatusOK {
			t.Errorf("host %q: %d", good, response.StatusCode)
		}
	}

	for _, origin := range []string{"", "null", "http://evil.example", "http://127.0.0.1:9999", "file://", "http://" + host + "/x"} {
		challenge := e.challenge()
		header := e.header(challenge, authEvent{})
		set := func(r *http.Request) {
			r.Header.Set("Origin", origin)
			if origin == "" {
				r.Header.Del("Origin")
			}
		}
		response := e.do(http.MethodPost, "/login?challenge="+challenge, func(r *http.Request) {
			r.Header.Set("Authorization", header)
			set(r)
		})
		if response.StatusCode != http.StatusForbidden {
			t.Errorf("login from origin %q: %d", origin, response.StatusCode)
		}
		noSessionCookie(t, response)
		response = e.do(http.MethodPost, "/logout", func(r *http.Request) { r.AddCookie(cookie); set(r) })
		if response.StatusCode != http.StatusForbidden {
			t.Errorf("logout from origin %q: %d", origin, response.StatusCode)
		}
	}
	if response := e.do(http.MethodGet, "/", withCookie(cookie)); response.StatusCode != http.StatusOK {
		t.Fatalf("a cross-site logout ended the session: %d", response.StatusCode)
	}
}

// Catches: a session cookie that scripts can read, that other sites' requests carry,
// or that lasts longer than a day; pages that can be framed or cached; and a logout
// that leaves the session alive on the server.
func TestCookieHeadersAndLogout(t *testing.T) {
	e := newEnv(t, &stubLedger{})
	cookie := e.login()
	if !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode || cookie.MaxAge != 86_400 || cookie.Path != "/" {
		t.Fatalf("cookie %+v", cookie)
	}
	response := e.do(http.MethodGet, "/", withCookie(cookie))
	for name, want := range map[string]string{
		"Content-Security-Policy": "frame-ancestors 'none'",
		"X-Frame-Options":         "DENY",
		"Cache-Control":           "no-store",
		// no-referrer makes browsers send "Origin: null" on form POSTs, which the
		// origin check refuses: logout and every later form would fail.
		"Referrer-Policy": "same-origin",
	} {
		if got := response.Header.Get(name); !strings.Contains(got, want) || (name == "Referrer-Policy" && got != want) {
			t.Errorf("%s: %q", name, response.Header.Get(name))
		}
	}
	if csp := response.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'self'") ||
		strings.Contains(csp, "unsafe") {
		t.Errorf("CSP %q", csp)
	}

	response = e.do(http.MethodPost, "/logout", withCookie(cookie))
	if response.StatusCode != http.StatusSeeOther {
		t.Fatalf("logout: %d", response.StatusCode)
	}
	if response := e.do(http.MethodGet, "/", withCookie(cookie)); response.StatusCode != http.StatusSeeOther {
		t.Fatalf("old cookie still works after logout: %d", response.StatusCode)
	}
}

// Catches: a login that works with no admin key set, for example with an empty key.
func TestNoAdminKey(t *testing.T) {
	e := newEnv(t, &stubLedger{})
	e.server.adminKey = ""
	page := body(t, e.do(http.MethodGet, "/login", nil))
	if !strings.Contains(page, "set-admin") || challengePattern.MatchString(page) {
		t.Fatalf("login page with no admin:\n%s", page)
	}
	// A challenge given before is still refused.
	e.server.adminKey = "x"
	challenge := e.challenge()
	e.server.adminKey = ""
	if response := e.loginWith(challenge, e.header(challenge, authEvent{})); response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("login with no admin key: %d", response.StatusCode)
	}
}

func figure(t *testing.T, page, id string) string {
	t.Helper()
	match := regexp.MustCompile(`id="` + id + `">(?:<b>)?([^<]*)<`).FindStringSubmatch(page)
	if match == nil {
		t.Fatalf("no figure %q in:\n%s", id, page)
	}
	return match[1]
}

// Catches: figures rounded the wrong way, so that a shortfall of under a sat or a
// main wallet just below 0 shows as nothing; and a missing node figure shown as 0.
func TestDashboardRounding(t *testing.T) {
	l := &stubLedger{overview: ledger.Overview{
		NodeSpendableMsat: 1_234_567_999, SubWalletMsat: 1_000_001, HoldsMsat: 1,
		MainAvailableMsat: -1, ShortfallMsat: 1,
	}}
	e := newEnv(t, l)
	cookie := e.login()
	page := body(t, e.do(http.MethodGet, "/", withCookie(cookie)))
	for id, want := range map[string]string{
		"node": "1,234,567 sat", "subwallets": "1,001 sat", "holds": "1 sat", "main": "-1 sat",
	} {
		if got := figure(t, page, id); got != want {
			t.Errorf("%s: %q, want %q", id, got, want)
		}
	}
	if !strings.Contains(page, `id="shortfall"><b>Shortfall: 1 sat.`) || !strings.Contains(page, `id="negative"`) {
		t.Errorf("warnings missing:\n%s", page)
	}

	l.err = errors.New("node down")
	page = body(t, e.do(http.MethodGet, "/", withCookie(cookie)))
	for _, id := range []string{"node", "subwallets", "holds", "main"} {
		if got := figure(t, page, id); got != "unknown" {
			t.Errorf("%s: %q with the node down", id, got)
		}
	}
	if !strings.Contains(page, `id="unknown"`) || strings.Contains(page, `id="shortfall"`) {
		t.Errorf("warnings with the node down:\n%s", page)
	}
}

// Catches: a sats figure written wrongly, such as a lost sign or misplaced commas.
func TestFormatSats(t *testing.T) {
	for msat, want := range map[int64]string{
		0: "0 sat", 999: "0 sat", 1_000: "1 sat", -1: "-1 sat", -1_000: "-1 sat", -1_001: "-2 sat",
		123_456_789_000: "123,456,789 sat", -1_000_000_000: "-1,000,000 sat",
	} {
		if got := formatSats(floorSats(msat)); got != want {
			t.Errorf("floor %d: %q, want %q", msat, got, want)
		}
	}
	for msat, want := range map[int64]int64{0: 0, 1: 1, 999: 1, 1_000: 1, 1_001: 2, -1: 0, -1_999: -1} {
		if got := ceilSats(msat); got != want {
			t.Errorf("ceil %d: %d, want %d", msat, got, want)
		}
	}
}
