package web

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/skip2/go-qrcode"

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/node/fake"
	"github.com/2rdzy/nwc4ln/internal/nwc"
	"github.com/2rdzy/nwc4ln/internal/store"
)

// appsEnv is the UI on a real database, ledger and NWC service, with a fake node.
type appsEnv struct {
	*env
	cookie  *http.Cookie
	ledger  *ledger.Ledger
	service *nwc.Service
	node    *fake.Node
	db      *store.Store
	dbPath  string
	log     *bytes.Buffer
}

func newAppsEnv(t *testing.T) *appsEnv {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "nwc4ln.db")
	db, err := store.Create(t.Context(), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	n := fake.New()
	n.SetSpendable(node.Spendable{ActiveMsat: 1_000_000_000, AllMsat: 1_000_000_000})
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	l := ledger.New(db, n, log)
	service, err := nwc.New(t.Context(), db, n, l, "ws://127.0.0.1:1", log)
	if err != nil {
		t.Fatal(err)
	}
	e := newEnvWith(t, Config{Ledger: l, Apps: service, SubWallets: service, Log: log})
	return &appsEnv{env: e, cookie: e.login(), ledger: l, service: service, node: n, db: db, dbPath: dbPath, log: &logs}
}

// post sends a form as the logged-in admin's browser would.
func (e *appsEnv) post(target string, form url.Values) *http.Response {
	e.t.Helper()
	r := httptest.NewRequest(http.MethodPost, "http://"+host+target, strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Origin", "http://"+host)
	r.AddCookie(e.cookie)
	w := httptest.NewRecorder()
	e.server.ServeHTTP(w, r)
	return w.Result()
}

func (e *appsEnv) get(target string) (*http.Response, string) {
	e.t.Helper()
	response := e.do(http.MethodGet, target, withCookie(e.cookie))
	return response, body(e.t, response)
}

func (e *appsEnv) apps() []ledger.App {
	e.t.Helper()
	apps, err := e.ledger.Apps(e.t.Context())
	if err != nil {
		e.t.Fatal(err)
	}
	return apps
}

var connectionPattern = regexp.MustCompile(`nostr(?:\+|&#43;)walletconnect://[0-9a-f]{64}\?relay=[^&<\s]+&amp;secret=([0-9a-f]{64})`)

// create adds an app through the form and returns the page and the client secret.
func (e *appsEnv) create(form url.Values) (string, string) {
	e.t.Helper()
	response := e.post("/apps", form)
	page := body(e.t, response)
	if response.StatusCode != http.StatusOK {
		e.t.Fatalf("create: %d\n%s", response.StatusCode, page)
	}
	match := connectionPattern.FindStringSubmatch(page)
	if match == nil {
		e.t.Fatalf("no connection string:\n%s", page)
	}
	return page, match[1]
}

func limitOf(app ledger.App) string {
	if app.LimitMsat == nil {
		return "none"
	}
	return strconv.FormatInt(*app.LimitMsat, 10)
}

// Catches: the UI storing other settings than the form says, no limit and a limit
// of 0 taken for each other, a checkbox read the wrong way, an expiry a day off, a
// change not passed to the running service, and a delete that leaves the key live.
func TestAppCreateEditDelete(t *testing.T) {
	e := newAppsEnv(t)
	e.create(url.Values{"name": {"shop"}, "limit": {"0"}, "renewal": {"weekly"}, "expires": {"2026-12-31"},
		"can_invoice": {"on"}})
	apps := e.apps()
	if len(apps) != 1 {
		t.Fatalf("%d apps", len(apps))
	}
	app := apps[0]
	if app.Name != "shop" || limitOf(app) != "0" || app.Renewal != ledger.RenewWeekly || app.CanPay || !app.CanInvoice ||
		!app.ExpiresAt.Equal(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("stored %+v, limit %s", app, limitOf(app))
	}
	// The service serves it at once: the ledger sees the account as usable.
	if _, err := e.ledger.Access(t.Context(), app.ID); err != nil {
		t.Fatalf("access: %v", err)
	}
	path := fmt.Sprintf("/apps/%d", app.ID)
	_, page := e.get(path)
	for _, want := range []string{`name="limit" inputmode="numeric" pattern="[0-9]*" value="0"`, `value="2026-12-31"`,
		`<option value="weekly" selected>`, `id="limit">0 sat weekly`, `id="left">0 sat`} {
		if !strings.Contains(page, want) {
			t.Errorf("app page lacks %q", want)
		}
	}
	if strings.Contains(page, `name="can_pay" checked`) || !strings.Contains(page, `name="can_invoice" checked`) {
		t.Errorf("checkboxes wrong:\n%s", page)
	}

	response := e.post(path, url.Values{"limit": {""}, "renewal": {"never"}, "expires": {""}, "can_pay": {"on"}})
	if response.StatusCode != http.StatusSeeOther || response.Header.Get("Location") != path {
		t.Fatalf("edit: %d %s", response.StatusCode, body(t, response))
	}
	app = e.apps()[0]
	if limitOf(app) != "none" || app.Renewal != ledger.RenewNever || !app.CanPay || app.CanInvoice || !app.ExpiresAt.IsZero() {
		t.Fatalf("after edit %+v, limit %s", app, limitOf(app))
	}
	if _, page := e.get("/apps"); !strings.Contains(page, fmt.Sprintf(`id="left-%d">no limit`, app.ID)) {
		t.Errorf("list does not say no limit:\n%s", page)
	}
	e.post(path, url.Values{"limit": {"5"}, "renewal": {"daily"}, "can_pay": {"on"}})
	if app = e.apps()[0]; limitOf(app) != "5000" {
		t.Fatalf("limit 5 sat stored as %s msat", limitOf(app))
	}

	if response, _ := e.get(path + "/delete"); response.StatusCode != http.StatusOK || len(e.apps()) != 1 {
		t.Fatalf("delete page: %d, or it deleted the app", response.StatusCode)
	}
	response = e.post(path+"/delete", nil)
	if response.StatusCode != http.StatusSeeOther || response.Header.Get("Location") != "/apps" {
		t.Fatalf("delete: %d", response.StatusCode)
	}
	if apps := e.apps(); len(apps) != 0 {
		t.Fatalf("still listed: %+v", apps)
	}
	if _, err := e.ledger.Access(t.Context(), app.ID); err == nil {
		t.Fatal("a deleted app can still be used")
	}
	if response, _ := e.get(path); response.StatusCode != http.StatusNotFound {
		t.Fatalf("deleted app page: %d", response.StatusCode)
	}
}

// Catches: unpaid rows left off the app page, which shows only settled ones by
// default, so a pending payment that holds the limit cannot be seen.
func TestAppPageShowsPendingRows(t *testing.T) {
	e := newAppsEnv(t)
	e.create(url.Values{"name": {"shop"}, "renewal": {"never"}, "can_pay": {"on"}, "can_invoice": {"on"}})
	id := e.apps()[0].ID
	if err := e.ledger.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.ledger.Close)
	if _, err := e.ledger.MakeInvoice(t.Context(), ledger.InvoiceRequest{
		AccountID: id, AmountMsat: 21_500, Description: "coffee <b>",
	}); err != nil {
		t.Fatal(err)
	}
	_, page := e.get(fmt.Sprintf("/apps/%d", id))
	if !strings.Contains(page, "<td>incoming</td><td>pending</td><td class=\"n\">21.5 sat</td>") ||
		!strings.Contains(page, "coffee &lt;b&gt;") {
		t.Fatalf("pending invoice missing or not escaped:\n%s", page)
	}
}

// Catches: limit figures rounded the wrong way, or a limit cut below usage shown as
// a negative amount left instead of 0.
func TestAppFigures(t *testing.T) {
	limit := func(msat int64) *int64 { return &msat }
	for _, c := range []struct {
		limit, used, left, usedSats int64
		over                        bool
	}{
		{1_500, 501, 0, 1, false},
		{10_999, 0, 10, 0, false},
		{1_000, 2_000, 0, 2, true},
		{0, 0, 0, 0, false},
	} {
		app := appView{App: ledger.App{AppSettings: ledger.AppSettings{LimitMsat: limit(c.limit)}, UsedMsat: c.used}}
		if app.Left() != c.left || app.Used() != c.usedSats || app.OverLimit() != c.over {
			t.Errorf("limit %d used %d: left %d used %d over %v", c.limit, c.used, app.Left(), app.Used(), app.OverLimit())
		}
	}
}

// Catches: a form value that is stored although it is wrong, such as a negative or
// fractional limit, one so large the msat figure would overflow, an unknown renewal,
// or a date that does not exist.
func TestAppFormRefusesBadValues(t *testing.T) {
	e := newAppsEnv(t)
	maxSats := strconv.FormatInt(ledger.MaxAmountMsat/msatPerSat, 10)
	good := func() url.Values {
		return url.Values{"name": {"shop"}, "limit": {"10"}, "renewal": {"monthly"}, "can_pay": {"on"}}
	}
	cases := map[string]func(url.Values){
		"no name":          func(v url.Values) { v.Set("name", "  ") },
		"negative limit":   func(v url.Values) { v.Set("limit", "-1") },
		"fractional limit": func(v url.Values) { v.Set("limit", "1.5") },
		"exponent":         func(v url.Values) { v.Set("limit", "1e3") },
		"text limit":       func(v url.Values) { v.Set("limit", "ten") },
		"over the maximum": func(v url.Values) { v.Set("limit", maxSats+"1") },
		// 18446744073709552 sats is 384 msat after the multiplication wraps.
		"wraps msat":  func(v url.Values) { v.Set("limit", "18446744073709552") },
		"over int64":  func(v url.Values) { v.Set("limit", "9223372036854775808") },
		"hourly":      func(v url.Values) { v.Set("renewal", "hourly") },
		"no renewal":  func(v url.Values) { v.Del("renewal") },
		"day first":   func(v url.Values) { v.Set("expires", "31/12/2026") },
		"30 February": func(v url.Values) { v.Set("expires", "2026-02-30") },
	}
	for name, change := range cases {
		form := good()
		change(form)
		if response := e.post("/apps", form); response.StatusCode != http.StatusBadRequest {
			t.Errorf("create, %s: %d", name, response.StatusCode)
		}
	}
	if apps := e.apps(); len(apps) != 0 {
		t.Fatalf("bad forms stored apps: %+v", apps)
	}

	form := good()
	form.Set("limit", maxSats)
	e.create(form)
	app := e.apps()[0]
	if limitOf(app) != strconv.FormatInt(ledger.MaxAmountMsat, 10) {
		t.Fatalf("maximum limit stored as %s", limitOf(app))
	}
	path := fmt.Sprintf("/apps/%d", app.ID)
	for name, change := range cases {
		if name == "no name" {
			continue // Editing does not change the name.
		}
		form := good()
		change(form)
		if response := e.post(path, form); response.StatusCode != http.StatusBadRequest {
			t.Errorf("edit, %s: %d", name, response.StatusCode)
		}
	}
	if after := e.apps()[0]; limitOf(after) != limitOf(app) || after.Renewal != app.Renewal {
		t.Fatalf("a bad edit changed the app: %+v", after)
	}
}

// Catches: the client secret shown a second time, or kept anywhere: in a later page,
// the log or the database. Only the create response may hold it, and it must not be
// cached.
func TestConnectionStringShownOnce(t *testing.T) {
	e := newAppsEnv(t)
	response := e.post("/apps", url.Values{"name": {"shop"}, "renewal": {"never"}, "can_pay": {"on"}})
	page := body(t, response)
	if response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("create response may be cached: %q", response.Header.Get("Cache-Control"))
	}
	match := connectionPattern.FindStringSubmatch(page)
	if match == nil || !strings.Contains(page, `<div id="qr"><svg`) {
		t.Fatalf("create page lacks the string or the QR code:\n%s", page)
	}
	secret := match[1]
	id := e.apps()[0].ID
	for _, path := range []string{"/", "/apps", "/apps/new", fmt.Sprintf("/apps/%d", id), fmt.Sprintf("/apps/%d/delete", id)} {
		if response, page := e.get(path); response.StatusCode != http.StatusOK || strings.Contains(page, secret) ||
			strings.Contains(page, "walletconnect") {
			t.Errorf("%s: %d, or shows the connection string", path, response.StatusCode)
		}
	}
	if strings.Contains(e.log.String(), secret) {
		t.Error("the log holds the secret")
	}
	for _, file := range []string{e.dbPath, e.dbPath + "-wal"} {
		data, err := os.ReadFile(file)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte(secret)) {
			t.Errorf("%s holds the secret", filepath.Base(file))
		}
	}
}

// Catches: a sub-wallet changed or deleted through the app pages, which would give it
// a limit or drop its balance without the sub-wallet rules; and a deleted or made-up
// ID that reaches a change.
func TestAppPagesOnlyServeApps(t *testing.T) {
	e := newAppsEnv(t)
	// With an app in the list, a lookup that matches the wrong row shows up.
	e.create(url.Values{"name": {"shop"}, "renewal": {"never"}, "can_pay": {"on"}})
	if _, err := e.service.AddSubWallet(t.Context(), "alice"); err != nil {
		t.Fatal(err)
	}
	subWallets, err := e.ledger.SubWallets(t.Context())
	if err != nil || len(subWallets) != 1 {
		t.Fatalf("%v %+v", err, subWallets)
	}
	sub := strconv.FormatInt(subWallets[0].ID, 10)
	for _, id := range []string{sub, "999", "abc", "-1"} {
		for _, target := range []string{"/apps/" + id, "/apps/" + id + "/delete"} {
			if response, _ := e.get(target); response.StatusCode != http.StatusNotFound {
				t.Errorf("GET %s: %d", target, response.StatusCode)
			}
			form := url.Values{"limit": {"0"}, "renewal": {"never"}}
			if response := e.post(target, form); response.StatusCode != http.StatusNotFound {
				t.Errorf("POST %s: %d", target, response.StatusCode)
			}
		}
	}
	if _, err := e.ledger.Access(t.Context(), subWallets[0].ID); err != nil {
		t.Fatalf("the sub-wallet was changed: %v", err)
	}
	if apps := e.apps(); len(apps) != 1 || limitOf(apps[0]) != "none" {
		t.Fatalf("the app was changed instead: %+v", apps)
	}
}

// Catches: a QR code drawn wrongly, such as rows and columns swapped or a module
// missed, and one the page's CSP would block because it uses a style attribute.
func TestQRCodeMatchesTheString(t *testing.T) {
	text := "nostr+walletconnect://" + strings.Repeat("ab", 32) + "?relay=wss%3A%2F%2Frelay.example&secret=" +
		strings.Repeat("cd", 32)
	svg, err := qrSVG(text)
	if err != nil {
		t.Fatal(err)
	}
	code, err := qrcode.New(text, qrcode.Medium)
	if err != nil {
		t.Fatal(err)
	}
	bitmap := code.Bitmap()
	drawn := map[[2]int]bool{}
	for _, m := range regexp.MustCompile(`M(\d+) (\d+)h(\d+)v1h-(\d+)z`).FindAllStringSubmatch(string(svg), -1) {
		x, _ := strconv.Atoi(m[1])
		y, _ := strconv.Atoi(m[2])
		width, _ := strconv.Atoi(m[3])
		if back, _ := strconv.Atoi(m[4]); back != width {
			t.Fatalf("run %q does not close", m[0])
		}
		for i := range width {
			drawn[[2]int{x + i, y}] = true
		}
	}
	dark := 0
	for y, row := range bitmap {
		for x, on := range row {
			if on {
				dark++
			}
			if on != drawn[[2]int{x, y}] {
				t.Fatalf("module %d,%d: drawn %v, want %v", x, y, drawn[[2]int{x, y}], on)
			}
		}
	}
	if dark != len(drawn) || strings.Contains(string(svg), "style") ||
		!strings.Contains(string(svg), fmt.Sprintf(`viewBox="0 0 %d %d"`, len(bitmap), len(bitmap))) {
		t.Fatalf("%d dark modules, %d drawn, or a bad frame:\n%s", dark, len(drawn), svg)
	}
}

// Catches: an exact amount written wrongly, such as a lost msat part or sign.
func TestFormatMsat(t *testing.T) {
	for msat, want := range map[int64]string{
		0: "0 sat", 1: "0.001 sat", 1_500: "1.5 sat", 1_000: "1 sat", 1_234_567_890: "1,234,567.89 sat", -1_001: "-1.001 sat",
	} {
		if got := formatMsat(msat); got != want {
			t.Errorf("%d: %q, want %q", msat, got, want)
		}
	}
}
