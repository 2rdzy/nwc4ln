package web

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// stubRelay is a RelayStatus with fixed answers.
type stubRelay struct {
	control, clearText, listed bool
	problem                    string
}

func (r *stubRelay) RelayState() (bool, bool, string) { return r.control, r.clearText, r.problem }
func (r *stubRelay) WaitListed(context.Context) bool  { return r.listed }

// Catches: an admin who believes the relay is locked when it is not: the warnings
// for relay control off, a clear-text URL and a refused allow-list must show
// exactly when they apply.
func TestDashboardRelayWarnings(t *testing.T) {
	for _, c := range []struct {
		relay stubRelay
		shown []string
	}{
		{stubRelay{}, []string{"relay-unlocked"}},
		{stubRelay{clearText: true}, []string{"relay-unlocked", "clear-text"}},
		{stubRelay{control: true}, nil},
		{stubRelay{control: true, problem: "the relay refused the allow-list: msg: invalid: x"},
			[]string{"relay-problem"}},
	} {
		e := newEnvWith(t, Config{Ledger: &stubLedger{}, Relay: &c.relay, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
		page := body(t, e.do(http.MethodGet, "/", withCookie(e.login())))
		for _, id := range []string{"relay-unlocked", "clear-text", "relay-problem"} {
			want := strings.Contains(strings.Join(c.shown, " "), id)
			if got := strings.Contains(page, `id="`+id+`"`); got != want {
				t.Errorf("%+v: %s shown %v, want %v", c.relay, id, got, want)
			}
		}
		if c.relay.problem != "" && !strings.Contains(page, "msg: invalid: x") {
			t.Errorf("the relay's reason is not shown")
		}
	}
}

// Catches: a connection string shown with no word that the relay has not
// confirmed it, so the admin cannot tell why the app's first request fails.
func TestConnectionPageUnlisted(t *testing.T) {
	e := newAppsEnv(t)
	for _, listed := range []bool{true, false} {
		e.server.relay = &stubRelay{control: true, listed: listed}
		for _, page := range []string{
			body(t, e.post("/apps", url.Values{"name": {"shop"}, "renewal": {"never"}, "can_pay": {"on"}})),
			body(t, e.post("/subwallets", url.Values{"name": {"holder"}})),
		} {
			if !strings.Contains(page, `id="connection"`) {
				t.Fatalf("no connection string:\n%s", page)
			}
			if got := strings.Contains(page, `id="unlisted"`); got == listed {
				t.Errorf("listed %v: note shown %v", listed, got)
			}
		}
	}
}
