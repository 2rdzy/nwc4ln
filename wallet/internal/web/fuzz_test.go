package web

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/2rdzy/nwc4ln/internal/ledger"
)

func formRequest(values url.Values) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(values.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}

// Catches: a fund or withdraw form that moves a fraction of a sat, nothing, a negative
// amount, or more than all bitcoin, from what the admin typed.
func FuzzAmountFromForm(f *testing.F) {
	for _, seed := range []string{"1", "0", "-1", "1.5", "1e3", " 7 ", "21000000000000000", "99999999999999999999", ""} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		msat, err := amountFromForm(formRequest(url.Values{"amount": {text}}))
		if err != nil {
			return
		}
		if msat < msatPerSat || msat > ledger.MaxAmountMsat || msat%msatPerSat != 0 {
			t.Fatalf("%q read as %d msat", text, msat)
		}
	})
}

// Catches: app settings stored that the ledger cannot use: a limit that is not whole
// sats, negative or above all bitcoin, or an unknown renewal.
func FuzzSettingsFromForm(f *testing.F) {
	f.Add("10", "daily", "2026-12-31")
	f.Add("", "never", "")
	f.Add("-1", "weekly", "not a date")
	f.Add("1.5", "hourly", "2026-02-30")
	f.Fuzz(func(t *testing.T, limit, renewal, expires string) {
		settings, err := settingsFromForm(formRequest(url.Values{"limit": {limit}, "renewal": {renewal}, "expires": {expires}}))
		if err != nil {
			return
		}
		if err := settings.Check(); err != nil {
			t.Fatalf("stored settings fail their own check: %v", err)
		}
		if settings.LimitMsat != nil && (*settings.LimitMsat < 0 || *settings.LimitMsat%msatPerSat != 0) {
			t.Fatalf("limit %q read as %d msat", limit, *settings.LimitMsat)
		}
	})
}
