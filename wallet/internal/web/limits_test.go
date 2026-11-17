package web

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// Catches: one request making the server hold a body of any size in memory. The
// cap is in the guard, so it covers every page; one form is enough to show it.
func TestOversizeBodyRefused(t *testing.T) {
	e := newAppsEnv(t)
	for _, path := range []string{"/apps", "/subwallets"} {
		response := e.post(path, url.Values{"name": {strings.Repeat("a", maxBody)}})
		if response.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("%s: status %d", path, response.StatusCode)
		}
	}
	if len(e.apps()) != 0 || len(e.subWallets()) != 0 {
		t.Fatal("an account was added from an oversize body")
	}
}

// Catches: a name longer than the form allows stored when the browser's maxlength
// is bypassed, and a name at the limit refused. "é" is two bytes, so the limit
// counts characters, not bytes.
func TestNameLength(t *testing.T) {
	e := newAppsEnv(t)
	for _, path := range []string{"/apps", "/subwallets"} {
		for length, want := range map[int]int{maxName: http.StatusOK, maxName + 1: http.StatusBadRequest} {
			response := e.post(path, url.Values{"name": {strings.Repeat("é", length)}, "renewal": {"never"}})
			if response.StatusCode != want {
				t.Fatalf("%s, %d characters: status %d\n%s", path, length, response.StatusCode, body(t, response))
			}
		}
	}
	if len(e.apps()) != 1 || len(e.subWallets()) != 1 {
		t.Fatalf("%d apps, %d sub-wallets", len(e.apps()), len(e.subWallets()))
	}
}
