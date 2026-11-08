//go:build integration

package nwc_test

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
	"github.com/2rdzy/nwc4ln/test/integration"
)

var uiURLPattern = regexp.MustCompile(`admin UI listening url=(http://\S+)`)

// uiURL reads the admin UI's address from the service log.
func (s *service) uiURL() string {
	s.t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		log, err := os.ReadFile(s.log.Name())
		if err != nil {
			s.t.Fatal(err)
		}
		if matches := uiURLPattern.FindAllStringSubmatch(string(log), -1); matches != nil {
			return matches[len(matches)-1][1]
		}
	}
	s.t.Fatal("no admin UI address in the log")
	return ""
}

func get(t *testing.T, client *http.Client, url string) (*http.Response, string) {
	t.Helper()
	response, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response, string(body)
}

// Catches: the admin UI not wired into start, a login that fails against the real
// process, or a dashboard that shows another main wallet figure than the one apps
// are given by the same process on the real node.
func TestAdminUI(t *testing.T) {
	n, err := integration.Load(integration.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	s := startService(t, n)
	app, err := nwctest.Parse(s.connectionString)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	waitForInfo(t, app)
	base := s.uiURL()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}

	if response, _ := get(t, client, base+"/"); response.StatusCode != http.StatusSeeOther {
		t.Fatalf("dashboard without a session: %d", response.StatusCode)
	}
	_, page := get(t, client, base+"/login")
	match := regexp.MustCompile(`data-challenge="([0-9a-f]{64})"`).FindStringSubmatch(page)
	if match == nil {
		t.Fatalf("no challenge:\n%s", page)
	}
	loginURL := base + "/login?challenge=" + match[1]
	event, err := nwctest.SignEvent(s.adminSecret, 27235, time.Now().Unix(),
		[][]string{{"u", loginURL}, {"method", "POST"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, loginURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Origin", base)
	request.Header.Set("Authorization", "Nostr "+base64.StdEncoding.EncodeToString(event))
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("login: %d", response.StatusCode)
	}

	reply := call(t, app, nwctest.NIP44, "get_balance")
	if reply.Error != nil {
		t.Fatalf("get_balance: %+v", reply.Error)
	}
	var balance struct {
		Balance int64 `json:"balance"`
	}
	if err := json.Unmarshal(reply.Result, &balance); err != nil {
		t.Fatal(err)
	}
	response, page = get(t, client, base+"/")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("dashboard: %d", response.StatusCode)
	}
	shown := regexp.MustCompile(`id="main"><b>([-0-9,]+) sat<`).FindStringSubmatch(page)
	if shown == nil {
		t.Fatalf("no main wallet figure:\n%s", page)
	}
	sats, err := strconv.ParseInt(strings.ReplaceAll(shown[1], ",", ""), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	// Nothing moves between the two reads: no payment runs, and the channels are idle.
	if balance.Balance <= 0 || sats != balance.Balance/1_000 {
		t.Fatalf("dashboard shows %d sat, get_balance gave %d msat", sats, balance.Balance)
	}
	if strings.Contains(page, `id="shortfall"`) || strings.Contains(page, `id="unknown"`) {
		t.Fatalf("warning on a healthy node:\n%s", page)
	}
}
