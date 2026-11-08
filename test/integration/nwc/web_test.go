//go:build integration

package nwc_test

import (
	"encoding/base64"
	"encoding/json"
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
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
// process, a dashboard that shows another main wallet figure than the one apps are
// given by the same process on the real node, and app changes in the UI that the
// running service does not hear of: a new app not served, or a deleted one still
// served.
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

	t.Run("AppAddedAndDeleted", func(t *testing.T) {
		response, page := postForm(t, client, base, "/apps", url.Values{
			"name": {"from the UI"}, "renewal": {"never"}, "can_pay": {"on"},
		})
		if response.StatusCode != http.StatusOK {
			t.Fatalf("create: %d\n%s", response.StatusCode, page)
		}
		match := regexp.MustCompile(`<textarea id="connection"[^>]*>([^<]+)<`).FindStringSubmatch(page)
		if match == nil {
			t.Fatalf("no connection string:\n%s", page)
		}
		added, err := nwctest.Parse(html.UnescapeString(match[1]))
		if err != nil {
			t.Fatal(err)
		}
		defer added.Close()
		waitForInfo(t, added)
		if reply := call(t, added, nwctest.NIP44, "get_info"); reply.Error != nil {
			t.Fatalf("get_info on the new app: %+v", reply.Error)
		}

		_, list := get(t, client, base+"/apps")
		id := regexp.MustCompile(`<a href="/apps/(\d+)">from the UI</a>`).FindStringSubmatch(list)
		if id == nil {
			t.Fatalf("new app not listed:\n%s", list)
		}
		if response, _ := postForm(t, client, base, "/apps/"+id[1]+"/delete", nil); response.StatusCode != http.StatusSeeOther {
			t.Fatalf("delete: %d", response.StatusCode)
		}
		// A revoked connection is told so, as with any other delete.
		if reply := call(t, added, nwctest.NIP44, "get_info"); reply.Error == nil || reply.Error.Code != "UNAUTHORIZED" {
			t.Fatalf("deleted app still served: %+v", reply.Error)
		}
	})

	t.Run("SubWalletThroughUI", func(t *testing.T) {
		connect := func(page string) *nwctest.Client {
			t.Helper()
			match := regexp.MustCompile(`<textarea id="connection"[^>]*>([^<]+)<`).FindStringSubmatch(page)
			if match == nil {
				t.Fatalf("no connection string:\n%s", page)
			}
			client, err := nwctest.Parse(html.UnescapeString(match[1]))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(client.Close)
			waitForInfo(t, client)
			return client
		}
		balanceOf := func(client *nwctest.Client) int64 {
			t.Helper()
			reply := call(t, client, nwctest.NIP44, "get_balance")
			if reply.Error != nil {
				t.Fatalf("get_balance: %+v", reply.Error)
			}
			var result struct {
				Balance int64 `json:"balance"`
			}
			if err := json.Unmarshal(reply.Result, &result); err != nil {
				t.Fatal(err)
			}
			return result.Balance
		}

		response, page := postForm(t, client, base, "/subwallets", url.Values{"name": {"alice"}})
		if response.StatusCode != http.StatusOK {
			t.Fatalf("create: %d\n%s", response.StatusCode, page)
		}
		holder := connect(page)
		_, list := get(t, client, base+"/subwallets")
		id := regexp.MustCompile(`<a href="/subwallets/(\d+)">alice</a>`).FindStringSubmatch(list)
		if id == nil {
			t.Fatalf("new sub-wallet not listed:\n%s", list)
		}
		path := "/subwallets/" + id[1]
		// Each move goes through the real node: an invoice, then its cancel (R10).
		move := func(action, sats string) {
			t.Helper()
			_, page := get(t, client, base+path)
			token := regexp.MustCompile(`action="` + path + `/` + action + `">\s*<input type="hidden" name="token" value="([0-9a-f]{64})"`).
				FindStringSubmatch(page)
			if token == nil {
				t.Fatalf("no %s form:\n%s", action, page)
			}
			response, result := postForm(t, client, base, path+"/"+action, url.Values{"amount": {sats}, "token": {token[1]}})
			if response.StatusCode != http.StatusOK || !strings.Contains(result, `id="result">Done:`) {
				t.Fatalf("%s: %d\n%s", action, response.StatusCode, result)
			}
		}
		move("fund", "1000")
		if balance := balanceOf(holder); balance != 1_000_000 {
			t.Fatalf("after fund the holder sees %d msat", balance)
		}
		move("withdraw", "400")
		if balance := balanceOf(holder); balance != 600_000 {
			t.Fatalf("after withdraw the holder sees %d msat", balance)
		}

		response, page = postForm(t, client, base, path+"/replace", nil)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("replace: %d\n%s", response.StatusCode, page)
		}
		replaced := connect(page)
		if reply := call(t, holder, nwctest.NIP44, "get_info"); reply.Error == nil || reply.Error.Code != "UNAUTHORIZED" {
			t.Fatalf("replaced key still served: %+v", reply.Error)
		}
		if balance := balanceOf(replaced); balance != 600_000 {
			t.Fatalf("after replace the holder sees %d msat", balance)
		}

		if response, _ := postForm(t, client, base, path+"/delete", nil); response.StatusCode != http.StatusSeeOther {
			t.Fatalf("delete: %d", response.StatusCode)
		}
		if reply := call(t, replaced, nwctest.NIP44, "get_info"); reply.Error == nil || reply.Error.Code != "UNAUTHORIZED" {
			t.Fatalf("deleted sub-wallet still served: %+v", reply.Error)
		}
	})
}

// postForm sends a form as the admin's browser would, with its origin.
func postForm(t *testing.T, client *http.Client, base, path string, form url.Values) (*http.Response, string) {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, base+path, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Origin", base)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := client.Do(request)
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
