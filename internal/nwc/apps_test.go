package nwc

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/node"
	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
)

func limitOf(msat int64) *int64 { return &msat }

// accountOf finds the account behind a client.
func (e *env) accountOf(client *nwctest.Client) int64 {
	e.t.Helper()
	c, found := e.service.connections[client.ServicePub]
	if !found {
		e.t.Fatal("no such connection")
	}
	return c.accountID
}

func (e *env) edit(change func(*ledger.AppSettings)) {
	e.t.Helper()
	settings := ledger.DefaultApp()
	settings.CanInvoice = true
	change(&settings)
	if err := e.service.EditApp(e.t.Context(), e.accountOf(e.client), settings); err != nil {
		e.t.Fatal(err)
	}
}

// every is each method a client can call, with params that would work.
func (e *env) everyMethod() map[string]any {
	return map[string]any{
		"get_info": nil, "get_balance": nil, "get_budget": nil, "list_transactions": nil,
		"lookup_invoice": map[string]any{"payment_hash": strings.Repeat("0", 64)},
		"make_invoice":   map[string]any{"amount": 1_000},
		"pay_invoice":    map[string]any{"invoice": e.node.OutsideInvoice(1 * sat)},
	}
}

func codeOf(response *nwctest.Response) string {
	if response.Error == nil {
		return ""
	}
	return response.Error.Code
}

// Catches: a key with pay off reading the account, and a key with invoice off
// making invoices. A public service holds a key with pay off.
func TestPermissions(t *testing.T) {
	cases := []struct {
		name           string
		canPay, canInv bool
		allowed        []string
	}{
		{"both on", true, true, allMethods},
		{"invoice off", true, false, []string{
			"get_balance", "get_budget", "get_info", "list_transactions", "lookup_invoice", "pay_invoice",
		}},
		{"pay off", false, true, []string{"get_info", "lookup_invoice", "make_invoice"}},
		{"both off", false, false, []string{"get_info", "lookup_invoice"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			e.edit(func(s *ledger.AppSettings) { s.CanPay, s.CanInvoice = c.canPay, c.canInv })
			for method, params := range e.everyMethod() {
				got := codeOf(e.call(e.client, method, params))
				restricted := got == codeRestricted
				if want := !slices.Contains(c.allowed, method); restricted != want {
					t.Errorf("%s: code %q, restricted should be %v", method, got, want)
				}
			}
			var info struct {
				Methods []string `json:"methods"`
			}
			response := e.call(e.client, "get_info", nil)
			if err := json.Unmarshal(response.Result, &info); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(info.Methods, c.allowed) {
				t.Errorf("get_info methods %v, want %v", info.Methods, c.allowed)
			}
			event, err := e.service.infoEvent(e.service.connections[e.client.ServicePub])
			if err != nil {
				t.Fatal(err)
			}
			if event.Content != strings.Join(c.allowed, " ") {
				t.Errorf("info event %q, want %v", event.Content, c.allowed)
			}
		})
	}
}

// Catches: an expired or deleted app that can still read or spend.
func TestUnauthorizedOnEveryMethod(t *testing.T) {
	t.Run("Expired", func(t *testing.T) {
		e := newEnv(t)
		e.edit(func(s *ledger.AppSettings) { s.ExpiresAt = time.Now().Add(-time.Hour) })
		for method, params := range e.everyMethod() {
			wantCode(t, e.call(e.client, method, params), codeUnauthorized)
		}
	})
	t.Run("Deleted", func(t *testing.T) {
		e := newEnv(t)
		if err := e.service.DeleteApp(t.Context(), e.accountOf(e.client)); err != nil {
			t.Fatal(err)
		}
		for method, params := range e.everyMethod() {
			wantCode(t, e.call(e.client, method, params), codeUnauthorized)
		}
		var revoked bool
		err := e.store.DB.QueryRow(`SELECT revoked_at IS NOT NULL FROM connections`).Scan(&revoked)
		if err != nil || !revoked {
			t.Fatalf("revoked %v, %v", revoked, err)
		}
	})
}

// Catches: a refusal that reaches the client under the wrong code.
func TestLimitCodes(t *testing.T) {
	e := newEnv(t)
	pay := func() *nwctest.Response {
		return e.call(e.client, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(100 * sat)})
	}
	e.edit(func(s *ledger.AppSettings) { s.LimitMsat = limitOf(50 * sat) })
	wantCode(t, pay(), codeQuotaExceeded)
	e.edit(func(s *ledger.AppSettings) {})
	e.node.SetSpendable(node.Spendable{ActiveMsat: 50 * sat})
	wantCode(t, pay(), codeInsufficientBalance)
	e.node.SetUnreachable(true)
	wantCode(t, pay(), codeInternal)
}

// Catches: a client misreading its budget, or told more than the app may spend.
func TestBudgetAndBalance(t *testing.T) {
	e := newEnv(t)
	balance := func() int64 {
		t.Helper()
		var got struct {
			Balance int64 `json:"balance"`
		}
		if err := json.Unmarshal(e.call(e.client, "get_balance", nil).Result, &got); err != nil {
			t.Fatal(err)
		}
		return got.Balance
	}
	budget := func() string {
		t.Helper()
		return string(e.call(e.client, "get_budget", nil).Result)
	}
	e.node.SetSpendable(node.Spendable{ActiveMsat: 1_000 * sat})

	if got := budget(); got != "{}" {
		t.Fatalf("no limit: %s", got)
	}
	if got := balance(); got != 1_000*sat {
		t.Fatalf("no limit: balance %d", got)
	}

	e.edit(func(s *ledger.AppSettings) { s.LimitMsat = limitOf(0) })
	if got := budget(); got != `{"used_budget":0,"total_budget":0,"renewal_period":"never"}` {
		t.Fatalf("zero limit: %s", got)
	}
	if got := balance(); got != 0 {
		t.Fatalf("zero limit: balance %d", got)
	}

	e.edit(func(s *ledger.AppSettings) { s.LimitMsat, s.Renewal = limitOf(300*sat), ledger.RenewMonthly })
	e.call(e.client, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(100 * sat)})
	var got map[string]any
	if err := json.Unmarshal([]byte(budget()), &got); err != nil {
		t.Fatal(err)
	}
	wantKeys(t, "get_budget", got, "used_budget", "total_budget", "renews_at", "renewal_period")
	now := time.Now().UTC()
	renews := time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC).Unix()
	if got["used_budget"] != float64(100*sat) || got["total_budget"] != float64(300*sat) ||
		got["renewal_period"] != "monthly" || got["renews_at"] != float64(renews) {
		t.Fatalf("got %v", got)
	}
	if got := balance(); got != 200*sat {
		t.Fatalf("balance %d, want the remaining limit", got)
	}

	e.node.SetSpendable(node.Spendable{ActiveMsat: 50 * sat})
	if got := balance(); got != 50*sat {
		t.Fatalf("balance %d, want the main wallet", got)
	}

	e.edit(func(s *ledger.AppSettings) { s.LimitMsat = limitOf(10 * sat) })
	if got := balance(); got != 0 {
		t.Fatalf("limit cut below usage: balance %d", got)
	}
}

// Catches: an edit that reaches only the next start, or a client refused for a right
// it just got. Usage is not touched by an edit.
func TestEditAppliesToNextRequest(t *testing.T) {
	e := newEnv(t)
	pay := func() *nwctest.Response {
		return e.call(e.client, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(1 * sat)})
	}
	e.edit(func(s *ledger.AppSettings) { s.CanPay = false })
	wantCode(t, pay(), codeRestricted)
	e.edit(func(s *ledger.AppSettings) {})
	if got := codeOf(pay()); got != "" {
		t.Fatalf("code %s", got)
	}
}

// Catches: a new app with no connection until a restart, an old key lost in the
// reload, and a loop that is never told about it.
func TestAddAppWhileRunning(t *testing.T) {
	e := newEnv(t)
	settings := ledger.DefaultApp()
	settings.LimitMsat = limitOf(1_000 * sat)
	connectionString, err := e.service.AddApp(t.Context(), "later", settings)
	if err != nil {
		t.Fatal(err)
	}
	client, err := nwctest.Parse(connectionString)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.service.changed:
	default:
		t.Fatal("the relay loop was not told")
	}
	if got := len(e.service.connectionList()); got != 2 {
		t.Fatalf("%d connections", got)
	}
	if got := string(e.call(client, "get_budget", nil).Result); !strings.Contains(got, `"total_budget":1000000`) {
		t.Fatalf("new app: %s", got)
	}
	if response := e.call(e.client, "get_info", nil); response.Error != nil {
		t.Fatalf("old app: %+v", response.Error)
	}
	filter := e.service.filter(0)
	if keys := filter.Tags["p"]; !slices.Contains(keys, client.ServicePub) || !slices.Contains(keys, e.client.ServicePub) {
		t.Fatalf("filter %v", keys)
	}
}

// Catches: an expired client still hearing about money.
func TestNoNotificationWhenExpired(t *testing.T) {
	e := newEnv(t)
	out := &sent{}
	e.service.publish = out.publish
	e.edit(func(s *ledger.AppSettings) { s.ExpiresAt = time.Now().Add(-time.Hour) })
	e.service.notify(t.Context(), ledger.Transaction{
		AccountID: e.accountOf(e.client), Type: ledger.TypeIncoming, State: ledger.StateSettled,
	})
	if len(out.events) != 0 {
		t.Fatalf("got %+v", out.events)
	}
}

// Catches: an info event that a relay may drop for an older one, because both came
// in the same second.
func TestInfoEventsAlwaysMoveForward(t *testing.T) {
	e := newEnv(t)
	c := e.service.connections[e.client.ServicePub]
	first, err := e.service.infoEvent(c)
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.service.infoEvent(c)
	if err != nil {
		t.Fatal(err)
	}
	if second.CreatedAt <= first.CreatedAt {
		t.Fatalf("%d, then %d", first.CreatedAt, second.CreatedAt)
	}
}

// Catches: bad settings stored by the path that creates an app.
func TestAddAppRefusesBadSettings(t *testing.T) {
	e := newEnv(t)
	_, err := e.service.AddApp(t.Context(), "bad", ledger.AppSettings{Renewal: "hourly"})
	if !errors.Is(err, ledger.ErrBadRenewal) {
		t.Fatalf("got %v", err)
	}
}

// Catches: a leaked app key that still works after a replace, or a replace that resets
// the app's limit or its usage, which would let the new key spend the limit again.
func TestReplaceAppConnection(t *testing.T) {
	e := newEnv(t)
	limit := int64(100 * sat)
	e.edit(func(s *ledger.AppSettings) { s.LimitMsat, s.Renewal = &limit, ledger.RenewNever })
	if response := e.call(e.client, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(50 * sat)}); response.Error != nil {
		t.Fatalf("pay: %+v", response.Error)
	}
	account := e.accountOf(e.client)
	before, err := e.ledger.AppBudget(t.Context(), account)
	if err != nil || before == nil {
		t.Fatalf("budget %+v, %v", before, err)
	}
	connectionString, err := e.service.ReplaceConnection(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	wantCode(t, e.call(e.client, "get_info", nil), codeUnauthorized)
	replaced, err := nwctest.Parse(connectionString)
	if err != nil {
		t.Fatal(err)
	}
	budget := e.result(replaced, "get_budget", nil)
	if budget["total_budget"] != float64(limit) || budget["used_budget"] != float64(before.UsedMsat) {
		t.Fatalf("budget after replace %v, want total %d used %d", budget, limit, before.UsedMsat)
	}
}
