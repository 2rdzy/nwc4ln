package nwc

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
)

func (e *env) addSubWallet(name string) *nwctest.Client {
	e.t.Helper()
	connectionString, err := e.service.AddSubWallet(e.t.Context(), name)
	if err != nil {
		e.t.Fatal(err)
	}
	client, err := nwctest.Parse(connectionString)
	if err != nil {
		e.t.Fatal(err)
	}
	return client
}

func (e *env) balanceOf(client *nwctest.Client) int64 {
	e.t.Helper()
	response := e.call(client, "get_balance", nil)
	if response.Error != nil {
		e.t.Fatalf("get_balance: %+v", response.Error)
	}
	var result struct {
		Balance int64 `json:"balance"`
	}
	if err := json.Unmarshal(response.Result, &result); err != nil {
		e.t.Fatal(err)
	}
	return result.Balance
}

// Catches: a sub-wallet refused a command, or told a figure that is not its own.
func TestSubWalletCommands(t *testing.T) {
	e := newEnv(t)
	wallet := e.addSubWallet("holder")
	var info struct {
		Methods []string `json:"methods"`
	}
	if err := json.Unmarshal(e.call(wallet, "get_info", nil).Result, &info); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(info.Methods, allMethods) {
		t.Fatalf("methods %v", info.Methods)
	}
	if got := e.balanceOf(wallet); got != 0 {
		t.Fatalf("new sub-wallet told %d, with %d on the node", got, testFunds)
	}
	if response := e.call(wallet, "get_budget", nil); response.Error != nil || string(response.Result) != "{}" {
		t.Fatalf("get_budget %+v, %s", response.Error, response.Result)
	}

	hash := e.makeInvoice(wallet, 1_000*sat)["payment_hash"].(string)
	if err := e.node.Settle(hash); err != nil {
		t.Fatal(err)
	}
	e.waitState(wallet, hash, "settled")
	if got := e.balanceOf(wallet); got != 1_000*sat {
		t.Fatalf("balance %d", got)
	}
	paid := e.call(wallet, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(100 * sat)})
	if paid.Error != nil {
		t.Fatalf("pay_invoice: %+v", paid.Error)
	}
	if got := e.balanceOf(wallet); got != 900*sat {
		t.Fatalf("balance %d", got)
	}
	e.now.Add(2) // The ledger's clock is real, and until defaults to this one.
	if got := len(e.list(wallet, nil)); got != 2 {
		t.Fatalf("%d transactions", got)
	}
	if got := len(e.list(e.client, nil)); got != 0 {
		t.Fatalf("an app sees %d of a sub-wallet's transactions", got)
	}
	wantCode(t, e.call(wallet, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(900 * sat)}),
		codeInsufficientBalance)

	wallets, err := e.ledger.SubWallets(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(wallets) != 1 || wallets[0].Name != "holder" || wallets[0].BalanceMsat != 900*sat || wallets[0].LastUsedAt.IsZero() {
		t.Fatalf("wallets %+v", wallets)
	}
	apps, err := e.ledger.Apps(t.Context())
	if err != nil || len(apps) != 1 {
		t.Fatalf("a sub-wallet shows up as an app: %+v, %v", apps, err)
	}
	if _, err := e.store.DB.Exec(`UPDATE accounts SET deleted_at = 1 WHERE type = 'sub_wallet'`); err != nil {
		t.Fatal(err)
	}
	if wallets, err := e.ledger.SubWallets(t.Context()); err != nil || len(wallets) != 0 {
		t.Fatalf("a deleted sub-wallet is listed: %+v, %v", wallets, err)
	}
}

// Catches: a sub-wallet's fixed settings changed through the app functions.
func TestAppFunctionsLeaveSubWalletsAlone(t *testing.T) {
	e := newEnv(t)
	wallet := e.addSubWallet("holder")
	id := e.accountOf(wallet)
	settings := ledger.DefaultApp()
	settings.LimitMsat = limitOf(1)
	if err := e.service.EditApp(t.Context(), id, settings); err == nil {
		t.Fatal("edited a sub-wallet as an app")
	}
	if err := e.service.DeleteApp(t.Context(), id); err == nil {
		t.Fatal("deleted a sub-wallet as an app")
	}
}

// Catches: one holder slowing the service for everyone. The requests over the rate
// write nothing and reach no node, and other connections are not affected.
func TestRateLimit(t *testing.T) {
	e := newEnv(t)
	flood := e.addSubWallet("flood")
	rows := func() int {
		var count int
		if err := e.store.DB.QueryRow(`SELECT count(*) FROM transactions`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	params := map[string]any{"amount": 1_000}
	for i := range rateBurst + 20 {
		response := e.call(flood, "make_invoice", params)
		if i < rateBurst && response.Error != nil {
			t.Fatalf("request %d: %+v", i, response.Error)
		}
		if i >= rateBurst {
			wantCode(t, response, codeRateLimited)
			if response.ResultType != "make_invoice" {
				t.Fatalf("result_type %q", response.ResultType)
			}
		}
	}
	if got := e.requests(); got != rateBurst {
		t.Errorf("%d requests stored, want %d", got, rateBurst)
	}
	if got := rows(); got != rateBurst {
		t.Errorf("%d invoices made, want %d", got, rateBurst)
	}
	if response := e.call(e.client, "get_info", nil); response.Error != nil {
		t.Errorf("another connection refused: %+v", response.Error)
	}
	e.now.Add(1)
	if response := e.call(flood, "make_invoice", params); response.Error != nil {
		t.Errorf("after a second: %+v", response.Error)
	}
}

// Catches: a relay repeat of a running payment answered RATE_LIMITED, which a client
// reads as a refusal while its money is moving.
func TestRepeatOverRateGetsNoReply(t *testing.T) {
	e := newEnv(t)
	e.service.burst = 1
	event := e.request(nwctest.NIP44, "get_info")
	if e.handle(event) == nil {
		t.Fatal("no reply")
	}
	if e.handle(event) != nil {
		t.Fatal("a repeat was answered")
	}
}

// Catches: a holder locked out by anyone who reads the relay. A copy of a signed
// request passes the signature check, so only the event ID can stop it spending rate.
func TestReplaySpendsNoRate(t *testing.T) {
	e := newEnv(t)
	event := e.request(nwctest.NIP44, "get_info")
	if e.handle(event) == nil {
		t.Fatal("no reply")
	}
	for range rateBurst + 10 {
		if e.handle(event) != nil {
			t.Fatal("a repeat was answered")
		}
	}
	if response := e.call(e.client, "get_info", nil); response.Error != nil {
		t.Fatalf("holder refused: %+v", response.Error)
	}
}

// Catches: the codes a client reads for the two caps.
func TestCapCodes(t *testing.T) {
	e := newEnv(t)
	response := e.call(e.client, "make_invoice", map[string]any{
		"amount": 1_000, "description": strings.Repeat("a", ledger.MaxDescriptionBytes+1),
	})
	wantCode(t, response, codeOther)
	if got := e.service.refused("make_invoice", ledger.ErrTooManyInvoices); got.Code != codeRateLimited {
		t.Fatalf("code %s", got.Code)
	}
}

func (e *env) onlySubWallet() ledger.SubWallet {
	e.t.Helper()
	wallets, err := e.ledger.SubWallets(e.t.Context())
	if err != nil || len(wallets) != 1 {
		e.t.Fatalf("sub-wallets %+v, %v", wallets, err)
	}
	return wallets[0]
}

// Catches: a stolen key that still works after a replace, a new key the service does
// not serve until a restart, or a replace that loses the money or the history.
func TestReplaceConnection(t *testing.T) {
	e := newEnv(t)
	old := e.addSubWallet("holder")
	account := e.onlySubWallet().ID
	row, err := e.ledger.Fund(t.Context(), account, 1_000*sat)
	if err != nil || row.State != ledger.StateSettled {
		t.Fatalf("fund %+v, %v", row, err)
	}
	e.now.Add(2) // The ledger's clock is real, and until defaults to this one.
	if got := len(e.list(old, nil)); got != 1 {
		t.Fatalf("holder sees %d transactions", got)
	}
	connectionString, err := e.service.ReplaceConnection(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	wantCode(t, e.call(old, "get_balance", nil), codeUnauthorized)
	wantCode(t, e.call(old, "pay_invoice", map[string]any{"invoice": e.node.OutsideInvoice(sat)}), codeUnauthorized)
	replaced, err := nwctest.Parse(connectionString)
	if err != nil {
		t.Fatal(err)
	}
	if got := e.balanceOf(replaced); got != 1_000*sat {
		t.Fatalf("balance %d after a replace", got)
	}
	if got := len(e.list(replaced, nil)); got != 1 {
		t.Fatalf("%d transactions after a replace", got)
	}
	var live, all int
	err = e.store.DB.QueryRow(`SELECT count(revoked_at IS NULL OR NULL), count(*) FROM connections WHERE account_id = ?`,
		account).Scan(&live, &all)
	if err != nil || live != 1 || all != 2 {
		t.Fatalf("%d live of %d connections, %v", live, all, err)
	}
	e.restart()
	wantCode(t, e.call(old, "get_balance", nil), codeUnauthorized)
	if got := e.balanceOf(replaced); got != 1_000*sat {
		t.Fatalf("balance %d after a restart", got)
	}
}

// Catches: a replace on an account it must leave alone, which would hand out a working
// key for a deleted account or change how an app connects.
func TestReplaceRefused(t *testing.T) {
	e := newEnv(t)
	e.addSubWallet("holder")
	wallet := e.onlySubWallet().ID
	if err := e.service.DeleteSubWallet(t.Context(), wallet); err != nil {
		t.Fatal(err)
	}
	var app int64
	if err := e.store.DB.QueryRow(`SELECT id FROM accounts WHERE type = 'app'`).Scan(&app); err != nil {
		t.Fatal(err)
	}
	for name, id := range map[string]int64{"deleted": wallet, "app": app, "unknown": 9_999} {
		if _, err := e.service.ReplaceConnection(t.Context(), id); !errors.Is(err, ledger.ErrUnknownAccount) {
			t.Errorf("%s: %v", name, err)
		}
	}
	var connections int
	if err := e.store.DB.QueryRow(`SELECT count(*) FROM connections`).Scan(&connections); err != nil || connections != 2 {
		t.Fatalf("%d connections, %v", connections, err)
	}
}

// Catches: a key that lives on after its sub-wallet is deleted, or an info event still
// published for it.
func TestDeleteSubWalletRevokesAtOnce(t *testing.T) {
	e := newEnv(t)
	wallet := e.addSubWallet("holder")
	if got := e.balanceOf(wallet); got != 0 {
		t.Fatalf("balance %d", got)
	}
	account := e.onlySubWallet().ID
	if err := e.service.DeleteSubWallet(t.Context(), account); err != nil {
		t.Fatal(err)
	}
	wantCode(t, e.call(wallet, "get_balance", nil), codeUnauthorized)
	wantCode(t, e.call(wallet, "make_invoice", map[string]any{"amount": sat}), codeUnauthorized)
	for _, c := range e.service.connectionList() {
		if c.accountID == account && !c.revoked {
			t.Fatalf("connection %d still counts as live", c.id)
		}
	}
}
