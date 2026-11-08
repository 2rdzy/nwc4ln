package web

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/node"
)

// start runs the ledger, which fund and withdraw need, and waits for its invoice
// stream, so that income settled by a test is heard.
func (e *appsEnv) start() {
	e.t.Helper()
	if err := e.ledger.Start(e.t.Context()); err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(e.ledger.Close)
	for deadline := time.Now().Add(5 * time.Second); e.node.Streams() == 0; {
		if time.Now().After(deadline) {
			e.t.Fatal("no invoice stream")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// addSubWallet adds a sub-wallet through the form and returns its ID and the client
// secret.
func (e *appsEnv) addSubWallet(name string) (int64, string) {
	e.t.Helper()
	response := e.post("/subwallets", url.Values{"name": {name}})
	page := body(e.t, response)
	match := connectionPattern.FindStringSubmatch(page)
	if response.StatusCode != http.StatusOK || match == nil || !strings.Contains(page, `<div id="qr"><svg`) {
		e.t.Fatalf("create: %d\n%s", response.StatusCode, page)
	}
	wallets := e.subWallets()
	return wallets[len(wallets)-1].ID, match[1]
}

func (e *appsEnv) subWallets() []ledger.SubWallet {
	e.t.Helper()
	wallets, err := e.ledger.SubWallets(e.t.Context())
	if err != nil {
		e.t.Fatal(err)
	}
	return wallets
}

func (e *appsEnv) balance(id int64) int64 {
	e.t.Helper()
	balance, err := e.ledger.Balance(e.t.Context(), id)
	if err != nil {
		e.t.Fatal(err)
	}
	return balance
}

var tokenPattern = regexp.MustCompile(`action="/subwallets/\d+/(fund|withdraw)">\s*<input type="hidden" name="token" value="([0-9a-f]{64})">`)

// tokens opens a sub-wallet's page, as the admin does before sending a form, and
// returns its form tokens by action.
func (e *appsEnv) tokens(id int64) map[string]string {
	e.t.Helper()
	response, page := e.get(fmt.Sprintf("/subwallets/%d", id))
	if response.StatusCode != http.StatusOK {
		e.t.Fatalf("sub-wallet page: %d", response.StatusCode)
	}
	found := map[string]string{}
	for _, match := range tokenPattern.FindAllStringSubmatch(page, -1) {
		found[match[1]] = match[2]
	}
	if len(found) != 2 {
		e.t.Fatalf("form tokens %v:\n%s", found, page)
	}
	return found
}

// move sends a fund or withdraw form with a fresh token.
func (e *appsEnv) move(id int64, action string, sats string) (*http.Response, string) {
	e.t.Helper()
	response := e.post(fmt.Sprintf("/subwallets/%d/%s", id, action),
		url.Values{"amount": {sats}, "token": {e.tokens(id)[action]}})
	return response, body(e.t, response)
}

func (e *appsEnv) all(accountID int64) []ledger.Transaction {
	e.t.Helper()
	rows, _, err := e.ledger.AllHistory(e.t.Context(), ledger.AllHistoryQuery{AccountID: accountID})
	if err != nil {
		e.t.Fatal(err)
	}
	return rows
}

// checkPair checks that the newest two rows are one move made by the ledger: an
// internal invoice settled under R10, with the main wallet side on no account.
func checkPair(t *testing.T, rows []ledger.Transaction, wallet, amountMsat int64, fund bool) {
	t.Helper()
	if len(rows) < 2 {
		t.Fatalf("%d rows", len(rows))
	}
	var out, in ledger.Transaction
	for _, row := range rows[:2] {
		if row.Type == ledger.TypeOutgoing {
			out = row
		} else {
			in = row
		}
	}
	payer, payee := wallet, int64(0)
	if fund {
		payer, payee = 0, wallet
	}
	if out.AccountID != payer || in.AccountID != payee || !out.Internal || out.PaymentHash == "" ||
		out.PaymentHash != in.PaymentHash || out.State != ledger.StateSettled || in.State != ledger.StateSettled ||
		out.AmountMsat != amountMsat || in.AmountMsat != amountMsat || out.FeeMsat != 0 {
		t.Fatalf("not one R10 move of %d msat:\nout %+v\nin  %+v", amountMsat, out, in)
	}
}

// Catches: the UI moving money another way than the ledger's fund and withdraw (a
// second money path); a replace that leaves the old key working or loses the
// balance; and a delete that leaves the key working.
func TestSubWalletThroughUI(t *testing.T) {
	e := newAppsEnv(t)
	e.start()
	id, secret := e.addSubWallet("alice")
	if wallets := e.subWallets(); len(wallets) != 1 || wallets[0].Name != "alice" || e.balance(id) != 0 {
		t.Fatalf("stored %+v", wallets)
	}

	response, page := e.move(id, actionFund, "1000")
	if response.StatusCode != http.StatusOK || !strings.Contains(page, `id="result">Done: 1,000 sat moved from the main wallet`) {
		t.Fatalf("fund: %d\n%s", response.StatusCode, page)
	}
	checkPair(t, e.all(ledger.AllAccounts), id, 1_000_000, true)
	response, page = e.move(id, actionWithdraw, "400")
	if response.StatusCode != http.StatusOK || !strings.Contains(page, `id="result">Done: 400 sat moved from alice`) {
		t.Fatalf("withdraw: %d\n%s", response.StatusCode, page)
	}
	rows := e.all(ledger.AllAccounts)
	checkPair(t, rows, id, 400_000, false)
	if len(rows) != 4 || e.balance(id) != 600_000 {
		t.Fatalf("%d rows, balance %d", len(rows), e.balance(id))
	}
	if _, page := e.get("/subwallets"); !strings.Contains(page, fmt.Sprintf(`id="balance-%d">600 sat`, id)) {
		t.Errorf("list balance:\n%s", page)
	}

	path := fmt.Sprintf("/subwallets/%d", id)
	if response, _ := e.get(path + "/replace"); response.StatusCode != http.StatusOK {
		t.Fatalf("replace page: %d", response.StatusCode)
	}
	response = e.post(path+"/replace", nil)
	page = body(t, response)
	match := connectionPattern.FindStringSubmatch(page)
	if response.StatusCode != http.StatusOK || match == nil || match[1] == secret || !strings.Contains(page, `id="replaced"`) {
		t.Fatalf("replace: %d\n%s", response.StatusCode, page)
	}
	var live, all int
	if err := e.db.DB.QueryRowContext(t.Context(), `SELECT count(*) FILTER (WHERE revoked_at IS NULL), count(*)
		FROM connections WHERE account_id = ?`, id).Scan(&live, &all); err != nil {
		t.Fatal(err)
	}
	if live != 1 || all != 2 || e.balance(id) != 600_000 {
		t.Fatalf("after replace: %d live of %d connections, balance %d", live, all, e.balance(id))
	}

	if response, page := e.get(path + "/delete"); response.StatusCode != http.StatusOK || len(e.subWallets()) != 1 ||
		!strings.Contains(page, `<b id="balance">600 sat</b>`) {
		t.Fatalf("delete page: %d, or it deleted, or no balance:\n%s", response.StatusCode, page)
	}
	response = e.post(path+"/delete", nil)
	if response.StatusCode != http.StatusSeeOther || response.Header.Get("Location") != "/subwallets" {
		t.Fatalf("delete: %d", response.StatusCode)
	}
	if err := e.db.DB.QueryRowContext(t.Context(), `SELECT count(*) FROM connections
		WHERE account_id = ? AND revoked_at IS NULL`, id).Scan(&live); err != nil || live != 0 {
		t.Fatalf("%d connections still live, %v", live, err)
	}
	if len(e.subWallets()) != 0 {
		t.Fatal("still listed")
	}
	if response, _ := e.get(path); response.StatusCode != http.StatusNotFound {
		t.Fatalf("deleted sub-wallet page: %d", response.StatusCode)
	}
}

// Catches: a double click, or a form sent again, that moves money twice; and a
// token that works for another action or another sub-wallet.
func TestDoubleSubmitMovesOnce(t *testing.T) {
	e := newAppsEnv(t)
	e.start()
	id, _ := e.addSubWallet("alice")
	other, _ := e.addSubWallet("bob")
	tokens := e.tokens(id)
	path := fmt.Sprintf("/subwallets/%d/fund", id)
	form := url.Values{"amount": {"100"}, "token": {tokens[actionFund]}}

	codes := make([]int, 2)
	var wg sync.WaitGroup
	for i := range codes {
		wg.Go(func() { codes[i] = e.post(path, form).StatusCode })
	}
	wg.Wait()
	if codes[0]+codes[1] != http.StatusOK+http.StatusConflict {
		t.Fatalf("two sends gave %v", codes)
	}
	if response := e.post(path, form); response.StatusCode != http.StatusConflict {
		t.Fatalf("resend: %d", response.StatusCode)
	}
	if rows := e.all(ledger.AllAccounts); len(rows) != 2 || e.balance(id) != 100_000 {
		t.Fatalf("%d rows, balance %d", len(rows), e.balance(id))
	}

	tokens = e.tokens(id)
	misused := []struct {
		path, token string
	}{
		{path, tokens[actionWithdraw]},
		{fmt.Sprintf("/subwallets/%d/fund", other), tokens[actionFund]},
		{path, ""},
		{path, strings.Repeat("ab", 32)},
	}
	for _, try := range misused {
		if response := e.post(try.path, url.Values{"amount": {"1"}, "token": {try.token}}); response.StatusCode != http.StatusConflict {
			t.Errorf("POST %s with a wrong token: %d", try.path, response.StatusCode)
		}
	}
	if rows := e.all(ledger.AllAccounts); len(rows) != 2 {
		t.Fatalf("a wrong token moved money: %d rows", len(rows))
	}
}

// Catches: a form token kept forever, or with no cap, so that page loads grow memory;
// and an old form still able to move money.
func TestFormTokenCapAndExpiry(t *testing.T) {
	e := newEnv(t, &stubLedger{})
	first := e.server.newFormToken(1, actionFund)
	for range maxForms {
		e.server.newFormToken(1, actionFund)
	}
	if len(e.server.forms) != maxForms || len(e.server.formOrder) != maxForms {
		t.Fatalf("%d tokens kept", len(e.server.forms))
	}
	if e.server.useFormToken(first, 1, actionFund) {
		t.Fatal("the oldest token still works")
	}
	fresh := e.server.newFormToken(1, actionFund)
	e.advance(formLife)
	if !e.server.useFormToken(fresh, 1, actionFund) {
		t.Fatal("a token ended early")
	}
	old := e.server.newFormToken(1, actionFund)
	e.advance(formLife + time.Second)
	if e.server.useFormToken(old, 1, actionFund) {
		t.Fatal("a token outlived its life")
	}
	e.server.newFormToken(1, actionFund)
	if len(e.server.forms) != 1 {
		t.Fatalf("expired tokens kept: %d", len(e.server.forms))
	}
}

// Catches: an amount read as a float or overflowing; a refusal that is shown as done,
// or as a 500; a bad amount that uses up the form; and an app, deleted sub-wallet or
// made-up ID that reaches a move, a replace or a delete.
func TestMoveRefusals(t *testing.T) {
	e := newAppsEnv(t)
	e.start()
	id, _ := e.addSubWallet("alice")
	path := fmt.Sprintf("/subwallets/%d/fund", id)

	tokens := e.tokens(id)
	for _, amount := range []string{"", "0", "-5", "1.5", "1e3", " ", "9223372036854775807",
		strconv.FormatInt(ledger.MaxAmountMsat/msatPerSat+1, 10)} {
		response := e.post(path, url.Values{"amount": {amount}, "token": {tokens[actionFund]}})
		if page := body(t, response); response.StatusCode != http.StatusBadRequest || !strings.Contains(page, "whole number of sats") {
			t.Errorf("amount %q: %d", amount, response.StatusCode)
		}
	}
	// A bad amount does not use up the form, so the admin can correct it.
	if response := e.post(path, url.Values{"amount": {"5"}, "token": {tokens[actionFund]}}); response.StatusCode != http.StatusOK {
		t.Fatalf("corrected amount: %d", response.StatusCode)
	}

	e.node.SetSpendable(node.Spendable{ActiveMsat: 1_000_000, AllMsat: 1_000_000})
	if response, page := e.move(id, actionFund, "996"); response.StatusCode != http.StatusBadRequest ||
		!strings.Contains(page, "more than the main wallet available") {
		t.Errorf("fund over main available: %d\n%s", response.StatusCode, page)
	}
	if response, page := e.move(id, actionWithdraw, "6"); response.StatusCode != http.StatusBadRequest ||
		!strings.Contains(page, "more than the sub-wallet&#39;s balance") {
		t.Errorf("withdraw over balance: %d\n%s", response.StatusCode, page)
	}
	if e.balance(id) != 5_000 || len(e.all(ledger.AllAccounts)) != 2 {
		t.Fatalf("a refusal moved money: balance %d", e.balance(id))
	}

	// The node refuses to cancel the invoice: nothing moves, and the page says so.
	e.node.FailNextCancel(errors.New("invoice already paid"))
	if response, page := e.move(id, actionWithdraw, "5"); response.StatusCode != http.StatusOK ||
		!strings.Contains(page, `id="result">Nothing moved`) {
		t.Errorf("refused cancel: %d\n%s", response.StatusCode, page)
	}
	if e.balance(id) != 5_000 {
		t.Fatalf("a failed withdraw moved money: balance %d", e.balance(id))
	}

	// The ledger stopped, as while the service starts or stops.
	tokens = e.tokens(id)
	e.ledger.Close()
	response := e.post(path, url.Values{"amount": {"1"}, "token": {tokens[actionFund]}})
	if page := body(t, response); response.StatusCode != http.StatusServiceUnavailable || !strings.Contains(page, "Nothing moved") {
		t.Errorf("ledger stopped: %d\n%s", response.StatusCode, page)
	}

	e.create(url.Values{"name": {"shop"}, "renewal": {"never"}, "can_pay": {"on"}})
	app := strconv.FormatInt(e.apps()[0].ID, 10)
	deleted, _ := e.addSubWallet("gone")
	if err := e.service.DeleteSubWallet(t.Context(), deleted); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{app, strconv.FormatInt(deleted, 10), "999", "abc"} {
		for _, suffix := range []string{"", "/replace", "/delete"} {
			if response, _ := e.get("/subwallets/" + target + suffix); response.StatusCode != http.StatusNotFound {
				t.Errorf("GET /subwallets/%s%s: %d", target, suffix, response.StatusCode)
			}
		}
		for _, suffix := range []string{"/fund", "/withdraw", "/replace", "/delete"} {
			if response := e.post("/subwallets/"+target+suffix, url.Values{"amount": {"1"}}); response.StatusCode != http.StatusNotFound {
				t.Errorf("POST /subwallets/%s%s: %d", target, suffix, response.StatusCode)
			}
		}
	}
	if apps := e.apps(); len(apps) != 1 {
		t.Fatalf("the app was deleted through a sub-wallet page")
	}
}

// Catches: a delete page that hides or rounds the money the holder loses.
func TestDeletePageShowsExactBalance(t *testing.T) {
	e := newAppsEnv(t)
	e.start()
	id, _ := e.addSubWallet("alice")
	invoice, err := e.ledger.MakeInvoice(t.Context(), ledger.InvoiceRequest{AccountID: id, AmountMsat: 21_500})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.node.Settle(invoice.PaymentHash); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); e.balance(id) != 21_500; {
		if time.Now().After(deadline) {
			t.Fatalf("income not credited: %d", e.balance(id))
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, page := e.get(fmt.Sprintf("/subwallets/%d/delete", id)); !strings.Contains(page, `id="funds">It holds <b id="balance">21.5 sat</b>`) {
		t.Fatalf("delete page:\n%s", page)
	}
}

// Catches: the main wallet side of moves left off (History matches one account); a
// filter that shows other accounts' rows; a deleted account's history lost; paging
// that skips or repeats a row; and a preimage on the page.
func TestTransactionsPage(t *testing.T) {
	e := newAppsEnv(t)
	e.start()
	e.create(url.Values{"name": {"shop"}, "renewal": {"never"}, "can_pay": {"on"}, "can_invoice": {"on"}})
	app := e.apps()[0].ID
	for i := range 49 {
		if _, err := e.ledger.MakeInvoice(t.Context(), ledger.InvoiceRequest{
			AccountID: app, AmountMsat: 1_000, Description: fmt.Sprintf("row-%02d", i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	alice, _ := e.addSubWallet("alice")
	e.move(alice, actionFund, "10")
	// A deleted sub-wallet with money would get a move from the sweep during the test,
	// so this one has an open invoice only.
	gone, _ := e.addSubWallet("gone")
	if _, err := e.ledger.MakeInvoice(t.Context(), ledger.InvoiceRequest{AccountID: gone, AmountMsat: 2_000}); err != nil {
		t.Fatal(err)
	}
	if err := e.service.DeleteSubWallet(t.Context(), gone); err != nil {
		t.Fatal(err)
	}
	all := e.all(ledger.AllAccounts)
	if len(all) != 52 {
		t.Fatalf("%d rows", len(all))
	}

	// Every row appears once over the two pages, and the second has the rest.
	rowPattern := regexp.MustCompile(`<tr><td>[^<]+</td><td>([^<]+)</td><td>(incoming|outgoing)`)
	_, first := e.get("/transactions")
	_, second := e.get("/transactions?page=2")
	if n := len(rowPattern.FindAllString(first, -1)); n != transactionsPerPage {
		t.Fatalf("page 1 has %d rows", n)
	}
	if n := len(rowPattern.FindAllString(second, -1)); n != 2 || !strings.Contains(second, `id="range">51 to 52 of 52`) {
		t.Fatalf("page 2 has %d rows:\n%s", n, second)
	}
	// Newest first, also within one second: the two oldest rows are on page 2.
	if !strings.Contains(second, "row-00") || !strings.Contains(second, "row-01") {
		t.Errorf("page 2 does not hold the oldest rows:\n%s", second)
	}
	for i := range 49 {
		if strings.Count(first+second, fmt.Sprintf("row-%02d", i)) != 1 {
			t.Errorf("row-%02d not shown exactly once", i)
		}
	}
	if !strings.Contains(first, `id="older" `) && !strings.Contains(first, `id="older">`) {
		t.Errorf("no link to page 2")
	}
	for _, row := range all {
		if row.Preimage != "" && strings.Contains(first+second, row.Preimage) {
			t.Fatalf("preimage shown for row %d", row.ID)
		}
	}

	accountOf := func(page string) map[string]int {
		found := map[string]int{}
		for _, match := range rowPattern.FindAllStringSubmatch(page, -1) {
			found[match[1]]++
		}
		return found
	}
	filters := map[string]map[string]int{
		"main":                       {"main wallet": 1},
		strconv.FormatInt(alice, 10): {"alice (sub-wallet)": 1},
		strconv.FormatInt(gone, 10):  {"gone (sub-wallet, deleted)": 1},
		strconv.FormatInt(app, 10):   {"shop (app)": 49},
	}
	for filter, want := range filters {
		response, page := e.get("/transactions?account=" + filter)
		if got := accountOf(page); response.StatusCode != http.StatusOK || fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("filter %s: %d, rows %v, want %v", filter, response.StatusCode, got, want)
		}
		if !strings.Contains(page, `value="`+filter+`" selected`) {
			t.Errorf("filter %s not shown as chosen", filter)
		}
	}
	if !strings.Contains(first, fmt.Sprintf(`<option value="%d">gone (sub-wallet, deleted)</option>`, gone)) {
		t.Errorf("deleted account missing from the filter:\n%s", first)
	}

	for target, want := range map[string]int{
		"/transactions?page=0": http.StatusBadRequest, "/transactions?page=x": http.StatusBadRequest,
		"/transactions?page=-1": http.StatusBadRequest, "/transactions?account=999": http.StatusNotFound,
		"/transactions?account=0": http.StatusNotFound, "/transactions?account=x": http.StatusNotFound,
		"/transactions?page=9": http.StatusOK,
	} {
		if response, _ := e.get(target); response.StatusCode != want {
			t.Errorf("%s: %d, want %d", target, response.StatusCode, want)
		}
	}
}
