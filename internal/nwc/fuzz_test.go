package nwc

import (
	"encoding/json"
	"testing"

	"github.com/getAlby/go-nostr"

	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
)

// Catches: a request from a connection holder that crashes the service, gets no
// readable reply, or writes a transaction for anything but a valid payment or
// invoice. The plain text is the decrypted content, so it reaches the parser.
func FuzzRequest(f *testing.F) {
	for _, seed := range []string{
		`{"method":"get_info"}`,
		`{"method":"pay_invoice","params":{"invoice":"lnbc1"}}`,
		`{"method":"pay_invoice","params":{"invoice":"x","amount":-1}}`,
		`{"method":"make_invoice","params":{"amount":1000}}`,
		`{"method":"make_invoice","params":{"amount":1e30,"expiry":-5}}`,
		`{"method":"make_invoice","params":{"amount":1.5}}`,
		`{"method":"lookup_invoice","params":{"payment_hash":"zz"}}`,
		`{"method":"list_transactions","params":{"limit":-1,"offset":9223372036854775807}}`,
		`{"method":"","params":null}`,
		`{"method":"get_balance","params":"x"}`,
		`[]`, `null`, ``, `{`,
	} {
		f.Add(seed)
	}
	e := newEnv(f)
	e.service.burst, e.service.invoiceBurst = 1<<30, 1<<30 // Fuzzing is not about rates.
	f.Fuzz(func(t *testing.T, plain string) {
		e.t = t // Helpers report to this input. Each fuzz process runs inputs one at a time.
		content, err := e.client.Encrypt(nwctest.NIP44, plain)
		if err != nil {
			t.Skip()
		}
		event, err := e.client.Seal(content, e.timestamp(), nostr.Tag{"encryption", nwctest.NIP44})
		if err != nil {
			t.Fatal(err)
		}
		before := e.transactions()
		reply := e.handle(event)
		if reply == nil {
			t.Fatalf("no reply to %q", plain)
		}
		response, err := e.client.Open(nwctest.NIP44, event, reply)
		if err != nil {
			t.Fatalf("unreadable reply to %q: %v", plain, err)
		}
		var request struct {
			Method string `json:"method"`
		}
		_ = json.Unmarshal([]byte(plain), &request)
		if grew := e.transactions() > before; grew && response.Error != nil {
			t.Fatalf("a refused %q wrote a transaction: %+v", plain, response.Error)
		} else if grew && request.Method != "make_invoice" && request.Method != "pay_invoice" {
			t.Fatalf("%q wrote a transaction", plain)
		}
	})
}

func (e *env) transactions() int {
	e.t.Helper()
	var count int
	if err := e.store.DB.QueryRow(`SELECT count(*) FROM transactions`).Scan(&count); err != nil {
		e.t.Fatal(err)
	}
	return count
}
