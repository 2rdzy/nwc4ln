package ledger

import (
	"testing"

	"github.com/2rdzy/nwc4ln/internal/node"
)

// Catches: an amount that is 0 or below, above all bitcoin, or other than the
// invoice's own, accepted for a payment.
func FuzzPayAmount(f *testing.F) {
	for _, seed := range [][2]int64{{0, 0}, {1000, 0}, {0, 1000}, {1000, 999}, {0, -1}, {-5, 0}, {0, MaxAmountMsat + 1},
		{MaxAmountMsat, MaxAmountMsat}, {1 << 62, 0}} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, invoiceMsat, requested int64) {
		amount, err := payAmount(node.DecodedInvoice{AmountMsat: invoiceMsat}, requested)
		if err != nil {
			return
		}
		if amount <= 0 || amount > MaxAmountMsat {
			t.Fatalf("invoice %d, requested %d: accepted %d", invoiceMsat, requested, amount)
		}
		if invoiceMsat != 0 && amount != invoiceMsat || invoiceMsat == 0 && amount != requested {
			t.Fatalf("invoice %d, requested %d: accepted %d", invoiceMsat, requested, amount)
		}
		if amount+Reserve(amount) <= amount {
			t.Fatalf("reserve of %d overflows", amount)
		}
	})
}
