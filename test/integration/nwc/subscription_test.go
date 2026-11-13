//go:build integration

package nwc_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/2rdzy/nwc4ln/internal/ledger"
	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
	"github.com/2rdzy/nwc4ln/test/integration"
)

// Catches: requests lost while the service swaps its relay subscription, which it does
// whenever an app or sub-wallet is added, changed or deleted. A client then waits for a
// reply that never comes. Each round sends a burst of requests from a connection that
// does not change, around a change to another one. NWC4LN_SUBSCRIPTION_ROUNDS sets the
// number of rounds.
func TestNoRequestLostOnSubscriptionChange(t *testing.T) {
	n, err := integration.Load(integration.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := waitReady(ctx, n); err != nil {
		t.Fatal(err)
	}
	rounds := envInt(t, "NWC4LN_SUBSCRIPTION_ROUNDS", 20)
	_, service := inProcess(t, ctx, n)
	connectionString, err := service.AddApp(ctx, "steady", ledger.DefaultApp())
	if err != nil {
		t.Fatal(err)
	}
	steady, err := nwctest.Parse(connectionString)
	if err != nil {
		t.Fatal(err)
	}
	defer steady.Close()
	waitForInfo(t, steady)

	var sent, lost atomic.Int64
	for round := range rounds {
		var burst sync.WaitGroup
		for i := range 4 {
			burst.Go(func() {
				time.Sleep(time.Duration(i) * 15 * time.Millisecond)
				callCtx, cancelCall := context.WithTimeout(ctx, 8*time.Second)
				defer cancelCall()
				sent.Add(1)
				if _, err := steady.Call(callCtx, nwctest.NIP44, "get_info"); err != nil {
					lost.Add(1)
				}
			})
		}
		if _, err := service.AddApp(ctx, fmt.Sprintf("change-%d", round), ledger.DefaultApp()); err != nil {
			t.Fatal(err)
		}
		burst.Wait()
		time.Sleep(time.Second) // Stay under the per-connection rate.
	}
	t.Logf("%d of %d requests got no reply across %d subscription changes", lost.Load(), sent.Load(), rounds)
	if lost.Load() > 0 {
		t.Fatalf("%d requests lost", lost.Load())
	}
}
