//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func network(t *testing.T) *Network {
	t.Helper()
	n, err := Load(DefaultConfig())
	if err != nil {
		t.Fatal(err) // A skip here would look like a pass.
	}
	return n
}

// Catches: the harness starts, but channels, routing, the chain or the relay do
// not work.
func TestSmoke(t *testing.T) {
	if err := network(t).Smoke(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// Catches: the harness reporting success without checking.
func TestSmokeFailsWhenNodeStopped(t *testing.T) {
	n := network(t)
	for _, node := range []string{NodeLNDB, NodeRelay} {
		t.Run(node, func(t *testing.T) {
			ctx := t.Context()
			if err := n.StopNode(node); err != nil {
				t.Fatal(err)
			}
			err := n.Smoke(ctx)
			if err == nil {
				t.Errorf("Smoke passed with %s stopped", node)
			} else if !strings.Contains(err.Error(), node) {
				t.Errorf("Smoke error does not name %s: %v", node, err)
			}
			if err := n.StartNode(ctx, node); err != nil {
				t.Fatal(err)
			}
			if err := n.WaitChannels(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Catches: leftover processes, ports or data that break the next run.
func TestDownLeavesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	// A second network, on its own ports, beside the one the other tests use.
	cfg := Config{Dir: filepath.Join(t.TempDir(), "net"), PortBase: DefaultPortBase + 100}
	n, err := Up(ctx, cfg)
	if err != nil {
		_ = Down(cfg)
		t.Fatal(err)
	}
	if _, err := Up(ctx, cfg); err == nil {
		t.Error("a second Up on the same folder passed")
	}
	pids := map[string]int{}
	for _, node := range []string{NodeKnots, NodeLNDA, NodeLNDB, NodeLNDC, NodeRelay} {
		pids[node] = n.pidOf(node)
		if pids[node] == 0 {
			t.Errorf("%s is not running after Up", node)
		}
	}
	if err := Down(cfg); err != nil {
		t.Fatal(err)
	}
	for node, pid := range pids {
		if exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid)); err == nil && exe != "" {
			t.Errorf("%s (PID %d) still runs", node, pid)
		}
	}
	if _, err := os.Stat(cfg.Dir); err == nil {
		t.Errorf("%s still exists", cfg.Dir)
	}
	if err := checkPortsFree(cfg.PortBase); err != nil {
		t.Error(err)
	}
}
