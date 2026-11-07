//go:build integration

package nwc_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/2rdzy/nwc4ln/internal/nwc/nwctest"
	"github.com/2rdzy/nwc4ln/test/integration"
)

// Catches: our own client sharing our misreading of the protocol, and wiring from
// an outside client through the relay, nwc4ln and LND, notifications included.
func TestAcceptance(t *testing.T) {
	n, err := integration.Load(integration.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	// The SDK goes in a scratch folder, so that no node_modules sits under the Go
	// module, and only what the lock file names is installed, with no scripts.
	work := t.TempDir()
	for _, name := range []string{"package.json", "package-lock.json", "acceptance.mjs"} {
		source, err := os.ReadFile(filepath.Join("acceptance", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(work, name), source, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	install := exec.CommandContext(t.Context(), "npm", "ci", "--ignore-scripts", "--no-audit", "--no-fund")
	install.Dir = work
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("npm ci: %v: %s", err, out)
	}
	regtest := filepath.Join(work, "regtest")
	if out, err := exec.CommandContext(t.Context(), "go", "build", "-o", regtest, "../cmd/regtest").CombinedOutput(); err != nil {
		t.Fatalf("build: %v: %s", err, out)
	}

	s := startService(t, n)
	client, err := nwctest.Parse(s.connectionString)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitForInfo(t, client)
	secondClient, err := nwctest.Parse(s.second)
	if err != nil {
		t.Fatal(err)
	}
	defer secondClient.Close()
	waitForInfo(t, secondClient)

	script := exec.CommandContext(t.Context(), "node", "acceptance.mjs")
	script.Dir = work
	script.Env = append(os.Environ(), "NWC_FIRST="+s.connectionString, "NWC_SECOND="+s.second, "REGTEST="+regtest)
	out, err := script.CombinedOutput()
	t.Logf("%s", out)
	if err != nil {
		t.Fatalf("acceptance: %v", err)
	}
}
