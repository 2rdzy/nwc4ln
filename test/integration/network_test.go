package integration

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Catches: down deleting a folder it did not create, if NWC4LN_IT_DIR is wrong.
func TestDownRefusesFolderWithoutMarker(t *testing.T) {
	dir := t.TempDir()
	keep := filepath.Join(dir, "keep")
	if err := os.WriteFile(keep, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Down(Config{Dir: dir}); err == nil {
		t.Fatal("Down removed a folder without the marker")
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("file gone: %v", err)
	}
}

// Catches: start beside another network on a port this one needs.
func TestUpRefusesTakenPort(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port

	cfg := Config{Dir: filepath.Join(t.TempDir(), "net"), PortBase: port - portRelay}
	_, err = Up(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "is taken") {
		t.Fatalf("Up error = %v, want a taken port", err)
	}
	if _, err := os.Stat(cfg.Dir); err == nil {
		t.Fatal("Up created its folder despite the conflict")
	}
}

// Catches: reading an in-flight update as the result, so a failed payment passes.
func TestLastPaymentStatusUsesFinalUpdate(t *testing.T) {
	output := []byte(`{"status":"IN_FLIGHT","failure_reason":"FAILURE_REASON_NONE"}
{"status":"FAILED","failure_reason":"FAILURE_REASON_NO_ROUTE"}`)
	status, reason, err := lastPaymentStatus(output)
	if err != nil {
		t.Fatal(err)
	}
	if status != "FAILED" || reason != "FAILURE_REASON_NO_ROUTE" {
		t.Fatalf("got %s %s", status, reason)
	}
}
