package web

import (
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"
)

var nodeStatusPattern = regexp.MustCompile(`id="node-status"><span class="dot" aria-hidden="true"></span>([^<]*)<`)

func nodeStatusOf(t *testing.T, page string) string {
	t.Helper()
	match := nodeStatusPattern.FindStringSubmatch(page)
	if match == nil {
		t.Fatalf("no node status in the sidebar:\n%s", page)
	}
	return match[1]
}

// Catches: the sidebar, shown on every page, reading the node. That read holds
// the payment lock, so only the dashboard may make it (spec section 8). Also
// catches a status that does not follow the dashboard's last read, or that names
// the wrong node type.
func TestSidebarNeverReadsNode(t *testing.T) {
	l := &stubLedger{}
	e := newEnvWith(t, Config{Ledger: l, NodeType: "cln", Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	cookie := e.login()
	for _, route := range routes {
		if route.method != http.MethodGet || route.public || route.path == "/" || route.path == "/nothing-here" {
			continue
		}
		page := body(t, e.do(http.MethodGet, route.path, withCookie(cookie)))
		if got := nodeStatusOf(t, page); got != "Core Lightning · not read yet" {
			t.Errorf("%s: status %q before any read", route.path, got)
		}
	}
	if n := l.reads.Load(); n != 0 {
		t.Fatalf("pages other than the dashboard read the node %d times", n)
	}

	l.err = errors.New("node down")
	page := body(t, e.do(http.MethodGet, "/", withCookie(cookie)))
	if got := nodeStatusOf(t, page); got != "Core Lightning · unreachable at 12:00 UTC" {
		t.Errorf("status after a failed read: %q", got)
	}
	l.err = nil
	e.advance(90 * time.Minute)
	body(t, e.do(http.MethodGet, "/", withCookie(cookie)))
	page = body(t, e.do(http.MethodGet, "/apps", withCookie(cookie)))
	if got := nodeStatusOf(t, page); got != "Core Lightning · reachable at 13:30 UTC" {
		t.Errorf("status after a good read: %q", got)
	}
	if n := l.reads.Load(); n != 2 {
		t.Errorf("%d node reads for two dashboard loads", n)
	}
}

// Catches: a wrong name for a node type in the sidebar.
func TestNodeName(t *testing.T) {
	for nodeType, want := range map[string]string{"lnd": "LND", "cln": "Core Lightning", "": "Node"} {
		if got := nodeName(nodeType); got != want {
			t.Errorf("%q: %q, want %q", nodeType, got, want)
		}
	}
}

// Catches: styling that the security policy drops without a word. Its
// default-src 'self' blocks style attributes and style elements, so every style
// must be in static/style.css.
func TestNoInlineStyles(t *testing.T) {
	inline := regexp.MustCompile(`(?i)\sstyle=|<style`)
	err := fs.WalkDir(templateFiles, "templates", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := fs.ReadFile(templateFiles, path)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(data), "\n") {
			if inline.MatchString(line) {
				t.Errorf("%s:%d: inline style", path, i+1)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
