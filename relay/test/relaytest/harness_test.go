//go:build relay

// Package relaytest runs the real nwc4ln-relay binary and talks to it as
// clients do, over websockets. It needs no wallet and no Lightning node.
package relaytest

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"fiatjaf.com/nostr"
	"github.com/fasthttp/websocket"
)

var binary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "nwc4ln-relay-test")
	if err != nil {
		panic(err)
	}
	binary = filepath.Join(dir, "nwc4ln-relay")
	build := exec.Command("go", "build", "-o", binary, "../../cmd/nwc4ln-relay")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// relay is one running relay process.
type relay struct {
	t       *testing.T
	dir     string
	extra   string
	control nostr.SecretKey
	cmd     *exec.Cmd
	addr    string
	mu      sync.Mutex
	log     strings.Builder
	exited  chan struct{}
}

var listening = regexp.MustCompile(`msg="relay listening" address=(\S+)`)

// startRelay writes a config with the given extra lines and starts the relay.
func startRelay(t *testing.T, extra string) *relay {
	t.Helper()
	r := &relay{t: t, dir: t.TempDir(), extra: extra, control: nostr.Generate()}
	if err := os.Chmod(r.dir, 0o700); err != nil { // The relay refuses a folder others can change.
		t.Fatal(err)
	}
	config := fmt.Sprintf("data_dir = %q\nlisten = \"127.0.0.1:0\"\ncontrol_key = %q\n%s",
		r.dir, r.control.Public().Hex(), extra)
	if err := os.WriteFile(r.configPath(), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	r.start()
	t.Cleanup(r.stop)
	return r
}

func (r *relay) configPath() string { return filepath.Join(r.dir, "nwc4ln-relay.toml") }

func (r *relay) start() {
	r.t.Helper()
	r.cmd = exec.Command(binary, "start", "--debug", "--config", r.configPath())
	stderr, err := r.cmd.StderrPipe()
	if err != nil {
		r.t.Fatal(err)
	}
	if err := r.cmd.Start(); err != nil {
		r.t.Fatal(err)
	}
	r.exited = make(chan struct{})
	found := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stderr)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			r.mu.Lock()
			r.log.WriteString(line + "\n")
			r.mu.Unlock()
			if m := listening.FindStringSubmatch(line); m != nil {
				select {
				case found <- m[1]:
				default:
				}
			}
		}
		_, _ = io.Copy(io.Discard, stderr)
		_ = r.cmd.Wait()
		close(r.exited)
	}()
	select {
	case r.addr = <-found:
	case <-r.exited:
		r.t.Fatalf("relay exited at start:\n%s", r.logText())
	case <-time.After(10 * time.Second):
		r.t.Fatalf("relay did not start:\n%s", r.logText())
	}
}

func (r *relay) stop() {
	if r.cmd == nil || r.cmd.Process == nil {
		return
	}
	_ = r.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-r.exited:
	case <-time.After(10 * time.Second):
		_ = r.cmd.Process.Kill()
		<-r.exited
	}
	r.cmd = nil
	if r.t.Failed() {
		r.t.Logf("relay log:\n%s", r.logText())
	}
}

func (r *relay) restart() {
	r.t.Helper()
	r.stop()
	r.start()
}

func (r *relay) logText() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.log.String()
}

// client is one websocket connection.
type client struct {
	t      *testing.T
	conn   *websocket.Conn
	in     chan nostr.Envelope
	closed chan struct{}
	wmu    sync.Mutex
}

func (r *relay) connect() *client {
	r.t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+r.addr, nil)
	if err != nil {
		r.t.Fatalf("connect: %v", err)
	}
	c := &client{t: r.t, conn: conn, in: make(chan nostr.Envelope, 4096), closed: make(chan struct{})}
	go func() {
		defer close(c.closed)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			env, err := nostr.ParseMessage(string(data))
			if err != nil {
				continue
			}
			select {
			case c.in <- env:
			default: // A test that does not read is not slowed down.
			}
		}
	}()
	r.t.Cleanup(func() { _ = conn.Close() })
	return c
}

func (c *client) sendRaw(text string) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.conn.WriteMessage(websocket.TextMessage, []byte(text))
}

func (c *client) send(text string) {
	c.t.Helper()
	if err := c.sendRaw(text); err != nil {
		c.t.Fatalf("send: %v", err)
	}
}

// publish sends an event and waits for its OK.
func (c *client) publish(event nostr.Event) (bool, string) {
	c.t.Helper()
	data, err := event.MarshalJSON()
	if err != nil {
		c.t.Fatal(err)
	}
	c.send(`["EVENT",` + string(data) + `]`)
	deadline := time.After(5 * time.Second)
	for {
		select {
		case env := <-c.in:
			if ok, isOK := env.(*nostr.OKEnvelope); isOK && ok.EventID == event.ID {
				return ok.OK, ok.Reason
			}
		case <-c.closed:
			return false, "connection closed"
		case <-deadline:
			c.t.Fatalf("no OK for %s", event.ID.Hex())
		}
	}
}

// subscribe sends a REQ. It returns the stored events, or the CLOSED reason.
func (c *client) subscribe(id string, filters ...nostr.Filter) (events []nostr.Event, closed string) {
	c.t.Helper()
	text := `["REQ",` + fmt.Sprintf("%q", id)
	for _, f := range filters {
		data, err := f.MarshalJSON()
		if err != nil {
			c.t.Fatal(err)
		}
		text += "," + string(data)
	}
	c.send(text + "]")
	deadline := time.After(5 * time.Second)
	for {
		select {
		case env := <-c.in:
			switch e := env.(type) {
			case *nostr.EventEnvelope:
				if e.SubscriptionID != nil && *e.SubscriptionID == id {
					events = append(events, e.Event)
				}
			case *nostr.EOSEEnvelope:
				if e.SubscriptionID == id {
					return events, ""
				}
			case *nostr.ClosedEnvelope:
				if e.SubscriptionID == id {
					return events, e.Reason
				}
			}
		case <-c.closed:
			return events, "connection closed"
		case <-deadline:
			c.t.Fatalf("no EOSE or CLOSED for %s", id)
		}
	}
}

// next waits for a live event on a subscription.
func (c *client) next(id string, wait time.Duration) (nostr.Event, bool) {
	deadline := time.After(wait)
	for {
		select {
		case env := <-c.in:
			if e, ok := env.(*nostr.EventEnvelope); ok && e.SubscriptionID != nil && *e.SubscriptionID == id {
				return e.Event, true
			}
		case <-c.closed:
			return nostr.Event{}, false
		case <-deadline:
			return nostr.Event{}, false
		}
	}
}

// isClosed reports whether the relay closed the connection within the wait.
func (c *client) isClosed(wait time.Duration) bool {
	select {
	case <-c.closed:
		return true
	default:
	}
	select {
	case <-c.closed:
		return true
	case <-time.After(wait):
		return false
	}
}

// keys is a key pair.
type keys struct {
	secret nostr.SecretKey
	pub    nostr.PubKey
	hex    string
}

func newKeys() keys {
	s := nostr.Generate()
	return keys{secret: s, pub: s.Public(), hex: s.Public().Hex()}
}

func sign(t *testing.T, by nostr.SecretKey, kind nostr.Kind, created nostr.Timestamp, content string, tags nostr.Tags) nostr.Event {
	t.Helper()
	e := nostr.Event{Kind: kind, CreatedAt: created, Content: content, Tags: tags}
	if err := e.Sign(by); err != nil {
		t.Fatal(err)
	}
	return e
}

// allowList builds a list of pairs, signed by the given key.
func allowList(t *testing.T, by nostr.SecretKey, created nostr.Timestamp, pairs ...keys) nostr.Event {
	t.Helper()
	tags := nostr.Tags{}
	for i := 0; i+1 < len(pairs); i += 2 {
		tags = append(tags, nostr.Tag{"pair", pairs[i].hex, pairs[i+1].hex})
	}
	return sign(t, by, 29194, created, "", tags)
}

func p(k keys) nostr.Tags { return nostr.Tags{{"p", k.hex}} }

// accept publishes an event that must be accepted.
func (c *client) accept(what string, event nostr.Event) {
	c.t.Helper()
	ok, reason := c.publish(event)
	mustOK(c.t, what, ok, reason)
}

func mustOK(t *testing.T, what string, ok bool, reason string) {
	t.Helper()
	if !ok {
		t.Fatalf("%s refused: %s", what, reason)
	}
}

func mustRefuse(t *testing.T, what string, ok bool, reason string) {
	t.Helper()
	if ok {
		t.Fatalf("%s accepted", what)
	}
	t.Logf("%s refused: %s", what, reason)
}
