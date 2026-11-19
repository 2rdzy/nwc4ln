package server

import (
	"context"
	"net"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// connLimits are the per-connection limits.
type connLimits struct {
	proven, unproven         int           // Open connections of each kind.
	unprovenTimeout          time.Duration // An unproven connection is closed after this.
	refusals                 int           // A connection is closed at this many refusals.
	messagesPerSec           float64
	messageBurst             int
	subscriptions            int
	provenBytes, unprovenBps int // Read rate in bytes a second.
	byteBurst                int // Read burst in bytes, at least one full message.
}

// tracker counts open connections. A connection is unproven until it sends an
// accepted event or filter. Strangers can never prove a connection, so a flood of
// them fills only the unproven pool, and the oldest unproven connection is closed
// to make room for a new one: the wallet always gets in. The tracker wraps the
// listener, because khatru gives its hooks no way to close a connection.
type tracker struct {
	net.Listener
	limits connLimits
	onDrop func(reason string, c *conn)

	mu       sync.Mutex
	byAddr   map[string]*conn
	unproven []*conn // Oldest first.
	proven   int
}

func newTracker(inner net.Listener, limits connLimits, onDrop func(string, *conn)) *tracker {
	return &tracker{Listener: inner, limits: limits, onDrop: onDrop, byAddr: make(map[string]*conn)}
}

// Accept wraps each new connection.
func (t *tracker) Accept() (net.Conn, error) {
	inner, err := t.Listener.Accept()
	if err != nil {
		return nil, err
	}
	c := &conn{
		Conn:     inner,
		t:        t,
		addr:     inner.RemoteAddr().String(),
		bytes:    rate.NewLimiter(rate.Limit(t.limits.unprovenBps), t.limits.byteBurst),
		messages: rate.NewLimiter(rate.Limit(t.limits.messagesPerSec), t.limits.messageBurst),
	}
	var evict *conn
	t.mu.Lock()
	t.byAddr[c.addr] = c
	if len(t.unproven) >= t.limits.unproven {
		evict = t.unproven[0]
	}
	t.unproven = append(t.unproven, c)
	t.mu.Unlock()
	if evict != nil {
		t.onDrop("unproven connections full, closed the oldest", evict)
		_ = evict.Close()
	}
	c.timer = time.AfterFunc(t.limits.unprovenTimeout, func() {
		if !c.isProven() {
			t.onDrop("sent nothing accepted in time", c)
			_ = c.Close()
		}
	})
	return c, nil
}

// lookup finds a connection by its remote address, as khatru's request shows it.
func (t *tracker) lookup(addr string) *conn {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.byAddr[addr]
}

// counts reports the open connections, for tests and logs.
func (t *tracker) counts() (proven, unproven int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.proven, len(t.unproven)
}

// conn is one client connection.
type conn struct {
	net.Conn
	t        *tracker
	addr     string
	timer    *time.Timer
	bytes    *rate.Limiter
	messages *rate.Limiter

	mu        sync.Mutex
	proven    bool
	closed    bool
	refusals  int
	listeners int
}

// Read bounds the bytes a connection may send. khatru checks each event's
// signature before any hook sees it, so this is what bounds that work.
func (c *conn) Read(p []byte) (int, error) {
	if len(p) > c.t.limits.byteBurst {
		p = p[:c.t.limits.byteBurst]
	}
	n, err := c.Conn.Read(p)
	if n > 0 {
		_ = c.bytes.WaitN(context.Background(), n)
	}
	return n, err
}

// Close closes the connection once and forgets it.
func (c *conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	wasProven := c.proven
	c.mu.Unlock()
	if c.timer != nil {
		c.timer.Stop()
	}
	t := c.t
	t.mu.Lock()
	if t.byAddr[c.addr] == c {
		delete(t.byAddr, c.addr)
	}
	if wasProven {
		t.proven--
	} else {
		for i, u := range t.unproven {
			if u == c {
				t.unproven = append(t.unproven[:i], t.unproven[i+1:]...)
				break
			}
		}
	}
	t.mu.Unlock()
	return c.Conn.Close()
}

func (c *conn) isProven() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.proven
}

// prove moves the connection to the proven pool after its first accepted event
// or filter. It fails when the proven pool is full.
func (c *conn) prove() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.proven {
		return true
	}
	if c.closed {
		return false
	}
	t := c.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.proven >= t.limits.proven {
		return false
	}
	for i, u := range t.unproven {
		if u == c {
			t.unproven = append(t.unproven[:i], t.unproven[i+1:]...)
			break
		}
	}
	t.proven++
	c.proven = true
	c.bytes.SetLimit(rate.Limit(t.limits.provenBytes))
	if c.timer != nil {
		c.timer.Stop()
	}
	return true
}

// allow takes one message from the connection's rate.
func (c *conn) allow() bool { return c.messages.Allow() }

// refuse counts a refusal and closes the connection at the limit. It reports
// whether the connection was closed.
func (c *conn) refuse() bool {
	c.mu.Lock()
	c.refusals++
	over := c.refusals >= c.t.limits.refusals
	c.mu.Unlock()
	if over {
		c.t.onDrop("too many refusals", c)
		_ = c.Close()
	}
	return over
}

// The open subscriptions, counted by khatru's listener hooks.
func (c *conn) addListener(delta int) {
	c.mu.Lock()
	c.listeners += delta
	c.mu.Unlock()
}

func (c *conn) listenerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.listeners
}
