// Package config loads and writes the nwc4ln-relay config file.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fiatjaf.com/nostr"
	"fiatjaf.com/nostr/nip19"
	"github.com/BurntSushi/toml"
)

// FileMode is the mode of the config file and of the saved allow-list. Neither
// holds a secret, but only the service user should be able to change them.
const FileMode = 0o600

// MinMessageBytes is the smallest message size limit the relay accepts. A wallet
// reply may carry 65,535 bytes of text, which is 87,472 bytes of NIP-44 content.
// With the event's other fields and the message frame it is about 88 KB.
const MinMessageBytes = 96 * 1024

// MaxRequestBuffer is the longest time requests are kept. The wallet refuses a
// request older than six hours, so a longer time keeps only requests nobody runs.
const MaxRequestBuffer = 6 * time.Hour

// Config holds the settings in the config file.
type Config struct {
	DataDir string `toml:"data_dir"`
	// Listen is the address the relay listens on, in clear text. It must be a
	// loopback address unless AllowNonLoopback is set: a proxy carries TLS.
	Listen           string `toml:"listen"`
	AllowNonLoopback bool   `toml:"allow_non_loopback,omitempty"`
	// ControlKey is the wallet's control public key, in hex. Only it may sign the
	// allow-list.
	ControlKey string `toml:"control_key"`
	// RequestBuffer is how long requests are kept for a wallet that reconnects,
	// such as "10m". Empty means 10 minutes.
	RequestBuffer string `toml:"request_buffer,omitempty"`
	Limits        Limits `toml:"limits,omitempty"`
}

// Limits bound the work strangers and clients can cause. A zero value means the
// default.
type Limits struct {
	MessageBytes        int64   `toml:"message_bytes,omitempty"`
	Connections         int     `toml:"connections,omitempty"`
	UnprovenConns       int     `toml:"unproven_connections,omitempty"`
	UnprovenTimeout     string  `toml:"unproven_timeout,omitempty"`
	Refusals            int     `toml:"refusals,omitempty"`
	MessagesPerSecond   float64 `toml:"messages_per_second,omitempty"`
	MessageBurst        int     `toml:"message_burst,omitempty"`
	Subscriptions       int     `toml:"subscriptions,omitempty"`
	BufferRequests      int     `toml:"buffer_requests,omitempty"`
	BufferPerClient     int     `toml:"buffer_per_client,omitempty"`
	BufferBytes         int64   `toml:"buffer_bytes,omitempty"`
	ProvenBytesPerSec   int     `toml:"proven_bytes_per_second,omitempty"`
	UnprovenBytesPerSec int     `toml:"unproven_bytes_per_second,omitempty"`
}

// Defaults for the limits (WP16 STOP 1, owner, 2026-11-19).
const (
	defaultMessageBytes      = 128 * 1024
	defaultConnections       = 256
	defaultUnproven          = 64
	defaultUnprovenTimeout   = 30 * time.Second
	defaultRefusals          = 20
	defaultMessagesPerSecond = 50
	defaultMessageBurst      = 1000
	defaultSubscriptions     = 10
	defaultBufferRequests    = 5000
	defaultBufferPerClient   = 500
	defaultBufferBytes       = 64 * 1024 * 1024
	defaultRequestBuffer     = 10 * time.Minute
	// A connection that has not yet sent an accepted event or filter may still
	// send one full allow-list at once, then 32 KiB a second. This bounds the
	// signature checks a stranger can cause before the relay sees the event.
	defaultUnprovenBytesPerSec = 32 * 1024
	defaultProvenBytesPerSec   = 1024 * 1024
)

// Settings are the checked values the relay runs with.
type Settings struct {
	DataDir             string
	Listen              string
	ControlKey          nostr.PubKey
	RequestBuffer       time.Duration
	MessageBytes        int64
	Connections         int
	UnprovenConns       int
	UnprovenTimeout     time.Duration
	Refusals            int
	MessagesPerSec      float64
	MessageBurst        int
	Subscriptions       int
	BufferRequests      int
	BufferPerClient     int
	BufferBytes         int64
	ProvenBytesPerSec   int
	UnprovenBytesPerSec int
	AllowNonLoopback    bool
}

// Default returns the settings for a new config file.
func Default(dataDir string, control nostr.PubKey) Config {
	return Config{DataDir: dataDir, Listen: "127.0.0.1:7447", ControlKey: control.Hex()}
}

// DefaultPath returns the config path used when none is given.
func DefaultPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("find home folder: %w", err)
	}
	return filepath.Join(home, ".nwc4ln-relay", "nwc4ln-relay.toml"), nil
}

// Load reads and checks the config file. Unknown keys are errors, so that a
// mistyped setting is not silently ignored.
func Load(path string) (Settings, error) {
	var cfg Config
	meta, err := toml.DecodeFile(path, &cfg)
	if err != nil {
		return Settings{}, fmt.Errorf("read config %s: %w", path, err)
	}
	if unknown := meta.Undecoded(); len(unknown) > 0 {
		keys := make([]string, len(unknown))
		for i, key := range unknown {
			keys[i] = key.String()
		}
		return Settings{}, fmt.Errorf("read config %s: unknown keys: %s", path, strings.Join(keys, ", "))
	}
	settings, err := cfg.Check()
	if err != nil {
		return Settings{}, fmt.Errorf("check config %s: %w", path, err)
	}
	return settings, nil
}

// Write creates the config file. It fails if the file already exists.
func Write(path string, cfg Config) (err error) {
	if _, err := cfg.Check(); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, FileMode)
	if err != nil {
		return fmt.Errorf("create config: %w", err)
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	if err := toml.NewEncoder(file).Encode(cfg); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

// ParseKey reads a public key given as npub or as 64 lowercase hex digits.
func ParseKey(text string) (nostr.PubKey, error) {
	if strings.HasPrefix(text, "npub1") {
		prefix, value, err := nip19.Decode(text)
		if err != nil || prefix != "npub" {
			return nostr.PubKey{}, fmt.Errorf("not a valid npub: %q", text)
		}
		key, ok := value.(nostr.PubKey)
		if !ok {
			return nostr.PubKey{}, fmt.Errorf("not a valid npub: %q", text)
		}
		return key, nil
	}
	if len(text) != 64 || strings.ToLower(text) != text {
		return nostr.PubKey{}, fmt.Errorf("a key is an npub or 64 lowercase hex digits, got %q", text)
	}
	key, err := nostr.PubKeyFromHex(text)
	if err != nil {
		return nostr.PubKey{}, fmt.Errorf("not a valid public key: %q: %w", text, err)
	}
	return key, nil
}

// Check validates the config and fills in defaults.
func (c Config) Check() (Settings, error) {
	s := Settings{DataDir: c.DataDir, Listen: c.Listen, AllowNonLoopback: c.AllowNonLoopback}
	if c.DataDir == "" {
		return s, errors.New("data_dir is required")
	}
	if !filepath.IsAbs(c.DataDir) {
		return s, fmt.Errorf("data_dir must be an absolute path, got %q", c.DataDir)
	}
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		return s, fmt.Errorf("listen must be host:port, got %q: %w", c.Listen, err)
	}
	if !c.AllowNonLoopback && !IsLoopback(c.Listen) {
		return s, fmt.Errorf("listen must be a loopback address, got %q: put a TLS proxy in front, "+
			"or set allow_non_loopback = true", c.Listen)
	}
	key, err := ParseKey(c.ControlKey)
	if err != nil {
		return s, fmt.Errorf("control_key: %w", err)
	}
	s.ControlKey = key

	if s.RequestBuffer, err = duration("request_buffer", c.RequestBuffer, defaultRequestBuffer); err != nil {
		return s, err
	}
	if s.RequestBuffer > MaxRequestBuffer {
		return s, fmt.Errorf("request_buffer is at most %s, got %s", MaxRequestBuffer, s.RequestBuffer)
	}
	l := c.Limits
	if s.UnprovenTimeout, err = duration("limits.unproven_timeout", l.UnprovenTimeout, defaultUnprovenTimeout); err != nil {
		return s, err
	}
	s.MessageBytes = orDefault(l.MessageBytes, defaultMessageBytes)
	if s.MessageBytes < MinMessageBytes {
		return s, fmt.Errorf("limits.message_bytes is at least %d, the largest wallet reply, got %d",
			MinMessageBytes, s.MessageBytes)
	}
	s.Connections = orDefault(l.Connections, defaultConnections)
	s.UnprovenConns = orDefault(l.UnprovenConns, defaultUnproven)
	s.Refusals = orDefault(l.Refusals, defaultRefusals)
	s.MessagesPerSec = orDefault(l.MessagesPerSecond, defaultMessagesPerSecond)
	s.MessageBurst = orDefault(l.MessageBurst, defaultMessageBurst)
	s.Subscriptions = orDefault(l.Subscriptions, defaultSubscriptions)
	if s.Subscriptions < 3 {
		return s, fmt.Errorf("limits.subscriptions is at least 3: the wallet holds 3 while it changes, got %d",
			s.Subscriptions)
	}
	s.BufferRequests = orDefault(l.BufferRequests, defaultBufferRequests)
	s.BufferPerClient = orDefault(l.BufferPerClient, defaultBufferPerClient)
	s.BufferBytes = orDefault(l.BufferBytes, defaultBufferBytes)
	s.ProvenBytesPerSec = orDefault(l.ProvenBytesPerSec, defaultProvenBytesPerSec)
	s.UnprovenBytesPerSec = orDefault(l.UnprovenBytesPerSec, defaultUnprovenBytesPerSec)
	for name, v := range map[string]float64{
		"limits.connections": float64(s.Connections), "limits.unproven_connections": float64(s.UnprovenConns),
		"limits.refusals": float64(s.Refusals), "limits.messages_per_second": s.MessagesPerSec,
		"limits.message_burst": float64(s.MessageBurst), "limits.buffer_requests": float64(s.BufferRequests),
		"limits.buffer_per_client": float64(s.BufferPerClient), "limits.buffer_bytes": float64(s.BufferBytes),
		"limits.proven_bytes_per_second":   float64(s.ProvenBytesPerSec),
		"limits.unproven_bytes_per_second": float64(s.UnprovenBytesPerSec),
	} {
		if v < 0 {
			return s, fmt.Errorf("%s must not be negative", name)
		}
	}
	return s, nil
}

func duration(name, text string, fallback time.Duration) (time.Duration, error) {
	if text == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(text)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration such as \"10m\", got %q", name, text)
	}
	return d, nil
}

func orDefault[T int | int64 | float64](v, fallback T) T {
	if v == 0 {
		return fallback
	}
	return v
}

// IsLoopback reports whether a host:port address is on the loopback interface
// only. An empty host would listen on every interface.
func IsLoopback(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// CheckNotShared refuses a file or folder that users other than its owner can
// change: they could replace the allow-list or the control key.
func CheckNotShared(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if mode := info.Mode().Perm(); mode&0o022 != 0 {
		return fmt.Errorf("%s has mode %#o: others can change it (chmod go-w %s)", path, mode, path)
	}
	return nil
}
