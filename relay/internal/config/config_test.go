package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fiatjaf.com/nostr"
	"fiatjaf.com/nostr/nip19"
)

func write(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nwc4ln-relay.toml")
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDefaults(t *testing.T) {
	key := nostr.Generate().Public()
	dir := t.TempDir()
	path := filepath.Join(dir, "nwc4ln-relay.toml")
	if err := Write(path, Default(dir, key)); err != nil {
		t.Fatal(err)
	}
	s, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.ControlKey != key || s.Listen != "127.0.0.1:7447" || s.RequestBuffer != 10*time.Minute ||
		s.MessageBytes != 128*1024 || s.Connections != 256 || s.UnprovenConns != 64 ||
		s.Subscriptions != 10 || s.Refusals != 20 || s.BufferRequests != 5000 {
		t.Fatalf("defaults: %+v", s)
	}
	if err := Write(path, Default(dir, key)); err == nil {
		t.Fatal("init wrote over an existing config")
	}
}

func TestRefuses(t *testing.T) {
	key := nostr.Generate().Public().Hex()
	base := "data_dir = \"/tmp/x\"\ncontrol_key = \"" + key + "\"\n"
	cases := map[string]string{
		"listen on every interface":             base + "listen = \"0.0.0.0:7447\"\n",
		"listen with no host":                   base + "listen = \":7447\"\n",
		"message limit below the largest reply": base + "listen = \"127.0.0.1:1\"\n[limits]\nmessage_bytes = 65536\n",
		"fewer than 3 subscriptions":            base + "listen = \"127.0.0.1:1\"\n[limits]\nsubscriptions = 2\n",
		"buffer over 6 hours":                   base + "listen = \"127.0.0.1:1\"\nrequest_buffer = \"7h\"\n",
		"unknown key":                           base + "listen = \"127.0.0.1:1\"\nlisten_port = 1\n",
		"uppercase control key":                 "data_dir = \"/tmp/x\"\nlisten = \"127.0.0.1:1\"\ncontrol_key = \"" + strings.ToUpper(key) + "\"\n",
		"relative data folder":                  "data_dir = \"x\"\nlisten = \"127.0.0.1:1\"\ncontrol_key = \"" + key + "\"\n",
	}
	for name, text := range cases {
		if _, err := Load(write(t, text)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	open := base + "listen = \"0.0.0.0:7447\"\nallow_non_loopback = true\n"
	if _, err := Load(write(t, open)); err != nil {
		t.Errorf("allow_non_loopback did not allow it: %v", err)
	}
}

func TestParseKey(t *testing.T) {
	key := nostr.Generate().Public()
	for _, text := range []string{key.Hex(), nip19.EncodeNpub(key)} {
		got, err := ParseKey(text)
		if err != nil || got != key {
			t.Errorf("%s: %v %v", text, got, err)
		}
	}
	if _, err := ParseKey("npub1xyz"); err == nil {
		t.Error("a broken npub was accepted")
	}
}
