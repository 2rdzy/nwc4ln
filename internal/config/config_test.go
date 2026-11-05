package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nwc4ln.toml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// A failure here means nwc4ln starts with wrong settings instead of saying so.
func TestLoadRefusesBadConfig(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{"unknown key", "data_dir='/d'\nlisten='127.0.0.1:1'\nrelayy='x'", "relayy"},
		{"bad syntax", "data_dir=", "read config"},
		{"no data folder", "listen='127.0.0.1:1'", "data_dir is required"},
		{"relative data folder", "data_dir='d'\nlisten='127.0.0.1:1'", "absolute"},
		{"bad listen address", "data_dir='/d'\nlisten='nowhere'", "host:port"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Load(writeFile(t, test.content))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got error %v, want one containing %q", err, test.want)
			}
		})
	}
}

func TestLoadRefusesMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "absent.toml")); err == nil {
		t.Fatal("no error for a missing file")
	}
}

// A failure here means a value does not survive the trip to disk and back.
func TestWriteThenLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nwc4ln.toml")
	want := Default("/data/with \"quotes\"")
	want.Relay = "ws://127.0.0.1:1"
	if err := Write(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// A failure here means a repeated write replaces the operator's settings.
func TestWriteRefusesExistingFile(t *testing.T) {
	path := writeFile(t, "keep me")
	if err := Write(path, Default("/d")); err == nil {
		t.Fatal("no error for an existing file")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "keep me" {
		t.Fatalf("file changed to %q", got)
	}
}
