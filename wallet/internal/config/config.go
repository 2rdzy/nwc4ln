// Package config loads and writes the nwc4ln config file.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

// FileMode is the mode of the config file. The file names secrets on disk, so
// only the service user may read it.
const FileMode = 0o600

// Config holds the settings in the config file. Relay and node settings are
// checked by the code that uses them.
type Config struct {
	DataDir         string `toml:"data_dir"`
	Listen          string `toml:"listen"`
	Relay           string `toml:"relay"`
	LNDHost         string `toml:"lnd_host"`
	LNDTLSCertPath  string `toml:"lnd_tls_cert_path"`
	LNDMacaroonPath string `toml:"lnd_macaroon_path"`
	// Node is the node type, "lnd" or "cln". Empty means "lnd".
	Node string `toml:"node,omitempty"`
	// CLNURL is Core Lightning's clnrest address, such as https://127.0.0.1:3010.
	CLNURL        string `toml:"cln_url,omitempty"`
	CLNCACertPath string `toml:"cln_ca_cert_path,omitempty"`
	// CLNRunePath is a file holding a rune limited to the methods nwc4ln uses.
	CLNRunePath string `toml:"cln_rune_path,omitempty"`
	// AllowedHosts are extra host names the admin UI answers to, such as a local
	// proxy's name. localhost, 127.0.0.1 and ::1 are always allowed.
	AllowedHosts []string `toml:"allowed_hosts,omitempty"`
	// DisableNIP04 turns off the old NWC encryption. Absent or false, NIP-04 is
	// accepted beside NIP-44, so older clients keep working.
	DisableNIP04 bool `toml:"disable_nip04,omitempty"`
	// RelayControl tells the relay, which must be nwc4ln-relay, which keys may use
	// it. Absent or false, the wallet works with any relay and sends no allow-list.
	RelayControl bool `toml:"relay_control,omitempty"`
}

// Default returns the settings for a new config file.
func Default(dataDir string) Config {
	return Config{DataDir: dataDir, Listen: "127.0.0.1:8080"}
}

// DefaultPath returns the config path used when none is given. It refuses to
// return it while only the folder of an older version exists, ~/.nwc4ln: init
// would otherwise start an empty ledger beside the old one, and start would fail
// without saying why. The folder is moved by hand, not by this code.
func DefaultPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("find home folder: %w", err)
	}
	dir := filepath.Join(home, ".nwc4ln-wallet")
	old := filepath.Join(home, ".nwc4ln")
	if _, err := os.Lstat(dir); errors.Is(err, os.ErrNotExist) {
		if _, err := os.Lstat(old); err == nil {
			return "", fmt.Errorf("found the old data folder %s and no %s: stop the service, run "+
				"mv %s %s, rename nwc4ln.toml in it to nwc4ln-wallet.toml, and change data_dir and any "+
				"other paths in that file to the new folder", old, dir, old, dir)
		}
	}
	return filepath.Join(dir, "nwc4ln-wallet.toml"), nil
}

// Load reads and checks the config file. Unknown keys are errors, so that a
// mistyped setting is not silently ignored.
func Load(path string) (Config, error) {
	var cfg Config
	meta, err := toml.DecodeFile(path, &cfg)
	if err != nil {
		return Config{}, fmt.Errorf("read config %s: %w", path, err)
	}
	if unknown := meta.Undecoded(); len(unknown) > 0 {
		keys := make([]string, len(unknown))
		for i, key := range unknown {
			keys[i] = key.String()
		}
		return Config{}, fmt.Errorf("read config %s: unknown keys: %s", path, strings.Join(keys, ", "))
	}
	if err := cfg.validate(); err != nil {
		return Config{}, fmt.Errorf("check config %s: %w", path, err)
	}
	return cfg, nil
}

// Write creates the config file. It fails if the file already exists.
func Write(path string, cfg Config) (err error) {
	if err := cfg.validate(); err != nil {
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

// Node types.
const (
	NodeLND = "lnd"
	NodeCLN = "cln"
)

// NodeType gives the node type, with LND for an empty setting.
func (c Config) NodeType() string {
	if c.Node == "" {
		return NodeLND
	}
	return c.Node
}

func (c Config) validate() error {
	if t := c.NodeType(); t != NodeLND && t != NodeCLN {
		return fmt.Errorf("node must be %q or %q, got %q", NodeLND, NodeCLN, c.Node)
	}
	if c.DataDir == "" {
		return errors.New("data_dir is required")
	}
	if !filepath.IsAbs(c.DataDir) {
		return fmt.Errorf("data_dir must be an absolute path, got %q", c.DataDir)
	}
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		return fmt.Errorf("listen must be host:port, got %q: %w", c.Listen, err)
	}
	if !IsLoopback(c.Listen) {
		return fmt.Errorf("listen must be a loopback address, got %q: use an SSH tunnel or a local proxy for remote use", c.Listen)
	}
	for _, host := range c.AllowedHosts {
		if host == "" || strings.ContainsAny(host, ":/ ") {
			return fmt.Errorf("allowed_hosts takes host names without a port, got %q", host)
		}
	}
	return nil
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
