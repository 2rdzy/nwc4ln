// Package integration runs a throwaway BLAKE2b regtest network for tests: one
// Knots node, three lightning-fork LND nodes, one Core Lightning node, and a
// local Nostr relay. See README.md for how to use it.
package integration

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"strings"
)

//go:embed versions.env
var versionsFile string

// pins holds the values of versions.env.
type pins struct {
	knotsVersion, knotsSHA256                  string
	lndRepo, lndTag, lndCommit, lndGo, lndTags string
	nakVersion, nakSHA256                      string
	clnRepo, clnVersion                        string
	// clnSHA256 is keyed by Ubuntu release, such as "24.04".
	clnSHA256 map[string]string
	// cacheKey names the binary folder, so changing a pin never reuses old files.
	cacheKey string
}

func loadPins() (pins, error) {
	values := map[string]string{}
	for line := range strings.Lines(versionsFile) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return pins{}, fmt.Errorf("versions.env: bad line %q", line)
		}
		values[key] = value
	}
	get := func(key string) string { return values[key] }
	p := pins{
		knotsVersion: get("KNOTS_VERSION"),
		knotsSHA256:  get("KNOTS_SHA256"),
		lndRepo:      get("LND_REPO"),
		lndTag:       get("LND_TAG"),
		lndCommit:    get("LND_COMMIT"),
		lndGo:        get("LND_GO"),
		lndTags:      get("LND_TAGS"),
		nakVersion:   get("NAK_VERSION"),
		nakSHA256:    get("NAK_SHA256"),
		clnRepo:      get("CLN_REPO"),
		clnVersion:   get("CLN_VERSION"),
		clnSHA256: map[string]string{
			"24.04": get("CLN_SHA256_UBUNTU_24_04"),
			"26.04": get("CLN_SHA256_UBUNTU_26_04"),
		},
	}
	for _, value := range []string{
		p.knotsVersion, p.knotsSHA256, p.lndRepo, p.lndTag, p.lndCommit, p.lndGo, p.lndTags, p.nakVersion, p.nakSHA256,
		p.clnRepo, p.clnVersion, p.clnSHA256["24.04"], p.clnSHA256["26.04"],
	} {
		if value == "" {
			return pins{}, fmt.Errorf("versions.env: a required value is missing")
		}
	}
	sum := sha256.Sum256([]byte(versionsFile))
	p.cacheKey = hex.EncodeToString(sum[:])[:12]
	return p, nil
}
