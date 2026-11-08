package integration

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// fetchBinaries returns a folder holding the pinned bitcoind, bitcoin-cli, lnd,
// lncli, nak and Core Lightning (under cln/), and fills it on first use. A "complete" file marks a finished
// folder, so an interrupted fetch starts again.
func fetchBinaries(ctx context.Context, p pins) (string, error) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return "", errors.New("the integration network needs Linux amd64")
	}
	root, err := cacheRoot()
	if err != nil {
		return "", err
	}
	binDir := filepath.Join(root, p.cacheKey)
	complete := filepath.Join(binDir, "complete")
	if _, err := os.Stat(complete); err == nil {
		return binDir, nil
	}
	if err := os.RemoveAll(binDir); err != nil {
		return "", err
	}
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return "", err
	}
	if err := fetchKnots(ctx, p, binDir); err != nil {
		return "", fmt.Errorf("fetch Knots: %w", err)
	}
	if err := fetchNak(ctx, p, binDir); err != nil {
		return "", fmt.Errorf("fetch nak: %w", err)
	}
	if err := fetchCLN(ctx, p, binDir); err != nil {
		return "", fmt.Errorf("fetch Core Lightning: %w", err)
	}
	if err := buildLND(ctx, p, binDir); err != nil {
		return "", fmt.Errorf("build lightning-fork: %w", err)
	}
	return binDir, os.WriteFile(complete, nil, 0o644)
}

func cacheRoot() (string, error) {
	if dir := os.Getenv("NWC4LN_IT_CACHE"); dir != "" {
		return dir, nil
	}
	home, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "nwc4ln-it"), nil
}

func fetchKnots(ctx context.Context, p pins, binDir string) error {
	url := fmt.Sprintf(
		"https://github.com/bitcoinknots/bitcoin/releases/download/v%[1]s/bitcoin-%[1]s-x86_64-linux-gnu.tar.gz",
		p.knotsVersion)
	archive := filepath.Join(binDir, "knots.tar.gz")
	if err := download(ctx, url, archive, p.knotsSHA256); err != nil {
		return err
	}
	defer os.Remove(archive)
	return extractKnots(archive, binDir)
}

func extractKnots(archive, binDir string) error {
	file, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer file.Close()
	unzipped, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	wanted := map[string]bool{"bitcoind": true, "bitcoin-cli": true}
	reader := tar.NewReader(unzipped)
	for len(wanted) > 0 {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("archive lacks %v", wanted)
		}
		if err != nil {
			return err
		}
		name := filepath.Base(header.Name)
		if !wanted[name] || filepath.Base(filepath.Dir(header.Name)) != "bin" {
			continue
		}
		delete(wanted, name)
		out, err := os.OpenFile(filepath.Join(binDir, name), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
		if err != nil {
			return err
		}
		_, err = io.Copy(out, reader)
		if err := errors.Join(err, out.Close()); err != nil {
			return err
		}
	}
	return nil
}

// fetchCLN unpacks the Core Lightning release for this Ubuntu release into
// binDir/cln, keeping its layout: lightningd finds its helpers and plugins from
// its own path. Go has no xz reader, so tar unpacks it.
func fetchCLN(ctx context.Context, p pins, binDir string) error {
	release, err := ubuntuRelease()
	if err != nil {
		return err
	}
	sum := p.clnSHA256[release]
	if sum == "" {
		return fmt.Errorf("no Core Lightning build is pinned for Ubuntu %s", release)
	}
	url := fmt.Sprintf("%[1]s/releases/download/%[2]s/clightning-%[2]s-Ubuntu-%[3]s-amd64.tar.xz",
		p.clnRepo, p.clnVersion, release)
	archive := filepath.Join(binDir, "cln.tar.xz")
	if err := download(ctx, url, archive, sum); err != nil {
		return err
	}
	defer os.Remove(archive)
	target := filepath.Join(binDir, "cln")
	if err := os.MkdirAll(target, 0o755); err != nil {
		return err
	}
	if output, err := exec.CommandContext(ctx, "tar", "-xJf", archive, "-C", target).CombinedOutput(); err != nil {
		return fmt.Errorf("unpack: %w: %s", err, output)
	}
	return nil
}

// ubuntuRelease reads VERSION_ID from /etc/os-release.
func ubuntuRelease() (string, error) {
	data, err := os.ReadFile("/etc/os-release")
	if err != nil {
		return "", err
	}
	var id, version string
	for line := range strings.Lines(string(data)) {
		key, value, _ := strings.Cut(strings.TrimSpace(line), "=")
		value = strings.Trim(value, `"`)
		switch key {
		case "ID":
			id = value
		case "VERSION_ID":
			version = value
		}
	}
	if id != "ubuntu" {
		return "", fmt.Errorf("core Lightning builds are pinned for Ubuntu only, this is %q", id)
	}
	return version, nil
}

func fetchNak(ctx context.Context, p pins, binDir string) error {
	url := fmt.Sprintf("https://github.com/fiatjaf/nak/releases/download/%[1]s/nak-%[1]s-linux-amd64", p.nakVersion)
	target := filepath.Join(binDir, "nak")
	if err := download(ctx, url, target, p.nakSHA256); err != nil {
		return err
	}
	return os.Chmod(target, 0o755)
}

// download saves url to target if its SHA-256 matches, and retries a failed
// connection. GitHub connections here have been seen to hang for minutes, so a
// short connect timeout matters.
func download(ctx context.Context, url, target, wantSHA256 string) error {
	client := &http.Client{
		Timeout:   10 * time.Minute,
		Transport: &http.Transport{DialContext: (&net.Dialer{Timeout: 15 * time.Second}).DialContext},
	}
	var err error
	for range 4 {
		if err = downloadOnce(ctx, client, url, target, wantSHA256); err == nil || ctx.Err() != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "retrying %s: %v\n", url, err)
	}
	return err
}

func downloadOnce(ctx context.Context, client *http.Client, url, target, wantSHA256 string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, response.Status)
	}
	out, err := os.Create(target)
	if err != nil {
		return err
	}
	hash := sha256.New()
	_, err = io.Copy(io.MultiWriter(out, hash), response.Body)
	if err := errors.Join(err, out.Close()); err != nil {
		return err
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != wantSHA256 {
		os.Remove(target)
		return fmt.Errorf("%s: SHA-256 is %s, want %s", url, got, wantSHA256)
	}
	return nil
}

func buildLND(ctx context.Context, p pins, binDir string) error {
	source, err := os.MkdirTemp(binDir, "lnd-source-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(source)

	clone := exec.CommandContext(ctx, "git", "clone", "--quiet", "--depth", "1", "--branch", p.lndTag, p.lndRepo, source)
	if output, err := clone.CombinedOutput(); err != nil {
		return fmt.Errorf("git clone: %w: %s", err, output)
	}
	head := exec.CommandContext(ctx, "git", "-C", source, "rev-parse", "HEAD")
	output, err := head.Output()
	if err != nil {
		return fmt.Errorf("git rev-parse: %w", err)
	}
	if got := strings.TrimSpace(string(output)); got != p.lndCommit {
		return fmt.Errorf("tag %s is at %s, want %s", p.lndTag, got, p.lndCommit)
	}
	for _, name := range []string{"lnd", "lncli"} {
		fmt.Fprintf(os.Stderr, "building %s, which takes some minutes\n", name)
		build := exec.CommandContext(ctx, "go", "build", "-trimpath", "-ldflags", "-s -w",
			"-tags", p.lndTags, "-o", filepath.Join(binDir, name), "./cmd/"+name)
		build.Dir = source
		build.Env = append(os.Environ(), "GOTOOLCHAIN="+p.lndGo, "CGO_ENABLED=0", "GOWORK=off", "GOFLAGS=")
		if output, err := build.CombinedOutput(); err != nil {
			return fmt.Errorf("go build %s: %w: %s", name, err, output)
		}
	}
	return nil
}
