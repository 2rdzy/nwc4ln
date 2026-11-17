package config

import (
	"fmt"
	"os"
)

// CheckPrivate refuses a file that users other than its owner can read or
// write, because it holds secrets: the database holds the wallet service keys.
// A missing file is not refused, so optional files such as the WAL can be
// checked too.
func CheckPrivate(path string) error {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		return fmt.Errorf("%s has mode %#o: allow its owner only (chmod 600 %s)", path, mode, path)
	}
	return nil
}

// CheckNotShared refuses a file or folder that users other than its owner can
// change: they could point the config at another node or macaroon, or replace
// the database.
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
