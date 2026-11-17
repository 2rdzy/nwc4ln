// Package store owns the SQLite database and its schema.
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"syscall"

	_ "modernc.org/sqlite" // Pure-Go SQLite driver, so the binary needs no C toolchain.
)

// FileMode is the mode of the database file. It holds wallet service keys.
// SQLite gives the WAL files the same mode.
const FileMode = 0o600

//go:embed migrations/*.sql
var migrations embed.FS

// Store is the open database.
type Store struct {
	DB *sql.DB
}

// Create makes a new database and applies the schema. It fails if the file
// already exists, so that it can never replace a ledger.
func Create(ctx context.Context, path string) (*Store, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, FileMode)
	if err != nil {
		return nil, fmt.Errorf("create database: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("create database: %w", err)
	}
	return open(ctx, path)
}

// ErrLocked means another process is running on this data folder.
var ErrLocked = errors.New("another nwc4ln is running on this data folder")

// LockFileName is the lock file in the data folder.
const LockFileName = "nwc4ln.lock"

// Lock takes the data folder for this process, and returns how to let it go. The
// ledger knows only the payments it started itself, so a second one would fail
// the first one's new payments. The system drops the lock if the process dies, so
// a crash leaves nothing behind. Readers such as backup do not need it.
func Lock(dataDir string) (release func() error, err error) {
	file, err := os.OpenFile(filepath.Join(dataDir, LockFileName), os.O_RDWR|os.O_CREATE, FileMode)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		closeErr := file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errors.Join(ErrLocked, closeErr)
		}
		return nil, errors.Join(fmt.Errorf("lock data folder: %w", err), closeErr)
	}
	return file.Close, nil
}

// Open opens an existing database and applies any new migrations.
func Open(ctx context.Context, path string) (*Store, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	return open(ctx, path)
}

func open(ctx context.Context, path string) (*Store, error) {
	dsn, err := dataSource(path)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	dir, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if err := migrate(ctx, db, dir); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return &Store{DB: db}, nil
}

// OpenReadOnly opens an existing database for reading only, with no migrations, so
// a reader such as check never changes a database the service has open. It fails if
// the schema is not the one this binary knows.
func OpenReadOnly(ctx context.Context, path string) (*Store, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("database path: %w", err)
	}
	query := url.Values{"mode": {"ro"}}
	query.Add("_pragma", "busy_timeout(5000)")
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: abs, RawQuery: query.Encode()}).String())
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	files, err := migrationFiles(must(fs.Sub(migrations, "migrations")))
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return nil, errors.Join(fmt.Errorf("read schema version: %w", err), db.Close())
	}
	if version != len(files) {
		return nil, errors.Join(fmt.Errorf("database schema version %d, this binary knows %d: start the service once first",
			version, len(files)), db.Close())
	}
	return &Store{DB: db}, nil
}

func must[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

// Close closes the database. A clean close also removes the WAL file.
func (s *Store) Close() error {
	return s.DB.Close()
}

// dataSource sets the options on every connection: WAL mode, foreign keys, a
// wait when the database is busy, and a full sync on each commit, so that a
// power cut cannot lose a committed payment. Transactions take the write lock
// when they begin, so that two cannot deadlock upgrading a read lock.
func dataSource(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("database path: %w", err)
	}
	query := url.Values{"_txlock": {"immediate"}}
	for _, pragma := range []string{
		"journal_mode(WAL)", "foreign_keys(1)", "busy_timeout(5000)", "synchronous(FULL)",
	} {
		query.Add("_pragma", pragma)
	}
	return (&url.URL{Scheme: "file", Path: abs, RawQuery: query.Encode()}).String(), nil
}

var migrationName = regexp.MustCompile(`^(\d{4})_[a-z0-9_]+\.sql$`)

// migrate applies the files in dir whose number is above the database's
// user_version. Each runs in one transaction with its version change, so a
// failed migration leaves the database as it was.
func migrate(ctx context.Context, db *sql.DB, dir fs.FS) error {
	files, err := migrationFiles(dir)
	if err != nil {
		return err
	}
	var current int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&current); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if current > len(files) {
		return fmt.Errorf("database schema version %d is newer than this binary's %d", current, len(files))
	}
	for i, name := range files[current:] {
		version := current + i + 1
		script, err := fs.ReadFile(dir, name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if err := applyMigration(ctx, db, version, string(script)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
	}
	return nil
}

func applyMigration(ctx context.Context, db *sql.DB, version int, script string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }() // Does nothing after a commit.
	if _, err := tx.ExecContext(ctx, script); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "PRAGMA user_version = "+strconv.Itoa(version)); err != nil {
		return err
	}
	return tx.Commit()
}

// migrationFiles lists the files in order, and checks they are numbered 1, 2,
// 3 and so on with no gaps.
func migrationFiles(dir fs.FS) ([]string, error) {
	entries, err := fs.ReadDir(dir, ".")
	if err != nil {
		return nil, fmt.Errorf("list migrations: %w", err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		files = append(files, entry.Name())
	}
	slices.Sort(files)
	for i, name := range files {
		match := migrationName.FindStringSubmatch(name)
		if match == nil {
			return nil, fmt.Errorf("bad migration name %q", name)
		}
		if number, _ := strconv.Atoi(match[1]); number != i+1 {
			return nil, fmt.Errorf("migration %q: expected number %d", name, i+1)
		}
	}
	return files, nil
}

// Backup writes a copy of the database at path to a new file at dest, with VACUUM
// INTO. It works while the service runs and writes, because it reads one snapshot, so
// it needs no lock. It runs no migrations, so it never changes the live database.
//
// The file is created empty with FileMode first, because VACUUM INTO alone gives
// one that other users can read, and it holds the wallet service keys. An existing
// file is refused and left alone. After a failure, the new file is removed.
func Backup(ctx context.Context, path, dest string) (err error) {
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("backup: %w", err)
	}
	dest, err = filepath.Abs(dest)
	if err != nil {
		return fmt.Errorf("backup: %w", err)
	}
	file, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, FileMode)
	if err != nil {
		return fmt.Errorf("backup: %w", err)
	}
	if err := file.Close(); err != nil {
		return errors.Join(fmt.Errorf("backup: %w", err), os.Remove(dest))
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, os.Remove(dest))
		}
	}()
	dsn, err := dataSource(path)
	if err != nil {
		return err
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("backup: %w", err)
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("backup: read schema version: %w", err)
	}
	if version == 0 {
		return fmt.Errorf("backup: %s is not an nwc4ln database", path)
	}
	if _, err := db.ExecContext(ctx, "VACUUM INTO ?", dest); err != nil {
		return fmt.Errorf("backup: %w", err)
	}
	return syncFile(dest)
}

// syncFile makes a new file and its folder entry survive a power cut.
func syncFile(path string) error {
	for _, name := range []string{path, filepath.Dir(path)} {
		file, err := os.Open(name)
		if err != nil {
			return fmt.Errorf("backup: %w", err)
		}
		err = file.Sync()
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return fmt.Errorf("backup: sync %s: %w", name, err)
		}
	}
	return nil
}

// adminKeySetting is the settings row with the admin's public key, in hex.
const adminKeySetting = "admin_pubkey"

// AdminKey returns the admin's public key in hex, or "" if none is set.
func (s *Store) AdminKey(ctx context.Context) (string, error) {
	var key string
	err := s.DB.QueryRowContext(ctx, `SELECT value FROM settings WHERE key = ?`, adminKeySetting).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return key, err
}

// SetAdminKey stores the admin's public key, in hex, replacing any earlier one.
func (s *Store) SetAdminKey(ctx context.Context, key string) error {
	if !hexKey.MatchString(key) {
		return fmt.Errorf("admin key must be 64 lowercase hex characters, got %q", key)
	}
	_, err := s.DB.ExecContext(ctx, `INSERT INTO settings (key, value) VALUES (?, ?)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value`, adminKeySetting, key)
	return err
}

var hexKey = regexp.MustCompile(`^[0-9a-f]{64}$`)
