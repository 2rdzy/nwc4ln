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
