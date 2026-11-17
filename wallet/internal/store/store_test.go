package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"testing/fstest"
)

func newStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "with space.db")
	db, err := Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, path
}

func version(t *testing.T, db *sql.DB) int {
	t.Helper()
	var got int
	if err := db.QueryRow("PRAGMA user_version").Scan(&got); err != nil {
		t.Fatal(err)
	}
	return got
}

func migrationCount(t *testing.T) int {
	t.Helper()
	files, err := os.ReadDir("migrations")
	if err != nil {
		t.Fatal(err)
	}
	return len(files)
}

// A failure here means every start after the first one fails, or data is lost.
func TestMigrationsRunOnEmptyDatabaseThenAgain(t *testing.T) {
	db, path := newStore(t)
	if got, want := version(t, db.DB), migrationCount(t); got != want {
		t.Fatalf("version %d, want %d", got, want)
	}
	if _, err := db.DB.Exec("INSERT INTO settings VALUES ('marker', 'kept')"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	again, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	var got string
	if err := again.DB.QueryRow("SELECT value FROM settings WHERE key = 'marker'").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "kept" {
		t.Fatalf("got %q", got)
	}
}

// A failure here means a failed upgrade leaves a half-built schema.
func TestFailedMigrationRollsBack(t *testing.T) {
	db, _ := newStore(t)
	before := version(t, db.DB)
	dir := fstest.MapFS{}
	for i := 1; i <= before; i++ {
		dir[fmt.Sprintf("%04d_old.sql", i)] = &fstest.MapFile{Data: []byte("SELECT 1;")}
	}
	bad := "CREATE TABLE half (id INTEGER); INSERT INTO absent VALUES (1);"
	dir[fmt.Sprintf("%04d_bad.sql", before+1)] = &fstest.MapFile{Data: []byte(bad)}

	if err := migrate(context.Background(), db.DB, dir); err == nil {
		t.Fatal("no error from a bad migration")
	}
	if got := version(t, db.DB); got != before {
		t.Fatalf("version %d, want %d", got, before)
	}
	var tables int
	err := db.DB.QueryRow("SELECT count(*) FROM sqlite_master WHERE name = 'half'").Scan(&tables)
	if err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatal("table from the failed migration still exists")
	}
}

// A failure here means an old binary runs against a schema it does not know.
func TestOpenRefusesNewerDatabase(t *testing.T) {
	db, path := newStore(t)
	if _, err := db.DB.Exec("PRAGMA user_version = 9999"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), path); err == nil {
		t.Fatal("no error for a newer database")
	}
}

// A failure here means a gap in the numbers silently skips a migration.
func TestMigrationFilesRefusesGap(t *testing.T) {
	dir := fstest.MapFS{"0001_a.sql": {}, "0003_b.sql": {}}
	if _, err := migrationFiles(dir); err == nil {
		t.Fatal("no error for a gap")
	}
}

// A failure here means rule R13 has nothing to rely on, and a repeat can pay twice.
func TestDuplicateNostrEventRefused(t *testing.T) {
	db, _ := newStore(t)
	const insert = "INSERT INTO requests (nostr_event_id, state, created_at) VALUES ('event', 'new', 1)"
	if _, err := db.DB.Exec(insert); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB.Exec(insert); err == nil {
		t.Fatal("duplicate event accepted")
	}
}

// A failure here means foreign keys are off on some connections.
func TestForeignKeysEnforced(t *testing.T) {
	db, _ := newStore(t)
	_, err := db.DB.Exec(`INSERT INTO connections
		(account_id, client_pubkey, service_key, role, created_at) VALUES (99, 'a', 'b', 'full', 1)`)
	if err == nil {
		t.Fatal("connection accepted for an account that does not exist")
	}
}

// A failure here means service keys are readable by other users.
func TestCreateFileModes(t *testing.T) {
	db, path := newStore(t)
	if _, err := db.DB.Exec("INSERT INTO settings VALUES ('a', 'b')"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{path, path + "-wal", path + "-shm"} {
		info, err := os.Stat(name)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != FileMode {
			t.Errorf("%s has mode %o, want %o", filepath.Base(name), got, FileMode)
		}
	}
}

// A failure here means a repeated create wipes the ledger.
func TestCreateRefusesExistingFile(t *testing.T) {
	_, path := newStore(t)
	if _, err := Create(context.Background(), path); err == nil {
		t.Fatal("no error for an existing database")
	}
}

func TestOpenRefusesMissingFile(t *testing.T) {
	if _, err := Open(context.Background(), filepath.Join(t.TempDir(), "absent.db")); err == nil {
		t.Fatal("no error for a missing database")
	}
}

// Catches: two processes on one data folder, whose ledgers would fail each other's
// new payments, and a lock that stays after its owner has gone.
func TestLockIsExclusiveUntilReleased(t *testing.T) {
	dir := t.TempDir()
	release, err := Lock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Lock(dir); !errors.Is(err, ErrLocked) {
		t.Fatalf("second lock: %v", err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	again, err := Lock(dir)
	if err != nil {
		t.Fatalf("after release: %v", err)
	}
	if err := again(); err != nil {
		t.Fatal(err)
	}
}

// Catches: a backup readable by other users, which holds the wallet service keys
// (VACUUM INTO alone gave 0644 under umask 022); leftover journal files; a backup that
// overwrites an existing file; and a failed backup that leaves a file behind.
func TestBackupFile(t *testing.T) {
	old := syscall.Umask(0o022)
	t.Cleanup(func() { syscall.Umask(old) })
	db, path := newStore(t)
	if _, err := db.DB.Exec("INSERT INTO settings VALUES ('a', 'b')"); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	dest := filepath.Join(dir, "backup.db")
	if err := Backup(context.Background(), path, dest); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("folder holds %v, %v", entries, err)
	}
	info, err := os.Stat(dest)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != FileMode {
		t.Fatalf("backup has mode %o, want %o", got, FileMode)
	}

	if err := Backup(context.Background(), path, dest); err == nil {
		t.Fatal("no error for an existing file")
	}
	restored, err := Open(context.Background(), dest)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restored.Close() }()
	var value string
	if err := restored.DB.QueryRow("SELECT value FROM settings WHERE key = 'a'").Scan(&value); err != nil || value != "b" {
		t.Fatalf("existing backup changed: %q, %v", value, err)
	}

	foreign := filepath.Join(t.TempDir(), "other.db")
	other, err := sql.Open("sqlite", foreign)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Exec("CREATE TABLE t (x)"); err != nil {
		t.Fatal(err)
	}
	_ = other.Close()
	failed := filepath.Join(dir, "failed.db")
	if err := Backup(context.Background(), foreign, failed); err == nil {
		t.Fatal("backed up a database that is not nwc4ln's")
	}
	if _, err := os.Stat(failed); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed backup left a file: %v", err)
	}
}

// Catches: an upgrade from the first schema that breaks or loses the ledger: the
// database a running node already has must gain the alerts table and keep its rows.
func TestUpgradeFromFirstSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "old.db")
	first, err := migrations.ReadFile("migrations/0001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, FileMode); err != nil {
		t.Fatal(err)
	}
	dsn, err := dataSource(path)
	if err != nil {
		t.Fatal(err)
	}
	old, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrate(ctx, old, fstest.MapFS{"0001_init.sql": {Data: first}}); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Exec(`INSERT INTO settings VALUES ('marker', 'kept')`); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got, want := version(t, db.DB), migrationCount(t); got != want {
		t.Fatalf("version %d, want %d", got, want)
	}
	var marker string
	if err := db.DB.QueryRow(`SELECT value FROM settings WHERE key = 'marker'`).Scan(&marker); err != nil || marker != "kept" {
		t.Fatalf("marker %q, %v", marker, err)
	}
	if _, err := db.DB.Exec(`INSERT INTO alerts (kind, key, message, created_at) VALUES ('k', 'x', 'm', 1)`); err != nil {
		t.Fatalf("no alerts table: %v", err)
	}
}

// Catches: check writing to a database the service has open, or reading one this
// binary cannot read.
func TestOpenReadOnly(t *testing.T) {
	db, path := newStore(t)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.DB.Exec(`INSERT INTO settings VALUES ('x', 'y')`); err == nil {
		t.Fatal("a read-only store accepted a write")
	}
	writer, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.DB.Exec(`PRAGMA user_version = 1`); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReadOnly(context.Background(), path); err == nil {
		t.Fatal("opened a database with another schema version")
	}
}
