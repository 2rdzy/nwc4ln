package allowlist

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"fiatjaf.com/nostr"
)

// FileName is the saved list's name in the data folder.
const FileName = "allowlist.json"

const fileMode = 0o600

// Store keeps the newest list in memory and in a file, so that a restart keeps
// it. Reads take no lock.
type Store struct {
	path    string
	control nostr.PubKey
	mu      sync.Mutex // Serialises Offer.
	current atomic.Pointer[List]
}

// Open loads the saved list, if there is one. A saved list that does not check
// out, for example after the control key changed, stops the relay: it names the
// file, and deleting it starts the relay with no list.
func Open(dataDir string, control nostr.PubKey) (*Store, error) {
	s := &Store{path: filepath.Join(dataDir, FileName), control: control}
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read allow-list: %w", err)
	}
	list, err := Read(data, control)
	if err != nil {
		return nil, fmt.Errorf("saved allow-list %s: %w (delete the file to start with no list; "+
			"the wallet sends a new one when it connects)", s.path, err)
	}
	s.current.Store(list)
	return s, nil
}

// Read decodes and checks a saved list.
func Read(data []byte, control nostr.PubKey) (*List, error) {
	var event nostr.Event
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return Parse(event, control)
}

// Current returns the newest list, or nil before the first one arrives.
func (s *Store) Current() *List { return s.current.Load() }

// Result is the outcome of an offered list.
type Result struct {
	Accepted bool
	// Reason is the OK message for a refused list.
	Reason string
	// FromControl is true when the list was signed by the control key, so a
	// refusal points at the wallet's clock or a fault and is worth a warning.
	FromControl bool
}

// Offer checks a list and, if it is newer than the saved one and inside the time
// window, saves it and makes it current. The same list sent again is accepted
// and changes nothing. A list that is not newer is refused with the saved
// list's time in the message, so the wallet can stamp its next list later.
func (s *Store) Offer(event nostr.Event, now time.Time) Result {
	if event.PubKey != s.control {
		return Result{Reason: "restricted: the allow-list must be signed by the control key"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	refuse := func(format string, args ...any) Result {
		return Result{Reason: fmt.Sprintf(format, args...), FromControl: true}
	}
	created := time.Unix(int64(event.CreatedAt), 0)
	if created.After(now.Add(MaxFuture)) {
		return refuse("invalid: created_at %d is more than %s ahead of the relay's clock (%d)",
			event.CreatedAt, MaxFuture, now.Unix())
	}
	if created.Before(now.Add(-MaxAge)) {
		return refuse("invalid: created_at %d is more than %s behind the relay's clock (%d)",
			event.CreatedAt, MaxAge, now.Unix())
	}
	current := s.current.Load()
	if current != nil {
		if event.ID == current.Event.ID {
			return Result{Accepted: true, FromControl: true}
		}
		if event.CreatedAt <= current.Event.CreatedAt {
			return refuse("invalid: not newer than the saved allow-list, created_at %d",
				current.Event.CreatedAt)
		}
	}
	list, err := Parse(event, s.control)
	if err != nil {
		return refuse("invalid: %s", err)
	}
	if err := s.save(event); err != nil {
		return refuse("error: cannot save the allow-list: %s", err)
	}
	s.current.Store(list)
	return Result{Accepted: true, FromControl: true}
}

// save writes the list to a new file and renames it over the old one, so that a
// crash leaves either the old list or the new one, never a part.
func (s *Store) save(event nostr.Event) (err error) {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	file, err := os.CreateTemp(dir, FileName+".*")
	if err != nil {
		return err
	}
	temp := file.Name()
	defer func() {
		if err != nil {
			_ = os.Remove(temp)
		}
	}()
	if err := file.Chmod(fileMode); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temp, s.path); err != nil {
		return err
	}
	folder, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, folder.Close()) }()
	return folder.Sync()
}
