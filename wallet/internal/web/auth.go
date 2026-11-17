package web

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/2rdzy/nwc4ln/internal/nwc"
)

const (
	// authKind is the NIP-98 HTTP auth event kind.
	authKind = 27235
	// challengeLife is how long a login challenge can be used, once.
	challengeLife = 5 * time.Minute
	// maxChallenges caps the challenges kept, because anyone can ask for one.
	maxChallenges = 100
	// clockSkew is how far a login event's time may be from ours, as NIP-98 suggests.
	clockSkew = 60 * time.Second
	// sessionLife is how long a login lasts.
	sessionLife = 24 * time.Hour
	// sessionCookie is the session cookie's name.
	sessionCookie = "nwc4ln_session"
)

func randomToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b) // Never fails: crypto/rand panics instead.
	return hex.EncodeToString(b)
}

// newChallenge gives a challenge for one login. The oldest is dropped past the cap.
func (s *Server) newChallenge() string {
	challenge := randomToken()
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	kept := s.order[:0]
	for _, old := range s.order {
		if _, ok := s.challenges[old]; ok && now.Sub(s.challenges[old]) <= challengeLife {
			kept = append(kept, old)
		} else {
			delete(s.challenges, old)
		}
	}
	s.order = kept
	if len(s.order) >= maxChallenges {
		delete(s.challenges, s.order[0])
		s.order = s.order[1:]
	}
	s.challenges[challenge] = now
	s.order = append(s.order, challenge)
	return challenge
}

// useChallenge removes a challenge, and reports whether it was valid. Any attempt
// uses it up, so a failed login cannot be retried with the same challenge.
func (s *Server) useChallenge(challenge string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	given, ok := s.challenges[challenge]
	delete(s.challenges, challenge)
	return ok && s.now().Sub(given) <= challengeLife
}

type loginData struct {
	Challenge string
	NoAdmin   bool
}

func (s *Server) loginPage(w http.ResponseWriter, r *http.Request) {
	if s.hasSession(r) {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	data := loginData{NoAdmin: s.adminKey == ""}
	if !data.NoAdmin {
		data.Challenge = s.newChallenge()
	}
	s.render(w, "login.html", data)
}

// login checks a NIP-98 event signed by the admin over this exact URL, with a
// challenge from the login page, and starts a session.
func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	secure, err := s.checkLogin(r)
	if err != nil {
		s.log.Warn("admin login refused", "reason", err)
		http.Error(w, "login refused", http.StatusUnauthorized)
		return
	}
	token := randomToken()
	s.mu.Lock()
	now := s.now()
	for old, end := range s.sessions {
		if !now.Before(end) {
			delete(s.sessions, old)
		}
	}
	s.sessions[token] = now.Add(sessionLife)
	s.mu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: token, Path: "/", MaxAge: int(sessionLife / time.Second),
		HttpOnly: true, SameSite: http.SameSiteStrictMode,
		// Behind an HTTPS proxy, the browser must never send it over plain HTTP.
		Secure: secure,
	})
	s.log.Info("admin logged in")
	w.WriteHeader(http.StatusNoContent)
}

// checkLogin reports whether the login came over HTTPS, as the signed URL says.
// The signature binds it, so the browser's own view decides the cookie's Secure flag.
func (s *Server) checkLogin(r *http.Request) (secure bool, err error) {
	challenge := r.URL.Query().Get("challenge")
	if !s.useChallenge(challenge) {
		return false, errors.New("unknown, used or expired challenge")
	}
	if s.adminKey == "" {
		return false, errors.New("no admin key is set")
	}
	encoded, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Nostr ")
	if !ok {
		return false, errors.New("no Nostr authorization header")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		return false, fmt.Errorf("authorization header: %w", err)
	}
	event, err := nwc.VerifyEvent(raw)
	if err != nil {
		return false, err
	}
	if event.Kind != authKind {
		return false, fmt.Errorf("event kind %d", event.Kind)
	}
	if skew := s.now().Sub(event.CreatedAt); skew > clockSkew || skew < -clockSkew {
		return false, fmt.Errorf("event time is %s off", skew)
	}
	if methods := event.TagValues("method"); len(methods) != 1 || methods[0] != http.MethodPost {
		return false, fmt.Errorf("method tags %q", methods)
	}
	urls := event.TagValues("u")
	path := "://" + r.Host + "/login?challenge=" + challenge
	if len(urls) != 1 || (urls[0] != "http"+path && urls[0] != "https"+path) {
		return false, fmt.Errorf("u tags %q", urls)
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<10))
	if err != nil {
		return false, err
	}
	digest := sha256.Sum256(body)
	for _, payload := range event.TagValues("payload") {
		if payload != hex.EncodeToString(digest[:]) {
			return false, errors.New("payload tag does not match the body")
		}
	}
	if event.PubKey != s.adminKey {
		return false, fmt.Errorf("key %s is not the admin key", event.PubKey)
	}
	return strings.HasPrefix(urls[0], "https:"), nil
}

func (s *Server) hasSession(r *http.Request) bool {
	cookie, err := r.Cookie(sessionCookie)
	if err != nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	end, ok := s.sessions[cookie.Value]
	return ok && s.now().Before(end)
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(sessionCookie); err == nil {
		s.mu.Lock()
		delete(s.sessions, cookie.Value)
		s.mu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: "", Path: "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteStrictMode,
	})
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}
