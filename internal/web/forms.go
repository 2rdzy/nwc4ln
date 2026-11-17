package web

import (
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// maxBody caps a request body. The largest form is a few hundred bytes.
	maxBody = 64 << 10
	// maxName caps an app or sub-wallet name, in characters, as the form's
	// maxlength does in the browser.
	maxName = 100
	// formLife is how long a fund or withdraw form can be sent, once.
	formLife = time.Hour
	// maxForms caps the form tokens kept. Each page load of a sub-wallet adds two.
	maxForms = 1_000
)

// formToken ties a form to one action on one account.
type formToken struct {
	accountID int64
	action    string
	given     time.Time
}

// newFormToken gives a token for one sending of a form that moves money. A second
// click or a resend of the same form then does nothing. It is not a CSRF token: the
// Origin check and SameSite already cover other sites. The oldest is dropped past
// the cap.
func (s *Server) newFormToken(accountID int64, action string) string {
	token := randomToken()
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	kept := s.formOrder[:0]
	for _, old := range s.formOrder {
		if form, ok := s.forms[old]; ok && now.Sub(form.given) <= formLife {
			kept = append(kept, old)
		} else {
			delete(s.forms, old)
		}
	}
	s.formOrder = kept
	if len(s.formOrder) >= maxForms {
		delete(s.forms, s.formOrder[0])
		s.formOrder = s.formOrder[1:]
	}
	s.forms[token] = formToken{accountID: accountID, action: action, given: now}
	s.formOrder = append(s.formOrder, token)
	return token
}

// useFormToken removes a token, and reports whether it was given for this action on
// this account and is still fresh. Of two requests with one token, only the first
// gets true.
func (s *Server) useFormToken(token string, accountID int64, action string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	form, ok := s.forms[token]
	delete(s.forms, token)
	return ok && form.accountID == accountID && form.action == action && s.now().Sub(form.given) <= formLife
}

// nameFromForm reads an app or sub-wallet name. A browser enforces maxlength, but
// a request can come from anything that holds the session.
func nameFromForm(r *http.Request, what string) (string, error) {
	name := strings.TrimSpace(r.PostFormValue("name"))
	if name == "" {
		return "", fmt.Errorf("The %s needs a name.", what)
	}
	if utf8.RuneCountInString(name) > maxName {
		return "", fmt.Errorf("The name can be at most %d characters.", maxName)
	}
	return name, nil
}
