// Package web is the admin UI. Only the admin logs in, with a Nostr key, and every
// page except the login page needs that session.
package web

import (
	"context"
	"embed"
	"errors"
	"html/template"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/2rdzy/nwc4ln/internal/ledger"
)

//go:embed templates/*.html
var templateFiles embed.FS

//go:embed static
var staticFiles embed.FS

// Ledger gives the figures the UI shows. Only Overview reads the node.
type Ledger interface {
	Overview(ctx context.Context) (ledger.Overview, error)
	Apps(ctx context.Context) ([]ledger.App, error)
	AppBudget(ctx context.Context, accountID int64) (*ledger.Budget, error)
	History(ctx context.Context, query ledger.HistoryQuery) ([]ledger.Transaction, int64, error)
	SubWallets(ctx context.Context) ([]ledger.SubWallet, error)
	Accounts(ctx context.Context) ([]ledger.Account, error)
	AllHistory(ctx context.Context, query ledger.AllHistoryQuery) ([]ledger.Transaction, int64, error)
	// Fund and Withdraw are the only way the UI moves money (R15).
	Fund(ctx context.Context, accountID, amountMsat int64) (ledger.Transaction, error)
	Withdraw(ctx context.Context, accountID, amountMsat int64) (ledger.Transaction, error)
	// Alerts and CloseAlert show and clear what the ledger found wrong.
	Alerts(ctx context.Context) ([]ledger.Alert, error)
	CloseAlert(ctx context.Context, id int64) error
	// NodeSynced is the node's last answer about the chain. It reads no node.
	NodeSynced() bool
	// SpendingPaused and SetSpendingPaused read and turn the freeze switch (R14).
	SpendingPaused(ctx context.Context) (bool, error)
	SetSpendingPaused(ctx context.Context, paused bool) error
}

// Config sets up the server.
type Config struct {
	// AdminKey is the admin's public key in hex. With none, nobody can log in.
	AdminKey string
	// AllowedHosts are extra host names to answer to, besides the loopback names.
	AllowedHosts []string
	// NodeType is the node type from the config, "lnd" or "cln". The sidebar names it.
	NodeType   string
	Ledger     Ledger
	Apps       AppService
	SubWallets SubWalletService
	Log        *slog.Logger
}

// Server is the admin UI.
type Server struct {
	adminKey string
	hosts    map[string]bool
	ledger   Ledger
	apps     AppService
	wallets  SubWalletService
	log      *slog.Logger
	pages    *template.Template
	handler  http.Handler
	nodeName string // Shown in the sidebar, such as "LND".

	now func() time.Time // Changed only by tests.

	mu         sync.Mutex
	challenges map[string]time.Time // Challenge to when it was given.
	order      []string             // Challenges, oldest first.
	sessions   map[string]time.Time // Session token to when it ends.
	forms      map[string]formToken // Form token to what it allows.
	formOrder  []string             // Form tokens, oldest first.
	lastRead   time.Time            // When the dashboard last read the node. Zero: never.
	lastReadOK bool                 // Whether that read answered.
}

// New returns the admin UI.
func New(cfg Config) (*Server, error) {
	s := &Server{
		adminKey: cfg.AdminKey, ledger: cfg.Ledger, apps: cfg.Apps, wallets: cfg.SubWallets, log: cfg.Log,
		nodeName:   nodeName(cfg.NodeType),
		now:        time.Now,
		hosts:      map[string]bool{"localhost": true, "127.0.0.1": true, "::1": true},
		challenges: map[string]time.Time{}, sessions: map[string]time.Time{}, forms: map[string]formToken{},
	}
	pages, err := template.New("").Funcs(template.FuncMap{
		"sats": formatSats, "msat": formatMsat, "time": formatTime, "node": s.nodeStatus,
	}).ParseFS(templateFiles, "templates/*.html")
	if err != nil {
		return nil, err
	}
	s.pages = pages
	for _, host := range cfg.AllowedHosts {
		s.hosts[strings.ToLower(strings.Trim(host, "[]"))] = true
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /login", s.loginPage)
	mux.HandleFunc("POST /login", s.login)
	mux.HandleFunc("POST /logout", s.logout)
	mux.Handle("GET /static/", http.FileServerFS(staticFiles))
	mux.HandleFunc("GET /{$}", s.dashboard)
	mux.HandleFunc("GET /apps", s.appsPage)
	mux.HandleFunc("GET /apps/new", s.newAppPage)
	mux.HandleFunc("POST /apps", s.createApp)
	mux.HandleFunc("GET /apps/{id}", s.appPage)
	mux.HandleFunc("POST /apps/{id}", s.editApp)
	mux.HandleFunc("GET /apps/{id}/replace", s.replaceAppPage)
	mux.HandleFunc("POST /apps/{id}/replace", s.replaceApp)
	mux.HandleFunc("GET /apps/{id}/delete", s.deleteAppPage)
	mux.HandleFunc("POST /apps/{id}/delete", s.deleteApp)
	mux.HandleFunc("GET /subwallets", s.subWalletsPage)
	mux.HandleFunc("GET /subwallets/new", s.newSubWalletPage)
	mux.HandleFunc("POST /subwallets", s.createSubWallet)
	mux.HandleFunc("GET /subwallets/{id}", s.subWalletPage)
	mux.HandleFunc("POST /subwallets/{id}/fund", s.fund)
	mux.HandleFunc("POST /subwallets/{id}/withdraw", s.withdraw)
	mux.HandleFunc("GET /subwallets/{id}/replace", s.replacePage)
	mux.HandleFunc("POST /subwallets/{id}/replace", s.replace)
	mux.HandleFunc("GET /subwallets/{id}/delete", s.deleteSubWalletPage)
	mux.HandleFunc("POST /subwallets/{id}/delete", s.deleteSubWallet)
	mux.HandleFunc("GET /transactions", s.transactionsPage)
	mux.HandleFunc("POST /alerts/{id}/close", s.closeAlert)
	mux.HandleFunc("POST /subwallets/pause", s.pauseSpending)
	mux.HandleFunc("POST /subwallets/resume", s.resumeSpending)
	s.handler = s.guard(mux)
	return s, nil
}

// public lists what can be reached without a session. Everything else needs one,
// so that a new page cannot be added without the check.
func public(r *http.Request) bool {
	return r.URL.Path == "/login" || strings.HasPrefix(r.URL.Path, "/static/")
}

// ServeHTTP checks every request before it reaches a page.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// guard runs the checks that apply to every request, in this order: the host,
// against DNS rebinding; the origin of anything that is not a GET, against requests
// from other sites; then the session.
func (s *Server) guard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := w.Header()
		header.Set("Content-Security-Policy",
			"default-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
		header.Set("X-Frame-Options", "DENY")
		header.Set("X-Content-Type-Options", "nosniff")
		// Not no-referrer: under it, browsers send "Origin: null" on a form POST, and
		// the origin check would refuse every form, logout included.
		header.Set("Referrer-Policy", "same-origin")
		header.Set("Cache-Control", "no-store")
		if !s.allowedHost(r.Host) {
			http.Error(w, "unknown host", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead && !sameOrigin(r) {
			http.Error(w, "cross-site request refused", http.StatusForbidden)
			return
		}
		if !public(r) && !s.hasSession(r) {
			if r.Method == http.MethodGet || r.Method == http.MethodHead {
				http.Redirect(w, r, "/login", http.StatusSeeOther)
				return
			}
			http.Error(w, "log in first", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// allowedHost checks the Host header by name. The port is not checked, so an SSH
// tunnel on another local port works. A rebinding attack sends its own name.
func (s *Server) allowedHost(hostPort string) bool {
	host := hostPort
	if h, _, err := net.SplitHostPort(hostPort); err == nil {
		host = h
	}
	return host != "" && s.hosts[strings.ToLower(strings.Trim(host, "[]"))]
}

// sameOrigin requires an Origin header naming the host the request was sent to.
// Browsers send it on every POST, so a missing one is refused too.
func sameOrigin(r *http.Request) bool {
	origin, err := url.Parse(r.Header.Get("Origin"))
	if err != nil || (origin.Scheme != "http" && origin.Scheme != "https") {
		return false
	}
	return origin.Host == r.Host && origin.Path == "" && origin.User == nil
}

// Serve answers on the listener until the context ends, then waits for running
// requests to finish.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	server := &http.Server{
		Handler: s, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute,
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	stop, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	err := server.Shutdown(stop)
	if serveErr := <-done; !errors.Is(serveErr, http.ErrServerClosed) {
		err = errors.Join(err, serveErr)
	}
	return err
}

func (s *Server) render(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.pages.ExecuteTemplate(w, name, data); err != nil {
		s.log.Error("render page", "page", name, "error", err)
	}
}
