package web

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/skip2/go-qrcode"

	"github.com/2rdzy/nwc4ln/internal/ledger"
)

// AppService changes apps. The running NWC service does it, so that its relay loop
// hears of each change at once.
type AppService interface {
	AddApp(ctx context.Context, name string, settings ledger.AppSettings) (string, error)
	EditApp(ctx context.Context, accountID int64, settings ledger.AppSettings) error
	DeleteApp(ctx context.Context, accountID int64) error
	// ReplaceConnection gives the app new keys and revokes the old ones.
	ReplaceConnection(ctx context.Context, accountID int64) (string, error)
}

// recentTransactions is how many rows the app page shows.
const recentTransactions = 50

const dateLayout = time.DateOnly

var renewals = []string{ledger.RenewNever, ledger.RenewDaily, ledger.RenewWeekly, ledger.RenewMonthly, ledger.RenewYearly}

// appView is one app as the pages show it.
type appView struct {
	ledger.App
	Budget *ledger.Budget // Nil without a limit.
}

func (a appView) HasLimit() bool { return a.LimitMsat != nil }

// Limit is rounded down, like everything the app may still spend.
func (a appView) Limit() int64 { return floorSats(*a.LimitMsat) }

// Used is rounded up: it is what the app has spent or holds.
func (a appView) Used() int64 { return ceilSats(a.UsedMsat) }

// Left is what the app may still spend in this period, rounded down. It stops at 0:
// after a limit is cut below usage, nothing more can be spent.
func (a appView) Left() int64 { return floorSats(max(*a.LimitMsat-a.UsedMsat, 0)) }

// OverLimit is true after a limit was cut below what the app had already used.
func (a appView) OverLimit() bool { return a.UsedMsat > *a.LimitMsat }

func (a appView) Expired(now time.Time) bool {
	return !a.ExpiresAt.IsZero() && !now.Before(a.ExpiresAt)
}

// LimitInput is the limit as the form field shows it: empty for no limit.
func (a appView) LimitInput() string {
	if a.LimitMsat == nil {
		return ""
	}
	return strconv.FormatInt(floorSats(*a.LimitMsat), 10)
}

// LastDay is the last day the app works, in UTC, as the form takes it.
func (a appView) LastDay() string {
	if a.ExpiresAt.IsZero() {
		return ""
	}
	return a.ExpiresAt.UTC().Add(-time.Nanosecond).Format(dateLayout)
}

// app finds the app with the ID in the path, or answers 404. Deleted apps and
// sub-wallets are not apps here.
func (s *Server) app(w http.ResponseWriter, r *http.Request) (appView, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err == nil {
		var apps []ledger.App
		if apps, err = s.ledger.Apps(r.Context()); err != nil {
			s.fail(w, err)
			return appView{}, false
		}
		for _, app := range apps {
			if app.ID == id {
				return appView{App: app}, true
			}
		}
	}
	s.problem(w, http.StatusNotFound, "There is no such app. It may have been deleted.")
	return appView{}, false
}

func (s *Server) appsPage(w http.ResponseWriter, r *http.Request) {
	apps, err := s.ledger.Apps(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	views := make([]appView, len(apps))
	for i, app := range apps {
		views[i] = appView{App: app}
	}
	s.render(w, "apps.html", map[string]any{"Apps": views, "Now": s.now()})
}

func (s *Server) newAppPage(w http.ResponseWriter, _ *http.Request) {
	s.render(w, "app_new.html", map[string]any{"Renewals": renewals, "App": appView{App: ledger.App{AppSettings: ledger.DefaultApp()}}})
}

// createApp shows the connection string in its own response, once. Only the client's
// public key is stored, so it cannot be shown again. The page is not cached
// (Cache-Control: no-store), and there is no redirect that would carry it.
func (s *Server) createApp(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.PostFormValue("name"))
	if name == "" {
		s.problem(w, http.StatusBadRequest, "The app needs a name.")
		return
	}
	settings, err := settingsFromForm(r)
	if err != nil {
		s.problem(w, http.StatusBadRequest, err.Error())
		return
	}
	connectionString, err := s.apps.AddApp(r.Context(), name, settings)
	if err != nil {
		s.changeFailed(w, err)
		return
	}
	qr, err := qrSVG(connectionString)
	if err != nil {
		// The app exists, so the string is still shown, without its picture.
		s.log.Error("draw QR code", "error", err)
	}
	s.render(w, "app_created.html", map[string]any{"Name": name, "ConnectionString": connectionString, "QR": qr})
}

func (s *Server) replaceAppPage(w http.ResponseWriter, r *http.Request) {
	app, ok := s.app(w, r)
	if !ok {
		return
	}
	s.render(w, "app_replace.html", app)
}

// replaceApp shows the new connection string once, in its own response, as createApp
// does. The old one stops working at once. Settings, usage and history stay.
func (s *Server) replaceApp(w http.ResponseWriter, r *http.Request) {
	app, ok := s.app(w, r)
	if !ok {
		return
	}
	connectionString, err := s.apps.ReplaceConnection(r.Context(), app.ID)
	if err != nil {
		s.changeFailed(w, err)
		return
	}
	qr, err := qrSVG(connectionString)
	if err != nil {
		s.log.Error("draw QR code", "error", err)
	}
	s.render(w, "app_created.html", map[string]any{
		"Name": app.Name, "ID": app.ID, "Replaced": true, "ConnectionString": connectionString, "QR": qr,
	})
}

func (s *Server) appPage(w http.ResponseWriter, r *http.Request) {
	app, ok := s.app(w, r)
	if !ok {
		return
	}
	budget, err := s.ledger.AppBudget(r.Context(), app.ID)
	if err != nil {
		s.fail(w, err)
		return
	}
	app.Budget = budget
	rows, total, err := s.ledger.History(r.Context(), ledger.HistoryQuery{
		AccountID: app.ID, Unpaid: true, Limit: recentTransactions,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	s.render(w, "app.html", map[string]any{
		"App": app, "Now": s.now(), "Renewals": renewals, "Transactions": rows, "Total": total,
	})
}

func (s *Server) editApp(w http.ResponseWriter, r *http.Request) {
	app, ok := s.app(w, r)
	if !ok {
		return
	}
	settings, err := settingsFromForm(r)
	if err != nil {
		s.problem(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.apps.EditApp(r.Context(), app.ID, settings); err != nil {
		s.changeFailed(w, err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/apps/%d", app.ID), http.StatusSeeOther)
}

func (s *Server) deleteAppPage(w http.ResponseWriter, r *http.Request) {
	app, ok := s.app(w, r)
	if !ok {
		return
	}
	s.render(w, "app_delete.html", app)
}

func (s *Server) deleteApp(w http.ResponseWriter, r *http.Request) {
	app, ok := s.app(w, r)
	if !ok {
		return
	}
	if err := s.apps.DeleteApp(r.Context(), app.ID); err != nil {
		s.changeFailed(w, err)
		return
	}
	http.Redirect(w, r, "/apps", http.StatusSeeOther)
}

// formError is a message for the admin about a form field.
type formError string

func (e formError) Error() string { return string(e) }

// settingsFromForm reads the settings fields. Amounts are whole sats, read as
// integers: no floats. The bounds are checked before the multiplication, so it
// cannot overflow.
func settingsFromForm(r *http.Request) (ledger.AppSettings, error) {
	settings := ledger.AppSettings{
		Renewal:    r.PostFormValue("renewal"),
		CanPay:     r.PostFormValue("can_pay") == "on",
		CanInvoice: r.PostFormValue("can_invoice") == "on",
	}
	if limit := strings.TrimSpace(r.PostFormValue("limit")); limit != "" {
		sats, err := strconv.ParseInt(limit, 10, 64)
		if err != nil || sats < 0 || sats > ledger.MaxAmountMsat/msatPerSat {
			return ledger.AppSettings{}, formError("The limit must be a whole number of sats from 0 to " +
				strconv.FormatInt(ledger.MaxAmountMsat/msatPerSat, 10) + ", or empty for no limit.")
		}
		msat := sats * msatPerSat
		settings.LimitMsat = &msat
	}
	if day := strings.TrimSpace(r.PostFormValue("expires")); day != "" {
		date, err := time.ParseInLocation(dateLayout, day, time.UTC)
		if err != nil {
			return ledger.AppSettings{}, formError("The expiry must be a date such as 2026-12-31, or empty.")
		}
		// The app works until the end of that day in UTC.
		settings.ExpiresAt = date.AddDate(0, 0, 1)
	}
	if err := settings.Check(); err != nil {
		return ledger.AppSettings{}, formError("The renewal must be one of the listed periods.")
	}
	return settings, nil
}

// changeFailed answers a refused change. Bad settings were checked already, so an
// unknown account means it was deleted in the meantime.
func (s *Server) changeFailed(w http.ResponseWriter, err error) {
	if errors.Is(err, ledger.ErrUnknownAccount) {
		s.problem(w, http.StatusNotFound, "There is no such app. It may have been deleted.")
		return
	}
	s.fail(w, err)
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	s.log.Error("admin UI request failed", "error", err)
	s.problem(w, http.StatusInternalServerError, "Something went wrong. The log has the details.")
}

func (s *Server) problem(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := s.pages.ExecuteTemplate(w, "problem.html", message); err != nil {
		s.log.Error("render page", "page", "problem.html", "error", err)
	}
}

// qrSVG draws a QR code as inline SVG. The page's CSP allows no data: images, and a
// separate image URL would have to carry or keep the secret. The SVG holds only
// numbers, so it is safe to insert as HTML.
func qrSVG(text string) (template.HTML, error) {
	code, err := qrcode.New(text, qrcode.Medium)
	if err != nil {
		return "", err
	}
	bitmap := code.Bitmap() // Includes the quiet zone around the code.
	var path strings.Builder
	for y, row := range bitmap {
		// One rectangle for each run of dark modules in a row.
		for x := 0; x < len(row); x++ {
			if !row[x] {
				continue
			}
			start := x
			for x+1 < len(row) && row[x+1] {
				x++
			}
			fmt.Fprintf(&path, "M%d %dh%dv1h-%dz", start, y, x-start+1, x-start+1)
		}
	}
	size := len(bitmap)
	return template.HTML(fmt.Sprintf(
		`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="320" height="320" shape-rendering="crispEdges" role="img" aria-label="QR code of the connection string">`+
			`<rect width="%d" height="%d" fill="#fff"/><path fill="#000" d="%s"/></svg>`,
		size, size, size, size, path.String())), nil
}

// formatMsat writes an exact amount in sats, with up to three decimals for the
// msat part, using only integers.
func formatMsat(msat int64) string {
	sign := ""
	if msat < 0 {
		sign, msat = "-", -msat
	}
	whole := strings.TrimSuffix(formatSats(msat/msatPerSat), " sat")
	if rest := msat % msatPerSat; rest != 0 {
		whole += strings.TrimRight(fmt.Sprintf(".%03d", rest), "0")
	}
	return sign + whole + " sat"
}

// formatTime writes a time in UTC, or a dash for none.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	return t.UTC().Format("2006-01-02 15:04 UTC")
}
