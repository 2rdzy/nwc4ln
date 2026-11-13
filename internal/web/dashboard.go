package web

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/2rdzy/nwc4ln/internal/ledger"
)

const msatPerSat = 1_000

type dashboardData struct {
	Known bool // False if the node could not be read.
	// Alerts are the open alerts. AlertsKnown is false if they could not be read.
	Alerts      []ledger.Alert
	AlertsKnown bool
	// Synced is the node's last answer about the chain.
	Synced bool
	// Paused is the freeze switch. PausedKnown is false if it could not be read.
	Paused, PausedKnown bool
	Overview            ledger.Overview
	ReadAt              string
	// Counted is false if the database could not list apps and sub-wallets.
	Counted     bool
	AppCount    int
	WalletCount int
}

// Figures in sats, rounded so that rounding never hides a shortfall: what the node
// has is rounded down, and what is owed is rounded up.
func (d dashboardData) NodeSpendable() int64 { return floorSats(d.Overview.NodeSpendableMsat) }
func (d dashboardData) SubWallets() int64    { return ceilSats(d.Overview.SubWalletMsat) }
func (d dashboardData) Holds() int64         { return ceilSats(d.Overview.HoldsMsat) }
func (d dashboardData) MainAvailable() int64 { return floorSats(d.Overview.MainAvailableMsat) }
func (d dashboardData) Shortfall() int64     { return ceilSats(d.Overview.ShortfallMsat) }

func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	now := s.now()
	data := dashboardData{ReadAt: now.UTC().Format(time.DateTime + " UTC")}
	overview, err := s.ledger.Overview(r.Context())
	if err != nil {
		s.log.Warn("dashboard: node figures unknown", "error", err)
	} else {
		data.Known, data.Overview = true, overview
	}
	s.noteRead(now, err == nil)
	data.Synced = s.ledger.NodeSynced()
	if paused, err := s.ledger.SpendingPaused(r.Context()); err != nil {
		s.log.Warn("dashboard: freeze switch unknown", "error", err)
	} else {
		data.Paused, data.PausedKnown = paused, true
	}
	if alerts, err := s.ledger.Alerts(r.Context()); err != nil {
		s.log.Warn("dashboard: alerts unknown", "error", err)
	} else {
		data.Alerts, data.AlertsKnown = alerts, true
	}
	apps, appsErr := s.ledger.Apps(r.Context())
	wallets, walletsErr := s.ledger.SubWallets(r.Context())
	if err := errors.Join(appsErr, walletsErr); err != nil {
		s.log.Warn("dashboard: counts unknown", "error", err)
	} else {
		data.Counted, data.AppCount, data.WalletCount = true, len(apps), len(wallets)
	}
	s.render(w, "dashboard.html", data)
}

// floorSats rounds down, toward minus infinity, so a negative figure grows.
func floorSats(msat int64) int64 {
	sats := msat / msatPerSat
	if msat%msatPerSat < 0 {
		sats--
	}
	return sats
}

// ceilSats rounds up, toward plus infinity.
func ceilSats(msat int64) int64 {
	sats := msat / msatPerSat
	if msat%msatPerSat > 0 {
		sats++
	}
	return sats
}

// formatSats writes sats with thousands separators, such as -12,345 sat.
func formatSats(sats int64) string {
	digits := strconv.FormatInt(sats, 10)
	sign := ""
	if digits[0] == '-' {
		sign, digits = "-", digits[1:]
	}
	out := make([]byte, 0, len(digits)+len(digits)/3)
	for i := range len(digits) {
		if i > 0 && (len(digits)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, digits[i])
	}
	return sign + string(out) + " sat"
}

// closeAlert hides one alert, then goes back to the dashboard.
func (s *Server) closeAlert(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err == nil {
		err = s.ledger.CloseAlert(r.Context(), id)
	} else {
		err = ledger.ErrNotFound
	}
	if errors.Is(err, ledger.ErrNotFound) {
		s.problem(w, http.StatusNotFound, "There is no such open alert.")
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) pauseSpending(w http.ResponseWriter, r *http.Request)  { s.setPaused(w, r, true) }
func (s *Server) resumeSpending(w http.ResponseWriter, r *http.Request) { s.setPaused(w, r, false) }

// setPaused turns the freeze switch, then goes back to the dashboard.
func (s *Server) setPaused(w http.ResponseWriter, r *http.Request, paused bool) {
	if err := s.ledger.SetSpendingPaused(r.Context(), paused); err != nil {
		s.fail(w, err)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}
