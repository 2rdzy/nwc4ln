package web

import (
	"net/http"
	"strconv"
	"time"

	"github.com/2rdzy/nwc4ln/internal/ledger"
)

const msatPerSat = 1_000

type dashboardData struct {
	Known    bool // False if the node could not be read.
	Overview ledger.Overview
	ReadAt   string
}

// Figures in sats, rounded so that rounding never hides a shortfall: what the node
// has is rounded down, and what is owed is rounded up.
func (d dashboardData) NodeSpendable() int64 { return floorSats(d.Overview.NodeSpendableMsat) }
func (d dashboardData) SubWallets() int64    { return ceilSats(d.Overview.SubWalletMsat) }
func (d dashboardData) Holds() int64         { return ceilSats(d.Overview.HoldsMsat) }
func (d dashboardData) MainAvailable() int64 { return floorSats(d.Overview.MainAvailableMsat) }
func (d dashboardData) Shortfall() int64     { return ceilSats(d.Overview.ShortfallMsat) }

func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	data := dashboardData{ReadAt: s.now().UTC().Format(time.DateTime + " UTC")}
	overview, err := s.ledger.Overview(r.Context())
	if err != nil {
		s.log.Warn("dashboard: node figures unknown", "error", err)
	} else {
		data.Known, data.Overview = true, overview
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
