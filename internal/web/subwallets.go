package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/2rdzy/nwc4ln/internal/ledger"
)

// SubWalletService changes sub-wallets. The running NWC service does it, so that its
// relay loop hears of each change at once.
type SubWalletService interface {
	AddSubWallet(ctx context.Context, name string) (string, error)
	DeleteSubWallet(ctx context.Context, accountID int64) error
	ReplaceConnection(ctx context.Context, accountID int64) (string, error)
}

const (
	actionFund     = "fund"
	actionWithdraw = "withdraw"
)

// subWalletView is one sub-wallet as the pages show it. The balance is shown exactly,
// in msat precision: it is money owed to its holder.
type subWalletView struct {
	ledger.SubWallet
}

// Withdrawable is the largest whole number of sats a withdraw can take. The balance
// already leaves out what pending payments hold.
func (v subWalletView) Withdrawable() int64 { return floorSats(max(v.BalanceMsat, 0)) }

// subWallet finds the sub-wallet with the ID in the path, or answers 404. Deleted
// sub-wallets and apps are not sub-wallets here.
func (s *Server) subWallet(w http.ResponseWriter, r *http.Request) (subWalletView, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err == nil {
		var wallets []ledger.SubWallet
		if wallets, err = s.ledger.SubWallets(r.Context()); err != nil {
			s.fail(w, err)
			return subWalletView{}, false
		}
		for _, wallet := range wallets {
			if wallet.ID == id {
				return subWalletView{SubWallet: wallet}, true
			}
		}
	}
	s.noSubWallet(w)
	return subWalletView{}, false
}

func (s *Server) noSubWallet(w http.ResponseWriter) {
	s.problem(w, http.StatusNotFound, "There is no such sub-wallet. It may have been deleted.")
}

func (s *Server) subWalletsPage(w http.ResponseWriter, r *http.Request) {
	wallets, err := s.ledger.SubWallets(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	views := make([]subWalletView, len(wallets))
	for i, wallet := range wallets {
		views[i] = subWalletView{SubWallet: wallet}
	}
	s.render(w, "subwallets.html", views)
}

func (s *Server) newSubWalletPage(w http.ResponseWriter, _ *http.Request) {
	s.render(w, "subwallet_new.html", nil)
}

// connectionPage is the one response that shows a sub-wallet's connection string.
type connectionPage struct {
	Title, Name, ConnectionString string
	ID                            int64
	Replaced                      bool
	QR                            any
}

// createSubWallet shows the connection string in its own response, once, as
// createApp does.
func (s *Server) createSubWallet(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.PostFormValue("name"))
	if name == "" {
		s.problem(w, http.StatusBadRequest, "The sub-wallet needs a name.")
		return
	}
	connectionString, err := s.wallets.AddSubWallet(r.Context(), name)
	if err != nil {
		s.fail(w, err)
		return
	}
	s.showConnection(w, connectionPage{Title: name + " added", Name: name, ConnectionString: connectionString})
}

func (s *Server) showConnection(w http.ResponseWriter, page connectionPage) {
	qr, err := qrSVG(page.ConnectionString)
	if err != nil {
		// The connection exists, so the string is still shown, without its picture.
		s.log.Error("draw QR code", "error", err)
	} else {
		page.QR = qr
	}
	s.render(w, "subwallet_connection.html", page)
}

func (s *Server) subWalletPage(w http.ResponseWriter, r *http.Request) {
	wallet, ok := s.subWallet(w, r)
	if !ok {
		return
	}
	rows, total, err := s.ledger.History(r.Context(), ledger.HistoryQuery{
		AccountID: wallet.ID, Unpaid: true, Limit: recentTransactions,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	s.render(w, "subwallet.html", map[string]any{
		"Wallet": wallet, "Transactions": rows, "Total": total,
		"FundToken":     s.newFormToken(wallet.ID, actionFund),
		"WithdrawToken": s.newFormToken(wallet.ID, actionWithdraw),
	})
}

func (s *Server) fund(w http.ResponseWriter, r *http.Request) {
	s.move(w, r, actionFund)
}

func (s *Server) withdraw(w http.ResponseWriter, r *http.Request) {
	s.move(w, r, actionWithdraw)
}

// move runs a fund or withdraw through the ledger, which settles it as an ordinary
// internal invoice (R15). The form token makes sure one form moves money once.
func (s *Server) move(w http.ResponseWriter, r *http.Request, action string) {
	wallet, ok := s.subWallet(w, r)
	if !ok {
		return
	}
	amountMsat, err := amountFromForm(r)
	if err != nil {
		s.problem(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.useFormToken(r.PostFormValue("token"), wallet.ID, action) {
		s.problem(w, http.StatusConflict, "This form was already sent, or it is too old, so nothing more was moved. "+
			"Open the sub-wallet again and check its transactions before trying again.")
		return
	}
	var row ledger.Transaction
	if action == actionFund {
		row, err = s.ledger.Fund(r.Context(), wallet.ID, amountMsat)
	} else {
		row, err = s.ledger.Withdraw(r.Context(), wallet.ID, amountMsat)
	}
	if err != nil {
		s.moveFailed(w, err, action)
		return
	}
	s.render(w, "subwallet_moved.html", map[string]any{"Wallet": wallet, "Action": action, "Row": row})
}

// moveFailed answers a refused fund or withdraw. The ledger promises that an error
// means nothing moved and nothing is held.
func (s *Server) moveFailed(w http.ResponseWriter, err error, action string) {
	switch {
	case errors.Is(err, ledger.ErrInsufficientBalance) && action == actionFund:
		s.problem(w, http.StatusBadRequest, "Nothing moved: the amount is more than the main wallet available. "+
			"The dashboard shows that figure.")
	case errors.Is(err, ledger.ErrInsufficientBalance):
		s.problem(w, http.StatusBadRequest, "Nothing moved: the amount is more than the sub-wallet's balance, "+
			"less what its pending payments hold.")
	case errors.Is(err, ledger.ErrUnknownAccount), errors.Is(err, ledger.ErrAccountDeleted):
		s.noSubWallet(w)
	default:
		// The ledger not running yet, the node out of reach, or a database error.
		s.log.Error("admin UI: "+action+" refused", "error", err)
		s.problem(w, http.StatusServiceUnavailable, "Nothing moved: "+action+" needs the node, and it could not "+
			"be done now. The log has the details. Try again once the node answers.")
	}
}

// amountFromForm reads a whole number of sats as an integer: no floats. The bounds are
// checked before the multiplication, so it cannot overflow.
func amountFromForm(r *http.Request) (int64, error) {
	sats, err := strconv.ParseInt(strings.TrimSpace(r.PostFormValue("amount")), 10, 64)
	if err != nil || sats < 1 || sats > ledger.MaxAmountMsat/msatPerSat {
		return 0, formError(fmt.Sprintf("The amount must be a whole number of sats from 1 to %d.",
			ledger.MaxAmountMsat/msatPerSat))
	}
	return sats * msatPerSat, nil
}

func (s *Server) replacePage(w http.ResponseWriter, r *http.Request) {
	wallet, ok := s.subWallet(w, r)
	if !ok {
		return
	}
	s.render(w, "subwallet_replace.html", wallet)
}

// replace shows the new connection string once, in its own response, as createApp
// does. The old one stops working at once. The balance and history stay.
func (s *Server) replace(w http.ResponseWriter, r *http.Request) {
	wallet, ok := s.subWallet(w, r)
	if !ok {
		return
	}
	connectionString, err := s.wallets.ReplaceConnection(r.Context(), wallet.ID)
	if err != nil {
		s.walletChangeFailed(w, err)
		return
	}
	s.showConnection(w, connectionPage{
		Title: "New connection string for " + wallet.Name, Name: wallet.Name, ID: wallet.ID,
		ConnectionString: connectionString, Replaced: true,
	})
}

// deleteSubWalletPage asks for confirmation, and shows the exact balance that will
// move to the main wallet (R16).
func (s *Server) deleteSubWalletPage(w http.ResponseWriter, r *http.Request) {
	wallet, ok := s.subWallet(w, r)
	if !ok {
		return
	}
	s.render(w, "subwallet_delete.html", wallet)
}

func (s *Server) deleteSubWallet(w http.ResponseWriter, r *http.Request) {
	wallet, ok := s.subWallet(w, r)
	if !ok {
		return
	}
	if err := s.wallets.DeleteSubWallet(r.Context(), wallet.ID); err != nil {
		s.walletChangeFailed(w, err)
		return
	}
	http.Redirect(w, r, "/subwallets", http.StatusSeeOther)
}

// walletChangeFailed answers a refused replace or delete. An unknown account means it
// was deleted in the meantime.
func (s *Server) walletChangeFailed(w http.ResponseWriter, err error) {
	if errors.Is(err, ledger.ErrUnknownAccount) {
		s.noSubWallet(w)
		return
	}
	s.fail(w, err)
}
