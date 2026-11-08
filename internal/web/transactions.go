package web

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/2rdzy/nwc4ln/internal/ledger"
)

// transactionsPerPage is how many rows one page of the Transactions page shows.
const transactionsPerPage = 50

// maxPage keeps the offset far from overflowing.
const maxPage = 1_000_000_000

// mainWalletFilter is the filter value for the main wallet side of funds and
// withdraws, which has no account.
const mainWalletFilter = "main"

type transactionRow struct {
	ledger.Transaction
	Account string
}

// transactionsPage lists rows of every account, newest first, in every state. The
// preimage is never shown. Deleted accounts keep their history and stay in the filter.
func (s *Server) transactionsPage(w http.ResponseWriter, r *http.Request) {
	accounts, err := s.ledger.Accounts(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	names := map[int64]string{0: "main wallet"}
	for _, account := range accounts {
		kind := "app"
		if account.Type == "sub_wallet" {
			kind = "sub-wallet"
		}
		if account.Deleted {
			kind += ", deleted"
		}
		names[account.ID] = account.Name + " (" + kind + ")"
	}

	filter := r.URL.Query().Get("account")
	accountID := ledger.AllAccounts
	switch filter {
	case "":
	case mainWalletFilter:
		accountID = 0
	default:
		id, err := strconv.ParseInt(filter, 10, 64)
		if _, known := names[id]; err != nil || id <= 0 || !known {
			s.problem(w, http.StatusNotFound, "There is no such account.")
			return
		}
		accountID = id
	}
	page := int64(1)
	if text := r.URL.Query().Get("page"); text != "" {
		if page, err = strconv.ParseInt(text, 10, 64); err != nil || page < 1 || page > maxPage {
			s.problem(w, http.StatusBadRequest, "The page must be a whole number from 1.")
			return
		}
	}

	rows, total, err := s.ledger.AllHistory(r.Context(), ledger.AllHistoryQuery{
		AccountID: accountID, Limit: transactionsPerPage, Offset: (page - 1) * transactionsPerPage,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	shown := make([]transactionRow, len(rows))
	for i, row := range rows {
		shown[i] = transactionRow{Transaction: row, Account: names[row.AccountID]}
	}
	link := func(page int64) string {
		query := url.Values{"page": {strconv.FormatInt(page, 10)}}
		if filter != "" {
			query.Set("account", filter)
		}
		return "/transactions?" + query.Encode()
	}
	data := map[string]any{
		"Accounts": accounts, "Filter": filter, "Rows": shown, "Total": total,
		"First": (page-1)*transactionsPerPage + 1, "Last": (page-1)*transactionsPerPage + int64(len(rows)),
	}
	if page > 1 {
		data["Newer"] = link(page - 1)
	}
	if page*transactionsPerPage < total {
		data["Older"] = link(page + 1)
	}
	s.render(w, "transactions.html", data)
}
