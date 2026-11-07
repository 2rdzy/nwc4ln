package nwc

import (
	"context"

	"github.com/2rdzy/nwc4ln/internal/ledger"
)

// notification is the payload of a kind 23197 or 23196 event.
type notification struct {
	Type         string      `json:"notification_type"`
	Notification transaction `json:"notification"`
}

// Notify takes a settled row from the ledger. It never waits: the ledger calls it
// while it follows the node, so a slow relay must not slow the money. Notifications
// are best effort, so a full queue drops this one, and a client can catch up with
// list_transactions.
func (s *Service) Notify(row ledger.Transaction) {
	select {
	case s.notifications <- row:
	default:
		s.log.Warn("notification dropped: queue is full", "hash", row.PaymentHash)
	}
}

func (s *Service) notifyLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case row := <-s.notifications:
			s.notify(ctx, row)
		}
	}
}

// notify tells the row's account about it, on each of its live connections.
func (s *Service) notify(ctx context.Context, row ledger.Transaction) {
	kind := "payment_received"
	if row.Type == ledger.TypeOutgoing {
		kind = "payment_sent"
	}
	payload := notification{Type: kind, Notification: s.present(row)}
	for _, c := range s.connectionList() {
		if c.accountID != row.AccountID {
			continue
		}
		revoked, err := s.isRevoked(ctx, c.id)
		if err != nil || revoked {
			continue
		}
		if _, err := s.ledger.Access(ctx, c.accountID); err != nil {
			continue // Expired or deleted: the connection has stopped working.
		}
		s.publishNotification(ctx, c, payload, schemeNIP44, kindNotification)
		s.publishNotification(ctx, c, payload, schemeNIP04, kindNotificationNIP04)
	}
}

// publishNotification sends once, with no retry. A relay refuses an event nobody
// listens for, and that is normal here, so it is logged quietly.
func (s *Service) publishNotification(ctx context.Context, c connection, payload notification, scheme string, kind int) {
	event, err := s.sealTo(c, payload, scheme, kind)
	if err == nil {
		err = s.publish(ctx, *event)
	}
	if err != nil {
		s.log.Debug("notification not sent", "connection", c.id, "kind", kind, "error", err)
	}
}
