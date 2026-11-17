package web

import "time"

// nodeName gives the name the sidebar shows for a node type.
func nodeName(nodeType string) string {
	switch nodeType {
	case "lnd":
		return "LND"
	case "cln":
		return "Core Lightning"
	case "":
		return "Node"
	}
	return nodeType
}

// nodeView is the node line at the foot of the sidebar.
type nodeView struct {
	State string // "reachable", "unreachable", or empty before the first read.
	Text  string
}

// noteRead keeps the result of the dashboard's node read for the sidebar.
func (s *Server) noteRead(at time.Time, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRead, s.lastReadOK = at, ok
}

// nodeStatus describes the last node read. It never reads the node itself: that
// read holds the payment lock, so only the dashboard makes it (spec section 8).
func (s *Server) nodeStatus() nodeView {
	s.mu.Lock()
	at, ok := s.lastRead, s.lastReadOK
	s.mu.Unlock()
	if at.IsZero() {
		return nodeView{Text: s.nodeName + " · not read yet"}
	}
	state := "unreachable"
	if ok {
		state = "reachable"
	}
	return nodeView{State: state, Text: s.nodeName + " · " + state + " at " + at.UTC().Format("15:04") + " UTC"}
}
