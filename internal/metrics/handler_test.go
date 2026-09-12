package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"raftmage/internal/raft"
)

func TestHandlerWritesCurrentCounterValues(t *testing.T) {
	node := raft.NewNode("node-1", nil, nil, nil)
	node.StartElection()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	Handler(node).ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "raftmage_elections_started_total 1") {
		t.Fatalf("body does not contain the expected elections_started line, got:\n%s", body)
	}
	if !strings.Contains(body, "raftmage_elections_won_total 1") {
		t.Fatalf("body does not contain the expected elections_won line, got:\n%s", body)
	}
	if !strings.Contains(body, "raftmage_votes_granted_total 0") {
		t.Fatalf("body does not contain a zero-valued counter line for an untouched metric, got:\n%s", body)
	}
}

func TestHandlerSetsPlainTextContentType(t *testing.T) {
	node := raft.NewNode("node-1", nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	Handler(node).ServeHTTP(rec, req)

	ct := rec.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type = %q, want a text/plain prefix", ct)
	}
}

func TestHandlerIncludesHelpAndTypeCommentsForEveryCounter(t *testing.T) {
	node := raft.NewNode("node-1", nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	Handler(node).ServeHTTP(rec, req)

	body := rec.Body.String()
	names := []string{
		"raftmage_elections_started_total",
		"raftmage_elections_won_total",
		"raftmage_votes_granted_total",
		"raftmage_votes_denied_total",
		"raftmage_entries_proposed_total",
		"raftmage_entries_committed_total",
		"raftmage_log_compactions_total",
		"raftmage_snapshots_installed_total",
		"raftmage_membership_changes_total",
	}
	for _, name := range names {
		if !strings.Contains(body, "# HELP "+name) || !strings.Contains(body, "# TYPE "+name+" counter") {
			t.Fatalf("body missing HELP/TYPE comments for %s, got:\n%s", name, body)
		}
	}
}
