package metrics

import (
	"fmt"
	"net/http"

	"raftmage/internal/raft"
)

func Handler(node *raft.Node) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m := node.Metrics()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		writeCounter(w, "raftmage_elections_started_total", "Elections started", m.ElectionsStarted)
		writeCounter(w, "raftmage_elections_won_total", "Elections won", m.ElectionsWon)
		writeCounter(w, "raftmage_votes_granted_total", "Votes granted", m.VotesGranted)
		writeCounter(w, "raftmage_votes_denied_total", "Votes denied", m.VotesDenied)
		writeCounter(w, "raftmage_entries_proposed_total", "Entries proposed by this node while leader", m.EntriesProposed)
		writeCounter(w, "raftmage_entries_committed_total", "Entries committed", m.EntriesCommitted)
		writeCounter(w, "raftmage_log_compactions_total", "Log compactions performed", m.LogCompactions)
		writeCounter(w, "raftmage_snapshots_installed_total", "InstallSnapshot RPCs applied", m.SnapshotsInstalled)
		writeCounter(w, "raftmage_membership_changes_total", "Cluster membership changes committed", m.MembershipChanges)
	})
}

func writeCounter(w http.ResponseWriter, name, help string, value uint64) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, value)
}
