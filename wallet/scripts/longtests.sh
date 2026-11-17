#!/usr/bin/env bash
# Runs the long tests (soak, crash loop, outages) on a fresh regtest network for each
# node in NWC4LN_LONG_NODES, for as long as the environment says. Each node gets its
# own network and logs, and a failure on one does not stop the next. Examples:
#
#   scripts/longtests.sh                                       # LND, short CI lengths
#   NWC4LN_SOAK_FOR=6h NWC4LN_LONG_NODES="lnd cln" scripts/longtests.sh
#
# The variables are listed at the top of test/integration/nwc/long_test.go. For runs of
# hours, set NWC4LN_IT_DIR to a folder on disk: /tmp may be cleared, and it fills.
# NWC4LN_LONG_LOGS names the folder for the logs, with one folder per node inside.
set -uo pipefail
cd "$(dirname "$0")/.."
if ! command -v go >/dev/null; then
	echo "go is not on PATH. Add the folder that holds it, for example:" >&2
	echo '  export PATH=$HOME/.local/go-1.26.2/bin:$PATH' >&2
	exit 2
fi
logs=${NWC4LN_LONG_LOGS:-$(mktemp -d)}
nodes=${NWC4LN_LONG_NODES:-${NWC4LN_IT_NODE:-lnd}}
failed=()
for node in $nodes; do
	mkdir -p "$logs/$node"
	echo "== $node: logs in $logs/$node"
	if ! go run ./test/integration/cmd/regtest up; then
		failed+=("$node (network did not start)")
		continue
	fi
	NWC4LN_IT_NODE=$node NWC4LN_LONG_REPORT=$logs/$node/soak-counts.json \
		go test -tags integration -p 1 -count=1 -timeout 0 -v -run '^(TestSoak|TestCrashLoop|TestOutages)$' \
		./test/integration/nwc/ 2>&1 | tee "$logs/$node/long.log"
	[ "${PIPESTATUS[0]}" -eq 0 ] || failed+=("$node")
	cp "${NWC4LN_IT_DIR:-${TMPDIR:-/tmp}/nwc4ln/it}"/*.log "$logs/$node/" 2>/dev/null || true
	go run ./test/integration/cmd/regtest down
done
if [ ${#failed[@]} -gt 0 ]; then
	echo "failed: ${failed[*]}" >&2
	exit 1
fi
echo "passed: $nodes"
