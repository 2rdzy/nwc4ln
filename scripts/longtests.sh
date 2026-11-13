#!/usr/bin/env bash
# Runs the long tests (soak, crash loop, outages) on a fresh regtest network, for as
# long as the environment says, then stops the network. Examples:
#
#   scripts/longtests.sh                                   # the short CI lengths
#   NWC4LN_SOAK_FOR=6h scripts/longtests.sh                # six hours of soak
#   NWC4LN_IT_NODE=cln NWC4LN_SOAK_FOR=24h NWC4LN_CRASH_FOR=1h scripts/longtests.sh
#
# The variables are listed at the top of test/integration/nwc/long_test.go. For runs of
# hours, set NWC4LN_IT_DIR to a folder on disk: /tmp may be cleared, and it fills.
# NWC4LN_LONG_LOGS names the folder for the log and the soak's counts.
set -euo pipefail
cd "$(dirname "$0")/.."
logs=${NWC4LN_LONG_LOGS:-$(mktemp -d)}
mkdir -p "$logs"
export NWC4LN_LONG_REPORT=${NWC4LN_LONG_REPORT:-$logs/soak-counts.json}
go run ./test/integration/cmd/regtest up
trap 'go run ./test/integration/cmd/regtest down' EXIT
echo "logs in $logs"
go test -tags integration -p 1 -count=1 -timeout 0 -v -run '^(TestSoak|TestCrashLoop|TestOutages)$' \
	./test/integration/nwc/ 2>&1 | tee "$logs/long.log"
