#!/usr/bin/env bash
# Checks the code against Go's vulnerability database with govulncheck, which
# reports only vulnerabilities the code can reach. It fails on any that
# vulncheck-known.txt does not list, and says which listed ones are gone. The
# weekly workflow runs it; it never runs on a pull request, so it cannot block a fix.
set -euo pipefail
cd "$(dirname "$0")/.."
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
out=$work/report
# Installed, not run with go run, which would hide its exit code.
GOBIN=$work go install golang.org/x/vuln/cmd/govulncheck@v1.1.4
status=0
"$work/govulncheck" ./... >"$out" 2>&1 || status=$?
cat "$out"
if [ "$status" -ne 0 ] && [ "$status" -ne 3 ]; then
	echo "govulncheck did not run (exit $status)" >&2
	exit "$status"
fi
found=$(sed -n 's/^Vulnerability #[0-9]*: \(GO-[0-9-]*\)$/\1/p' "$out" | sort -u)
known=$(grep -o '^GO-[0-9-]*' scripts/vulncheck-known.txt | sort -u)
gone=$(comm -13 <(echo "$found") <(echo "$known") | grep . || true)
new=$(comm -23 <(echo "$found") <(echo "$known") | grep . || true)
if [ -n "$gone" ]; then
	echo "No longer found, remove from scripts/vulncheck-known.txt: $gone"
fi
if [ -n "$new" ]; then
	echo "New vulnerabilities the code can reach: $new" >&2
	exit 1
fi
echo "No new vulnerabilities."
