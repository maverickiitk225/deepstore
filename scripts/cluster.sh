#!/usr/bin/env bash
# Start N servers and leave them running until Ctrl-C. Default is 3.
set -euo pipefail

count="${1:-3}"
if [[ ! "$count" =~ ^[1-9][0-9]*$ ]]; then
	echo "usage: $0 [servers]" >&2
	exit 2
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bin="$root/deepstore"
data="$root/.cluster"
logs="$data/logs"
leader="http://127.0.0.1:7000"

cd "$root"
go build -o "$bin" ./cmd/deepstore
mkdir -p "$logs"

pids=()
cleanup() {
	local pid
	for pid in "${pids[@]+"${pids[@]}"}"; do
		if kill -0 "$pid" 2>/dev/null; then
			kill "$pid" 2>/dev/null || true
		fi
	done
	wait || true
}
trap cleanup EXIT INT TERM

start_node() {
	local id="$1" listen="$2" raft="$3" dir="$4" join="${5:-}"
	local -a args=(serve -data-dir "$dir" -listen "$listen" -raft-addr "$raft" -node-id "$id")
	if [[ -n "$join" ]]; then
		args+=(-join "$join")
	fi
	mkdir -p "$dir"
	"$bin" "${args[@]}" >"$logs/$id.log" 2>&1 &
	pids+=("$!")
	last_pid=$!
}

wait_listening() {
	local id="$1" pid="$2"
	local log="$logs/$id.log"
	local i
	for ((i = 0; i < 300; i++)); do
		if [[ -f "$log" ]] && grep -q "listening on" "$log"; then
			return 0
		fi
		if ! kill -0 "$pid" 2>/dev/null; then
			echo "$id exited before it was listening" >&2
			cat "$log" >&2 || true
			exit 1
		fi
		sleep 0.1
	done
	echo "timed out waiting for $id" >&2
	cat "$log" >&2 || true
	exit 1
}

for ((n = 1; n <= count; n++)); do
	http=$((7000 + (n - 1) * 2))
	raft=$((http + 1))
	if ((n == 1)); then
		start_node "node$n" "127.0.0.1:$http" "127.0.0.1:$raft" "$data/node$n"
		wait_listening "node$n" "$last_pid"
	else
		start_node "node$n" "127.0.0.1:$http" "127.0.0.1:$raft" "$data/node$n" "$leader"
	fi
done

for ((n = 2; n <= count; n++)); do
	wait_listening "node$n" "${pids[$((n - 1))]}"
done

echo "cluster is up ($count servers)"
for ((n = 1; n <= count; n++)); do
	echo "  node$n  http://127.0.0.1:$((7000 + (n - 1) * 2))"
done
echo
"$bin" put -addr "$leader" name ada
"$bin" get -addr "$leader" name
echo
echo "logs are in $logs"
echo "ctrl-c stops the cluster"
wait
