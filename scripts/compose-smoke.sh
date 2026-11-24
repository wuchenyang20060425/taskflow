#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
base="http://127.0.0.1:${TASKFLOW_HTTP_PORT:-8080}"
project="taskflow-smoke-$(python3 -c 'import secrets; print(secrets.token_hex(5))')"
compose=(docker compose --project-name "$project")
output=$(mktemp)
cleanup() {
    if [[ "${1:-0}" != 0 ]]; then
        "${compose[@]}" logs --tail=100 || true
    fi
    "${compose[@]}" down --volumes || true
    rm -f -- "$output"
}
trap 'cleanup "$?"' EXIT
"${compose[@]}" up --build --wait --scale worker=2
curl --fail --silent --show-error --max-time 5 "$base/ready"
reply=$(curl --fail --silent --show-error --max-time 10 -F 'file=@examples/sample.pdf' "$base/tasks")
id=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$reply")
for ((i=0;i<30;i++)); do
    if curl --fail --silent --max-time 2 "$base/tasks/$id/result" -o "$output"; then
        cmp "$output" examples/sample.txt
        echo "Task completed and downloaded sample matched"
        exit 0
    fi
    sleep 1
done
echo "Sample did not complete within 30 attempts" >&2
exit 1
