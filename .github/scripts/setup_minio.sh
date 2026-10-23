#!/bin/bash
set -euo pipefail

minio_dir="${RUNNER_TEMP:?RUNNER_TEMP must be set}/juicefs-ci-minio"
minio_pid=""

minio_running() {
    [[ "$(readlink "/proc/$minio_pid/exe" 2>/dev/null || true)" == "$minio_dir/minio" ]]
}

stop_minio() {
    if [[ -z "$minio_pid" ]]; then
        [[ -f "$minio_dir/pid" ]] || return 0
        read -r minio_pid < "$minio_dir/pid"
    fi
    [[ "$minio_pid" =~ ^[0-9]+$ ]] || return 1
    for signal in TERM KILL; do
        minio_running || break
        kill -"$signal" "$minio_pid" 2>/dev/null || true
        for _ in {1..50}; do
            minio_running || break
            sleep 0.1
        done
    done
    if minio_running; then
        echo "MinIO did not stop" >&2
        return 1
    fi
    rm -rf -- "$minio_dir"
}

case "${1:-start}" in
    stop) stop_minio; exit ;;
    start) ;;
    *) echo "Usage: $0 [start|stop]" >&2; exit 1 ;;
esac

[[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]] || {
    echo "The CI MinIO binary requires Linux x86_64" >&2
    exit 1
}
mkdir -m 700 "$minio_dir"

cleanup_failed_start() {
    local status=$?
    if (( status != 0 )); then
        [[ ! -f "$minio_dir/minio.log" ]] || cat "$minio_dir/minio.log" >&2
        stop_minio
        rm -rf -- "$minio_dir"
    fi
}
trap cleanup_failed_start EXIT

version=RELEASE.2022-01-25T19-56-04Z
curl --fail --silent --show-error --location --retry 3 --max-time 120 \
    --proto '=https' --tlsv1.2 \
    "https://github.com/minio/minio/releases/download/$version/minio.linux-amd64.$version" \
    --output "$minio_dir/minio"
printf '%s  %s\n' \
    19502eea616dc4dfc40425f62f2a36bda92e612f39cf78b123d9144842c553f2 \
    "$minio_dir/minio" | sha256sum --check --status
chmod 700 "$minio_dir/minio"

MINIO_ROOT_USER=testUser MINIO_ROOT_PASSWORD=testUserPassword \
    nohup "$minio_dir/minio" --config-dir "$minio_dir/config" \
    --certs-dir "$minio_dir/certs" server "$minio_dir/data" \
    --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 \
    > "$minio_dir/minio.log" 2>&1 &
minio_pid=$!
printf '%s\n' "$minio_pid" > "$minio_dir/pid"
for _ in {1..30}; do
    kill -0 "$minio_pid" 2>/dev/null || exit 1
    if curl --fail --silent --max-time 1 http://127.0.0.1:9000/minio/health/ready > /dev/null; then
        exit 0
    fi
    sleep 1
done
echo "MinIO did not become ready" >&2
exit 1
