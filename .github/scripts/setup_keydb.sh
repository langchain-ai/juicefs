#!/bin/bash
set -euo pipefail

keydb_dir="${RUNNER_TEMP:?RUNNER_TEMP must be set}/juicefs-ci-keydb"
image=eqalpha/keydb:x86_64_v6.3.4@sha256:eceb1806730c7850395b8262300182c2e15a6e5dacbf0b72cbab110518caf43f

stop_keydb() {
    [[ -d "$keydb_dir" ]] || return 0
    local owner image_id="" port container_id existing_id
    local -a containers=()
    owner=$(< "$keydb_dir/owner")
    [[ "$owner" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]] || return 1
    if [[ -f "$keydb_dir/image-id" ]]; then
        image_id=$(< "$keydb_dir/image-id")
        [[ "$image_id" =~ ^sha256:[0-9a-f]{64}$ ]] || return 1
    fi
    for port in 6377 6378; do
        [[ -f "$keydb_dir/$port.cid" ]] || continue
        container_id=$(< "$keydb_dir/$port.cid")
        [[ "$container_id" =~ ^[0-9a-f]{64}$ && -n "$image_id" ]] || return 1
        existing_id=$(timeout 10 docker container ls --all --quiet --no-trunc --filter "id=$container_id") || return 1
        [[ -n "$existing_id" ]] || continue
        [[ "$existing_id" == "$container_id" ]] || return 1
        existing_id=$(timeout 10 docker container inspect --format '{{.Image}} {{ index .Config.Labels "io.juicefs.ci-keydb.owner" }}' "$container_id") || return 1
        if [[ "$existing_id" != "$image_id $owner" ]]; then
            echo "Refusing to remove a KeyDB container with different ownership" >&2
            return 1
        fi
        containers+=("$container_id")
    done
    for container_id in "${containers[@]}"; do
        if [[ "${1:-false}" == true ]]; then
            timeout 5 docker logs --tail 50 "$container_id" >&2 || true
        fi
        timeout 15 docker stop --time 5 "$container_id" > /dev/null || return 1
        timeout 15 docker rm --volumes "$container_id" > /dev/null || return 1
    done
    rm -rf -- "$keydb_dir"
}

case "${1:-start}" in
    stop) stop_keydb; exit ;;
    start) ;;
    *) echo "Usage: $0 [start|stop]" >&2; exit 1 ;;
esac

mkdir -m 700 "$keydb_dir"
read -r owner < /proc/sys/kernel/random/uuid
printf '%s\n' "$owner" > "$keydb_dir/owner"

cleanup_failed_start() {
    local status=$?
    if (( status != 0 )); then
        stop_keydb true
    fi
}
trap cleanup_failed_start EXIT

timeout 180 docker pull --platform linux/amd64 "$image"
timeout 10 docker image inspect --format '{{.Id}}' "$image" > "$keydb_dir/image-id"
for port in 6377 6378; do
    provider=none
    storage_args=()
    if [[ "$port" == 6378 ]]; then
        provider=flash
        storage_args=(--storage-provider flash /flash/)
    fi
    timeout 30 docker run --detach --platform linux/amd64 --pull never \
        --name "juicefs-ci-keydb-$owner-$port" --cidfile "$keydb_dir/$port.cid" \
        --label "io.juicefs.ci-keydb.owner=$owner" --user keydb --workdir /flash \
        --cap-drop ALL --security-opt no-new-privileges \
        --publish "127.0.0.1:$port:6379" \
        "$image" keydb-server --port 6379 --bind 0.0.0.0 --protected-mode no \
        --daemonize no --dir /flash "${storage_args[@]}"
    container_id=$(< "$keydb_dir/$port.cid")
    ready=false
    for _ in {1..30}; do
        if [[ "$(timeout 2 docker exec "$container_id" keydb-cli --raw ping 2>/dev/null || true)" == PONG ]]; then
            info=$(timeout 3 docker exec "$container_id" keydb-cli --raw info)
            if grep -Fxq "storage_provider:$provider" <<< "${info//$'\r'/}"; then
                ready=true
                break
            fi
        fi
        sleep 1
    done
    if [[ "$ready" != true ]]; then
        echo "KeyDB $provider fixture did not become ready on port $port" >&2
        exit 1
    fi
done
