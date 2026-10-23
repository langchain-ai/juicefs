#!/bin/bash
set -euo pipefail

minio_dir="${RUNNER_TEMP:?RUNNER_TEMP must be set}/juicefs-ci-minio"
minio_pid=""
minio_address=127.0.0.1:9000
minio_user=testUser
minio_password=testUserPassword
mode="${1:-start}"

minio_running() {
    [[ "$(readlink "/proc/$minio_pid/exe" 2>/dev/null || true)" == "$minio_dir/minio" ]]
}

stop_minio_container() {
    local owner container_id="" image_id="" existing_id image_ids
    local container_exists=false image_exists=false
    owner=$(< "$minio_dir/docker-owner")
    [[ "$owner" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]] || return 1
    if [[ -f "$minio_dir/image-id" ]]; then
        image_id=$(< "$minio_dir/image-id")
        [[ "$image_id" =~ ^sha256:[0-9a-f]{64}$ ]] || return 1
        image_ids=$(docker image ls --all --quiet --no-trunc) || return 1
        if grep -Fxq -- "$image_id" <<< "$image_ids"; then
            existing_id=$(docker image inspect --format '{{ index .Config.Labels "io.juicefs.ci-minio.owner" }}' "$image_id") || return 1
            if [[ "$existing_id" != "$owner" ]]; then
                echo "Refusing to remove a MinIO image with different ownership" >&2
                return 1
            fi
            image_exists=true
        fi
    fi
    if [[ -f "$minio_dir/container-id" ]]; then
        container_id=$(< "$minio_dir/container-id")
        [[ "$container_id" =~ ^[0-9a-f]{64}$ && -n "$image_id" ]] || return 1
        existing_id=$(docker container ls --all --quiet --no-trunc --filter "id=$container_id") || return 1
        if [[ -n "$existing_id" ]]; then
            [[ "$existing_id" == "$container_id" ]] || return 1
            existing_id=$(docker container inspect --format '{{.Image}} {{ index .Config.Labels "io.juicefs.ci-minio.owner" }}' "$container_id") || return 1
            if [[ "$existing_id" != "$image_id $owner" ]]; then
                echo "Refusing to remove a MinIO container with different ownership" >&2
                return 1
            fi
            container_exists=true
        fi
    fi
    if [[ "$container_exists" == true ]]; then
        docker stop --time 5 "$container_id" > /dev/null || return 1
        docker rm "$container_id" > /dev/null || return 1
    fi
    if [[ "$image_exists" == true ]]; then
        docker image rm "$image_id" > /dev/null || return 1
    fi
}

stop_minio() {
    if [[ -f "$minio_dir/docker-owner" ]]; then
        stop_minio_container || return 1
        rm -rf -- "$minio_dir"
        return
    fi
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

case "$mode" in
    stop) stop_minio; exit ;;
    start) ;;
    sync)
        minio_address=0.0.0.0:9000
        minio_user=minioadmin
        minio_password=minioadmin
        ;;
    dump)
        minio_user=minioadmin
        minio_password=minioadmin
        ;;
    *) echo "Usage: $0 [start|sync|dump|stop]" >&2; exit 1 ;;
esac

[[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]] || {
    echo "The CI MinIO binary requires Linux x86_64" >&2
    exit 1
}
mkdir -m 700 "$minio_dir"

cleanup_failed_start() {
    local status=$?
    if (( status != 0 )); then
        if [[ "$mode" == dump && -f "$minio_dir/container-id" && -f "$minio_dir/docker-owner" ]]; then
            local container_id owner actual_owner
            container_id=$(< "$minio_dir/container-id")
            owner=$(< "$minio_dir/docker-owner")
            actual_owner=$(docker container inspect --format '{{ index .Config.Labels "io.juicefs.ci-minio.owner" }}' "$container_id" 2>/dev/null || true)
            if [[ -n "$owner" && "$actual_owner" == "$owner" ]]; then
                docker logs "$container_id" >&2 || true
            fi
        else
            [[ ! -f "$minio_dir/minio.log" ]] || cat "$minio_dir/minio.log" >&2
        fi
        stop_minio || return 1
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

if [[ "$mode" == sync || "$mode" == dump ]]; then
    client_version=RELEASE.2022-01-07T06-01-38Z
    curl --fail --silent --show-error --location --retry 3 --max-time 120 \
        --proto '=https' --tlsv1.2 \
        "https://github.com/minio/mc/releases/download/$client_version/mc.linux-amd64.$client_version" \
        --output "$minio_dir/mc"
    printf '%s  %s\n' \
        65efbfa31d85aaed8cee12e502b04108bd908d207a0527942c3735fa7d74c4f9 \
        "$minio_dir/mc" | sha256sum --check --status
    chmod 700 "$minio_dir/mc"
fi

if [[ "$mode" == dump ]]; then
    read -r owner < /proc/sys/kernel/random/uuid
    printf '%s\n' "$owner" > "$minio_dir/docker-owner"
    printf 'FROM scratch\nCOPY minio /minio\nENTRYPOINT ["/minio"]\n' > "$minio_dir/Dockerfile"
    printf '*\n!Dockerfile\n!minio\n' > "$minio_dir/.dockerignore"
    docker build --platform linux/amd64 --iidfile "$minio_dir/image-id" \
        --label "io.juicefs.ci-minio.owner=$owner" "$minio_dir"
    image_id=$(< "$minio_dir/image-id")
    docker run --detach --platform linux/amd64 --name "juicefs-ci-minio-$owner" \
        --cidfile "$minio_dir/container-id" --label "io.juicefs.ci-minio.owner=$owner" \
        --cap-drop ALL --security-opt no-new-privileges \
        --publish 127.0.0.1:9000:9000 \
        --env "MINIO_ROOT_USER=$minio_user" --env "MINIO_ROOT_PASSWORD=$minio_password" \
        "$image_id" --config-dir /config --certs-dir /certs server /data \
        --address 0.0.0.0:9000 --console-address 127.0.0.1:9001
    container_id=$(< "$minio_dir/container-id")
else
    MINIO_ROOT_USER="$minio_user" MINIO_ROOT_PASSWORD="$minio_password" \
        nohup "$minio_dir/minio" --config-dir "$minio_dir/config" \
        --certs-dir "$minio_dir/certs" server "$minio_dir/data" \
        --address "$minio_address" --console-address 127.0.0.1:9001 \
        > "$minio_dir/minio.log" 2>&1 &
    minio_pid=$!
    printf '%s\n' "$minio_pid" > "$minio_dir/pid"
fi
for _ in {1..30}; do
    if [[ "$mode" == dump ]]; then
        [[ "$(docker container inspect --format '{{.State.Running}}' "$container_id")" == true ]] || exit 1
    else
        kill -0 "$minio_pid" 2>/dev/null || exit 1
    fi
    if curl --fail --silent --max-time 1 http://127.0.0.1:9000/minio/health/ready > /dev/null; then
        exit 0
    fi
    sleep 1
done
echo "MinIO did not become ready" >&2
exit 1
