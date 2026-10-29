#!/usr/bin/env bash
# Local Redis and S3-compatible object store for run.sh, each behind delayproxy
# so the client sees production-like latency instead of loopback speed.
#
#   backend.sh up     start everything and write $STATE_DIR/env for run.sh
#   backend.sh down   stop it and delete its data
#
# OBJECT_STORE=rustfs|minio picks the store. S3_PROFILE (none, s3-standard,
# s3-express) and S3_PROXY_ARGS shape object latency; REDIS_DELAY is the one-way
# delay added to metadata traffic. Calibrate both against production; see
# README.md.
set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="${STATE_DIR:-/var/tmp/jfs-pgbench-backend}"
OBJECT_STORE="${OBJECT_STORE:-rustfs}"
RUSTFS_IMAGE="${RUSTFS_IMAGE:-rustfs/rustfs:1.0.0-beta.7}"
MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:RELEASE.2025-04-22T22-12-26Z}"
REDIS_IMAGE="${REDIS_IMAGE:-redis:7.0}"
STORE_PORT="${STORE_PORT:-39000}"
REDIS_PORT="${REDIS_PORT:-39379}"
S3_PROXY_PORT="${S3_PROXY_PORT:-39100}"
REDIS_PROXY_PORT="${REDIS_PROXY_PORT:-39380}"
S3_PROFILE="${S3_PROFILE:-s3-standard}"
S3_PROXY_ARGS="${S3_PROXY_ARGS:-}"
REDIS_DELAY="${REDIS_DELAY:-250us}"
ACCESS_KEY="${ACCESS_KEY:-jfsbench}"
SECRET_KEY="${SECRET_KEY:-jfsbench-secret}"
BUCKET_NAME="${BUCKET_NAME:-pgbench}"
STORE_CTR=jfs-pgbench-store
REDIS_CTR=jfs-pgbench-redis

log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

stop_proxy() {
	local f="$STATE_DIR/$1.pid"
	if [[ -f "$f" ]]; then
		kill "$(cat "$f")" 2>/dev/null || true
		rm -f "$f"
	fi
}

start_proxy() { # name args...
	local name="$1"
	shift
	stop_proxy "$name"
	"$STATE_DIR/delayproxy" "$@" >"$STATE_DIR/$name.log" 2>&1 &
	echo $! >"$STATE_DIR/$name.pid"
	sleep 0.5
	kill -0 "$(cat "$STATE_DIR/$name.pid")" 2>/dev/null || die "$name exited: $(tail -1 "$STATE_DIR/$name.log")"
}

wait_for() { # description command...
	local what="$1"
	shift
	for _ in $(seq 60); do
		"$@" >/dev/null 2>&1 && return 0
		sleep 1
	done
	die "$what not ready"
}

cmd_up() {
	command -v docker >/dev/null || die "docker is required"
	command -v go >/dev/null || die "go is required to build delayproxy"
	mkdir -p "$STATE_DIR/store-data"
	(cd "$HERE/delayproxy" && go build -o "$STATE_DIR/delayproxy" .)

	docker rm -f "$STORE_CTR" "$REDIS_CTR" >/dev/null 2>&1 || true
	case "$OBJECT_STORE" in
	rustfs)
		docker run -d --name "$STORE_CTR" --user 0:0 -p "127.0.0.1:$STORE_PORT:9000" \
			-e "RUSTFS_ACCESS_KEY=$ACCESS_KEY" -e "RUSTFS_SECRET_KEY=$SECRET_KEY" \
			-v "$STATE_DIR/store-data:/data" "$RUSTFS_IMAGE" >/dev/null
		wait_for RustFS curl -sf "http://127.0.0.1:$STORE_PORT/health/ready"
		;;
	minio)
		docker run -d --name "$STORE_CTR" -p "127.0.0.1:$STORE_PORT:9000" \
			-e "MINIO_ROOT_USER=$ACCESS_KEY" -e "MINIO_ROOT_PASSWORD=$SECRET_KEY" \
			-v "$STATE_DIR/store-data:/data" "$MINIO_IMAGE" server /data >/dev/null
		wait_for MinIO curl -sf "http://127.0.0.1:$STORE_PORT/minio/health/ready"
		;;
	*) die "OBJECT_STORE must be rustfs or minio" ;;
	esac
	docker run -d --name "$REDIS_CTR" -p "127.0.0.1:$REDIS_PORT:6379" "$REDIS_IMAGE" \
		redis-server --appendonly no --save "" --maxmemory-policy noeviction >/dev/null
	wait_for Redis docker exec "$REDIS_CTR" redis-cli ping

	# shellcheck disable=SC2086 # S3_PROXY_ARGS is a flag list
	start_proxy s3proxy -mode http -listen "127.0.0.1:$S3_PROXY_PORT" \
		-upstream "http://127.0.0.1:$STORE_PORT" -profile "$S3_PROFILE" -stats-interval 60s $S3_PROXY_ARGS
	start_proxy redisproxy -mode tcp -listen "127.0.0.1:$REDIS_PROXY_PORT" \
		-upstream "127.0.0.1:$REDIS_PORT" -delay "$REDIS_DELAY"
	wait_for "S3 proxy" curl -s -o /dev/null "http://127.0.0.1:$S3_PROXY_PORT/"
	wait_for "Redis proxy" bash -c "exec 3<>/dev/tcp/127.0.0.1/$REDIS_PROXY_PORT"

	cat >"$STATE_DIR/env" <<EOF
export META_URL=redis://127.0.0.1:$REDIS_PROXY_PORT/1
export STORAGE=s3
export BUCKET=http://127.0.0.1:$S3_PROXY_PORT/$BUCKET_NAME
export ACCESS_KEY=$ACCESS_KEY
export SECRET_KEY=$SECRET_KEY
EOF
	log "$OBJECT_STORE behind delayproxy ($S3_PROFILE) and Redis behind ${REDIS_DELAY} one-way delay are up"
	log "source $STATE_DIR/env before run.sh; proxy logs are in $STATE_DIR"
}

cmd_down() {
	stop_proxy s3proxy
	stop_proxy redisproxy
	# The store writes its data as root; remove it from inside the container.
	docker exec "$STORE_CTR" sh -c 'rm -rf /data/* /data/.[!.]*' >/dev/null 2>&1 || true
	docker rm -f "$STORE_CTR" "$REDIS_CTR" >/dev/null 2>&1 || true
	rm -rf "${STATE_DIR:?}/store-data" "$STATE_DIR/env" ||
		die "could not delete $STATE_DIR/store-data; remove it before the next up"
	log "backend stopped"
}

case "${1:-}" in
up) cmd_up ;;
down) cmd_down ;;
*) die "usage: backend.sh up|down" ;;
esac
