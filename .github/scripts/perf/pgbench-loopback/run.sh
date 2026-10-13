#!/usr/bin/env bash
# pgbench on an ext4 image that lives on JuiceFS and is attached with losetup:
# the I/O path of a VM disk image (random small writes into one large file, an
# fsync of that file on every guest flush, per-instance copies made with
# `juicefs clone`), without a VM. Compares JuiceFS variants (binaries, env,
# mount options) in interleaved repetitions. See README.md.
#
#   run.sh run              build the base image if needed, then run every rep
#   run.sh summarize DIR    print per-variant medians of DIR/results.tsv
#   run.sh cleanup          tear down mounts left by an interrupted run
set -euo pipefail

META_URL="${META_URL:-}"
STORAGE="${STORAGE:-s3}"
BUCKET="${BUCKET:-}"
ACCESS_KEY="${ACCESS_KEY:-}"
SECRET_KEY="${SECRET_KEY:-}"
VOLUME_NAME="${VOLUME_NAME:-pgbench}"

# name=/path/to/juicefs, space separated. Per variant, optionally:
#   VARIANT_<name>_ENV="K=V ..."         extra environment for the mount
#   VARIANT_<name>_MOUNT_OPTS="--opt ..." extra mount options
VARIANTS="${VARIANTS:-default=$(command -v juicefs || echo /usr/local/bin/juicefs)}"
# sandbox-host's production options, minus the cache size, plus no background
# jobs so trash cleanup and metadata backups don't land in a measurement.
MOUNT_OPTS="${MOUNT_OPTS:---buffer-size=4096 --prefetch=6 --max-uploads=50 --cache-size=20480 --no-usage-report --no-bgjob}"

WORK_DIR="${WORK_DIR:-/var/tmp/jfs-pgbench}"
OUT_DIR="${OUT_DIR:-$WORK_DIR/results/$(date +%Y%m%d-%H%M%S)}"
REPS="${REPS:-5}"
COLD="${COLD:-1}"
KEEP_RUNS="${KEEP_RUNS:-0}"
# name=value xattrs set on each run's image, e.g. user.juicefs.profile=vm-disk.
# Setting them through FUSE needs --enable-xattr in the mount options.
IMAGE_XATTRS="${IMAGE_XATTRS:-}"

IMAGE_SIZE="${IMAGE_SIZE:-32G}"
PG_SCALE="${PG_SCALE:-1000}"
PG_MODE="${PG_MODE:-tpcb}" # tpcb or select
PG_CLIENTS="${PG_CLIENTS:-16}"
PG_JOBS="${PG_JOBS:-4}"
PG_DURATION="${PG_DURATION:-600}"
PG_SAMPLING_RATE="${PG_SAMPLING_RATE:-1}"
PG_SHARED_BUFFERS="${PG_SHARED_BUFFERS:-128MB}"
PG_EXTRA_CONF="${PG_EXTRA_CONF:-}"
PG_USER="${PG_USER:-postgres}"
PG_PORT="${PG_PORT:-55432}"
PG_MEMORY_MAX="${PG_MEMORY_MAX:-}" # e.g. 4G: cgroup v2 limit standing in for guest RAM
PG_BIN="${PG_BIN:-}"

MNT="$WORK_DIR/jfs"
PGMNT="$WORK_DIR/pgdisk"
SOCK_DIR="$WORK_DIR/sock"
CGROUP="/sys/fs/cgroup/jfs-pgbench"
BASE_REL="pgbench/base-s${PG_SCALE}-${IMAGE_SIZE}.ext4"
RUNS_REL="pgbench/runs"
METRICS_PORT="${METRICS_PORT:-9567}"
METRICS_ADDR=""

log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

need() {
	local c
	for c in "$@"; do command -v "$c" >/dev/null || die "missing command: $c"; done
}

now() { date +%s.%N; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.3f", b - a }'; }

find_pg_bin() {
	if [[ -n "$PG_BIN" ]]; then return; fi
	if command -v pg_config >/dev/null && [[ -x "$(pg_config --bindir)/initdb" ]]; then
		PG_BIN="$(pg_config --bindir)"
		return
	fi
	PG_BIN="$(ls -d /usr/lib/postgresql/*/bin 2>/dev/null | sort -V | tail -1 || true)"
	[[ -x "$PG_BIN/initdb" ]] || die "no PostgreSQL server binaries found; set PG_BIN"
}

as_pg() { runuser -u "$PG_USER" -- "$@"; }

variant_bin() { printf '%s' "${1#*=}"; }
variant_name() { printf '%s' "${1%%=*}"; }
variant_var() { local v="VARIANT_${1}_${2}"; printf '%s' "${!v:-}"; }

# ---- JuiceFS mount -------------------------------------------------------

JFS_BIN=""

MOUNTS=0

jfs_mount() { # name bin cache_dir log_file [extra options]
	local name="$1" bin="$2" cache="$3" logf="$4" extra="${5:-}" env_kv opts
	# A fresh port per mount, so a client still exiting can never be scraped
	# in place of the current one.
	METRICS_ADDR="127.0.0.1:$((METRICS_PORT + MOUNTS % 200))"
	MOUNTS=$((MOUNTS + 1))
	env_kv="$(variant_var "$name" ENV)"
	opts="$(variant_var "$name" MOUNT_OPTS)"
	mkdir -p "$MNT" "$cache"
	# shellcheck disable=SC2086 # option lists are word-split on purpose
	env $env_kv "$bin" mount -d --log "$logf" --cache-dir "$cache" --metrics "$METRICS_ADDR" \
		$MOUNT_OPTS $opts $extra "$META_URL" "$MNT" >>"$logf.out" 2>&1 || die "mount failed, see $logf and $logf.out"
	JFS_BIN="$bin"
	local i
	for i in $(seq 100); do
		mountpoint -q "$MNT" && return 0
		sleep 0.2
	done
	die "JuiceFS did not mount at $MNT"
}

jfs_umount() {
	local i
	if mountpoint -q "$MNT"; then
		for i in 1 2 3 4 5; do
			"$JFS_BIN" umount "$MNT" >/dev/null 2>&1 && break
			sleep 2
		done
		if mountpoint -q "$MNT"; then
			log "juicefs umount failed; lazy-unmounting $MNT"
			umount -l "$MNT"
		fi
	fi
	# umount returns before the client exits; it may still be writing its cache
	# dir, and the next mount must not share a metrics port with it.
	for i in $(seq 300); do
		pgrep -f -- " mount .* $MNT\$" >/dev/null || return 0
		sleep 0.2
	done
	log "WARNING: a JuiceFS client for $MNT is still running"
}

# rm_dir DIR: remove a client's cache dir, retrying while late writes land.
rm_dir() {
	local i
	for i in $(seq 50); do
		rm -rf "$1" 2>/dev/null && return 0
		sleep 0.2
	done
	log "WARNING: could not remove $1"
}

scrape() {
	curl -fsS "http://$METRICS_ADDR/metrics" >"$1" 2>/dev/null && return 0
	log "WARNING: no metrics from $METRICS_ADDR; object and cache columns for this run are empty"
	: >"$1"
}

# metric FILE NAME [LABEL_SUBSTRING]: sum of a metric across its label sets.
metric() {
	awk -v n="juicefs_$2" -v f="${3:-}" '
		$1 !~ /^#/ { split($1, a, "{"); if (a[1] == n && index($1, f)) s += $NF }
		END { printf "%.0f", s }' "$1"
}

delta() { awk -v a="$(metric "$1" "$3" "${4:-}")" -v b="$(metric "$2" "$3" "${4:-}")" 'BEGIN { printf "%.0f", b - a }'; }

# ---- disk image and PostgreSQL ------------------------------------------

LOOP=""

attach() { # image
	LOOP="$(losetup --find --show "$1")"
	mkdir -p "$PGMNT"
	mount -o noatime "$LOOP" "$PGMNT"
}

detach() {
	if mountpoint -q "$PGMNT"; then umount "$PGMNT"; fi
	if [[ -n "$LOOP" ]]; then
		losetup -d "$LOOP" || true
		LOOP=""
	fi
}

pg_ctl_start() { # log_file
	mkdir -p "$SOCK_DIR"
	chown "$PG_USER" "$SOCK_DIR"
	local cmd=(runuser -u "$PG_USER" -- "$PG_BIN/pg_ctl" -D "$PGMNT/data" -l "$1" -w -t 900
		-o "-k $SOCK_DIR -p $PG_PORT -c listen_addresses=''" start)
	if [[ -n "$PG_MEMORY_MAX" ]]; then
		(echo "$BASHPID" >"$CGROUP/cgroup.procs" && exec "${cmd[@]}") >/dev/null
	else
		"${cmd[@]}" >/dev/null
	fi
}

pg_ctl_stop() {
	[[ -f "$PGMNT/data/postmaster.pid" ]] || return 0
	as_pg "$PG_BIN/pg_ctl" -D "$PGMNT/data" -m fast -w -t 900 stop >/dev/null
}

# Page cache the server faults in is charged to its cgroup, so a memory limit
# bounds the filesystem cache the way guest RAM does.
setup_cgroup() {
	[[ -n "$PG_MEMORY_MAX" ]] || return 0
	local parent
	parent="$(dirname "$CGROUP")"
	mkdir -p "$CGROUP"
	if [[ ! -f "$CGROUP/memory.max" ]]; then
		echo +memory >"$parent/cgroup.subtree_control" 2>/dev/null || true
	fi
	[[ -f "$CGROUP/memory.max" ]] ||
		die "PG_MEMORY_MAX needs cgroup v2 with the memory controller enabled for $parent"
	echo "$PG_MEMORY_MAX" >"$CGROUP/memory.max"
}

drop_caches() {
	sync
	echo 3 >/proc/sys/vm/drop_caches
}

cleanup() {
	set +e
	pg_ctl_stop 2>/dev/null
	detach 2>/dev/null
	while read -r dev; do losetup -d "$dev"; done < <(losetup -nO NAME,BACK-FILE 2>/dev/null | awk -v p="$MNT/" 'index($2, p) == 1 { print $1 }')
	[[ -n "$JFS_BIN" ]] || JFS_BIN="$(variant_bin "${VARIANTS%% *}")"
	jfs_umount
	[[ -d "$CGROUP" ]] && rmdir "$CGROUP" 2>/dev/null
	return 0
}

# ---- one-time setup ------------------------------------------------------

ensure_volume() {
	local bin; bin="$(variant_bin "${VARIANTS%% *}")"
	if "$bin" status "$META_URL" >/dev/null 2>&1; then
		return
	fi
	[[ -n "$BUCKET" ]] || die "volume not formatted and BUCKET is unset"
	log "formatting volume $VOLUME_NAME"
	# Trash off: deleted run images free their objects immediately instead of
	# accumulating for a day.
	"$bin" format --storage "$STORAGE" --bucket "$BUCKET" --access-key "$ACCESS_KEY" \
		--secret-key "$SECRET_KEY" --trash-days 0 "$META_URL" "$VOLUME_NAME" >&2
}

ensure_base() { # variant
	local spec="$1" name bin base
	name="$(variant_name "$spec")"
	bin="$(variant_bin "$spec")"
	base="$MNT/$BASE_REL"
	jfs_mount "$name" "$bin" "$WORK_DIR/cache/base" "$OUT_DIR/base-mount.log"
	if [[ -f "$base.complete" ]]; then
		log "reusing base image $BASE_REL"
		jfs_umount
		return
	fi
	log "building base image $BASE_REL (scale $PG_SCALE, $IMAGE_SIZE)"
	mkdir -p "$(dirname "$base")"
	rm -f "$base"
	truncate -s "$IMAGE_SIZE" "$base"
	# Initialize inode tables and the journal now, so no run pays for it.
	mkfs.ext4 -q -F -E lazy_itable_init=0,lazy_journal_init=0 "$base"
	attach "$base"
	mkdir "$PGMNT/data"
	chown "$PG_USER" "$PGMNT/data"
	as_pg "$PG_BIN/initdb" -D "$PGMNT/data" -A trust >"$OUT_DIR/initdb.log"
	{
		echo "shared_buffers = '$PG_SHARED_BUFFERS'"
		echo "fsync = on"
		echo "synchronous_commit = on"
		echo "full_page_writes = on"
		printf '%s\n' "$PG_EXTRA_CONF"
	} >>"$PGMNT/data/postgresql.conf"
	pg_ctl_start "$OUT_DIR/base-postgres.log"
	as_pg "$PG_BIN/pgbench" -h "$SOCK_DIR" -p "$PG_PORT" -i -s "$PG_SCALE" -q postgres >"$OUT_DIR/pgbench-init.log" 2>&1
	as_pg "$PG_BIN/psql" -h "$SOCK_DIR" -p "$PG_PORT" -q -c 'VACUUM ANALYZE' -c CHECKPOINT postgres
	pg_ctl_stop
	detach
	sync
	# Snapshots are compacted before sandboxes clone them; start from the same.
	"$bin" compact "$base" >"$OUT_DIR/base-compact.log" 2>&1
	chmod 0444 "$base"
	echo "scale=$PG_SCALE size=$IMAGE_SIZE built=$(date -u +%FT%TZ)" >"$base.complete"
	jfs_umount
}

# ---- fragmentation from `juicefs info --raw` -----------------------------

# Prints "chunkIndex sliceId length" per visible segment of FILE.
segments() {
	"$JFS_BIN" info --raw "$1" 2>/dev/null | awk '
		/^ *chunks:/ { on = 1; next }
		/^ *(objects|flocks|plocks):/ { on = 0 }
		on { gsub(/[^0-9]+/, " "); n = split($0, f, " "); if (n == 5) print f[1], f[2], f[5] }'
}

# fragmentation RUN_SEGS BASE_SEGS: segments per chunk (mean, max), percent of
# data bytes still shared with the base, and MiB the run no longer shares.
fragmentation() {
	awk '
		NR == FNR { base[$2] = 1; next }
		$2 != 0 { segs[$1]++; total += $3; if ($2 in base) shared += $3 }
		END {
			for (c in segs) { n++; sum += segs[c]; if (segs[c] > max) max = segs[c] }
			printf "%.2f\t%d\t%.1f\t%.0f", (n ? sum / n : 0), max,
				(total ? 100 * shared / total : 0), (total - shared) / 1048576
		}' "$2" "$1"
}

# chunk_meta FILE: entries in FILE's chunk lists and the Redis memory they take,
# in KiB. NA unless the metadata engine is Redis.
chunk_meta() {
	if [[ "$META_URL" != redis://* && "$META_URL" != rediss://* ]] || ! command -v redis-cli >/dev/null; then
		printf 'NA\tNA'
		return
	fi
	local ino chunks i
	ino="$(stat -c %i "$1")"
	chunks=$((($(stat -c %s "$1") + (64 << 20) - 1) / (64 << 20)))
	for ((i = 0; i < chunks; i++)); do
		echo "LLEN c${ino}_$i"
		echo "MEMORY USAGE c${ino}_$i"
	done | redis-cli -u "$META_URL" 2>/dev/null | awk '
		NR % 2 == 1 { n += $1 } NR % 2 == 0 && $1 ~ /^[0-9]+$/ { b += $1 }
		END { printf "%d\t%.0f", n, b / 1024 }'
}

# ---- one measured run ----------------------------------------------------

latency_pcts() { # txn log prefix -> "p50 p99 p99.9" in ms
	cat "$1".* 2>/dev/null | awk '{ print $3 }' | sort -n | awk '
		{ v[NR] = $1 }
		END {
			if (!NR) { printf "NA\tNA\tNA"; exit }
			printf "%.2f\t%.2f\t%.2f", v[int(NR * .50) + 1] / 1000, v[int(NR * .99) + 1] / 1000,
				v[int(NR * .999) + 1] / 1000
		}'
}

run_one() { # rep variant base_segments
	local rep="$1" spec="$2" base_segs="$3" name bin id dir img
	name="$(variant_name "$spec")"
	bin="$(variant_bin "$spec")"
	id="r${rep}-${name}"
	dir="$OUT_DIR/runs/$id"
	install -d -o "$PG_USER" "$dir"
	log "[$id] mount"
	rm_dir "${WORK_DIR:?}/cache/$id"
	jfs_mount "$name" "$bin" "$WORK_DIR/cache/$id" "$dir/juicefs.log"
	"$bin" version >"$dir/juicefs-version.txt" 2>&1 || true

	img="$MNT/$RUNS_REL/$(basename "$OUT_DIR")-$id.ext4"
	mkdir -p "$(dirname "$img")"
	local t clone_s ready_s reclone_s
	t="$(now)"
	"$bin" clone "$MNT/$BASE_REL" "$img" >/dev/null
	clone_s="$(since "$t")"
	chmod 0644 "$img"
	local kv
	for kv in $IMAGE_XATTRS; do
		setfattr -n "${kv%%=*}" -v "${kv#*=}" "$img" || die "setfattr $kv failed; is --enable-xattr in the mount options?"
	done
	if [[ "$COLD" == 1 ]]; then drop_caches; fi

	attach "$img"
	t="$(now)"
	pg_ctl_start "$dir/postgres.log"
	ready_s="$(since "$t")"

	scrape "$dir/metrics-before.prom"
	local mode_args=()
	[[ "$PG_MODE" == select ]] && mode_args=(-S)
	log "[$id] pgbench $PG_MODE ${PG_DURATION}s"
	(cd "$dir" && as_pg "$PG_BIN/pgbench" -h "$SOCK_DIR" -p "$PG_PORT" -n -M prepared "${mode_args[@]}" \
		-c "$PG_CLIENTS" -j "$PG_JOBS" -T "$PG_DURATION" -P 10 \
		-l --sampling-rate "$PG_SAMPLING_RATE" --log-prefix "$dir/txn" postgres) >"$dir/pgbench.log" 2>&1 ||
		die "pgbench failed, see $dir/pgbench.log"
	scrape "$dir/metrics-after.prom"

	pg_ctl_stop
	detach
	sync

	# The image is now fragmented the way a used sandbox disk is; cloning it
	# stands in for the clone sandbox-host takes while a VM is paused.
	t="$(now)"
	"$bin" clone "$img" "$img.reclone" >/dev/null
	reclone_s="$(since "$t")"
	rm -f "$img.reclone"

	# Inspect and read the image through a read-only mount with an empty cache:
	# reading a chunk with 5 or more slices otherwise starts compacting it,
	# which would change the layout being measured.
	jfs_umount
	rm_dir "${WORK_DIR:?}/cache/$id"
	jfs_mount "$name" "$bin" "$WORK_DIR/cache/$id" "$dir/juicefs-read.log" --read-only
	local meta seq_s
	meta="$(chunk_meta "$img")"
	segments "$img" >"$dir/segments.txt"
	drop_caches
	scrape "$dir/metrics-read-before.prom"
	t="$(now)"
	dd if="$img" of=/dev/null bs=4M status=none
	seq_s="$(since "$t")"
	scrape "$dir/metrics-read-after.prom"
	local rb="$dir/metrics-read-before.prom" ra="$dir/metrics-read-after.prom"
	local frag tps lat_avg pcts
	frag="$(fragmentation "$dir/segments.txt" "$base_segs")"

	local b="$dir/metrics-before.prom" a="$dir/metrics-after.prom" hits miss
	hits="$(delta "$b" "$a" blockcache_hits)"
	miss="$(delta "$b" "$a" blockcache_miss)"
	tps="$(awk '/^tps = / { print $3; exit }' "$dir/pgbench.log")"
	lat_avg="$(awk '/^latency average = / { print $4; exit }' "$dir/pgbench.log")"
	pcts="$(latency_pcts "$dir/txn")"
	gzip -1 "$dir"/txn.* 2>/dev/null || true

	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$name" "$rep" "$PG_MODE" "${tps:-NA}" "${lat_avg:-NA}" "$pcts" "$ready_s" "$clone_s" "$reclone_s" "$frag" \
		"$(awk -v b="$(delta "$b" "$a" object_request_data_bytes 'method="PUT"')" 'BEGIN { printf "%.0f", b / 1048576 }')" \
		"$(awk -v b="$(delta "$b" "$a" object_request_data_bytes 'method="GET"')" 'BEGIN { printf "%.0f", b / 1048576 }')" \
		"$(delta "$b" "$a" object_request_durations_histogram_seconds_count 'method="PUT"')" \
		"$(delta "$b" "$a" object_request_durations_histogram_seconds_count 'method="GET"')" \
		"$(awk -v h="$hits" -v m="$miss" 'BEGIN { printf "%.1f", (h + m) ? 100 * h / (h + m) : 0 }')" \
		"$(awk -v b="$(delta "$b" "$a" compact_size_histogram_bytes_sum)" 'BEGIN { printf "%.0f", b / 1048576 }')" \
		"$meta" "$seq_s" \
		"$(awk -v s="$seq_s" -v n="$(stat -c %s "$img")" 'BEGIN { printf "%.0f", (s > 0 ? n / 1048576 / s : 0) }')" \
		"$(delta "$rb" "$ra" object_request_durations_histogram_seconds_count 'method="GET"')" \
		"$(awk -v b="$(delta "$rb" "$ra" object_request_data_bytes 'method="GET"')" 'BEGIN { printf "%.0f", b / 1048576 }')" \
		>>"$OUT_DIR/results.tsv"

	jfs_umount
	rm_dir "${WORK_DIR:?}/cache/$id"
	if [[ "$KEEP_RUNS" != 1 ]]; then
		jfs_mount "$name" "$bin" "$WORK_DIR/cache/$id" "$dir/juicefs-cleanup.log"
		rm -f "$img"
		jfs_umount
		rm_dir "${WORK_DIR:?}/cache/$id"
	fi
	log "[$id] tps=${tps:-NA} p50/p99/p99.9(ms)=$(tr '\t' / <<<"$pcts") ready=${ready_s}s reclone=${reclone_s}s seq_read=${seq_s}s meta=$(tr '\t' / <<<"$meta")"
}

HEADER=$'variant\trep\tmode\ttps\tlat_avg_ms\tlat_p50_ms\tlat_p99_ms\tlat_p999_ms\tpg_ready_s\tclone_s\treclone_s\tsegs_per_chunk\tsegs_per_chunk_max\tshared_with_base_pct\tunshared_mib\tput_mib\tget_mib\tput_reqs\tget_reqs\tcache_hit_pct\tcompacted_mib\tmeta_entries\tmeta_kib\tseq_read_s\tseq_read_mibps\tseq_get_reqs\tseq_get_mib'

cmd_run() {
	[[ -n "$META_URL" ]] || die "set META_URL to the metadata engine URL"
	[[ $EUID -eq 0 ]] || die "run as root (FUSE, losetup, mount, drop_caches)"
	need losetup mkfs.ext4 mountpoint runuser curl awk sort
	[[ -z "$IMAGE_XATTRS" ]] || need setfattr
	find_pg_bin
	id "$PG_USER" >/dev/null 2>&1 || die "user $PG_USER does not exist; set PG_USER"
	local spec names=()
	for spec in $VARIANTS; do
		[[ "$spec" == *=* ]] || die "variant '$spec' is not name=/path/to/juicefs"
		[[ "$(variant_name "$spec")" =~ ^[A-Za-z0-9_]+$ ]] || die "variant name in '$spec' must be [A-Za-z0-9_]"
		[[ -x "$(variant_bin "$spec")" ]] || die "not executable: $(variant_bin "$spec")"
		names+=("$spec")
	done

	install -d -o "$PG_USER" "$OUT_DIR"
	runuser -u "$PG_USER" -- test -w "$OUT_DIR" || die "$PG_USER cannot write $OUT_DIR; pick an OUT_DIR it can reach"
	trap cleanup EXIT
	trap 'exit 130' INT TERM
	{
		echo "date=$(date -u +%FT%TZ) host=$(hostname) kernel=$(uname -r)"
		echo "variants=$VARIANTS"
		for spec in "${names[@]}"; do
			local n; n="$(variant_name "$spec")"
			echo "  $n env='$(variant_var "$n" ENV)' opts='$(variant_var "$n" MOUNT_OPTS)'"
		done
		echo "mount_opts=$MOUNT_OPTS storage=$STORAGE image_xattrs=$IMAGE_XATTRS"
		echo "image=$IMAGE_SIZE scale=$PG_SCALE mode=$PG_MODE clients=$PG_CLIENTS jobs=$PG_JOBS duration=$PG_DURATION"
		echo "shared_buffers=$PG_SHARED_BUFFERS memory_max=${PG_MEMORY_MAX:-none} cold=$COLD reps=$REPS"
		echo "postgres=$("$PG_BIN/postgres" --version)"
		grep -E '^(MemTotal|MemAvailable)' /proc/meminfo
	} >"$OUT_DIR/config.txt"
	local db_mib=$((PG_SCALE * 15)) avail_mib
	avail_mib="$(awk '/^MemAvailable/ { print int($2 / 1024) }' /proc/meminfo)"
	if [[ -z "$PG_MEMORY_MAX" && "$db_mib" -lt "$avail_mib" ]]; then
		log "WARNING: the database (~${db_mib} MiB) fits in free memory (${avail_mib} MiB);" \
			"reads will mostly hit the page cache. Raise PG_SCALE or set PG_MEMORY_MAX."
	fi

	setup_cgroup
	ensure_volume
	ensure_base "${names[0]}"

	# Base slice IDs, for measuring how much of a run image still shares them.
	jfs_mount "$(variant_name "${names[0]}")" "$(variant_bin "${names[0]}")" "$WORK_DIR/cache/base" "$OUT_DIR/base-mount.log"
	segments "$MNT/$BASE_REL" >"$OUT_DIR/base-segments.txt"
	jfs_umount

	echo "$HEADER" >"$OUT_DIR/results.tsv"
	local rep order
	for rep in $(seq "$REPS"); do
		# Alternate the order each rep so drift (bucket warm-up, a noisy
		# neighbor) doesn't favor whichever variant always goes first.
		order=("${names[@]}")
		if ((rep % 2 == 0)); then
			local i rev=()
			for ((i = ${#order[@]} - 1; i >= 0; i--)); do rev+=("${order[i]}"); done
			order=("${rev[@]}")
		fi
		for spec in "${order[@]}"; do
			run_one "$rep" "$spec" "$OUT_DIR/base-segments.txt"
		done
	done
	cmd_summarize "$OUT_DIR"
}

cmd_summarize() {
	local f="${1:?usage: run.sh summarize OUT_DIR}/results.tsv"
	[[ -f "$f" ]] || die "no $f"
	# Median per variant, then min..max, for every numeric column.
	awk -F'\t' '
		NR == 1 { for (i = 1; i <= NF; i++) col[i] = $i; ncol = NF; next }
		{
			if (!($1 in seen)) { seen[$1] = 1; order[++nv] = $1 }
			n[$1]++
			for (i = 4; i <= ncol; i++) val[$1, i, n[$1]] = $i
		}
		function med(v, i,   k, m, a, j, t) {
			m = n[v]
			for (k = 1; k <= m; k++) a[k] = val[v, i, k] + 0
			for (k = 2; k <= m; k++) { t = a[k]; for (j = k - 1; j >= 1 && a[j] > t; j--) a[j + 1] = a[j]; a[j + 1] = t }
			lo = a[1]; hi = a[m]
			return (m % 2) ? a[(m + 1) / 2] : (a[m / 2] + a[m / 2 + 1]) / 2
		}
		END {
			printf "%-22s", "metric (median, min..max)"
			for (v = 1; v <= nv; v++) printf "  %-26s", order[v] " (n=" n[order[v]] ")"
			print ""
			for (i = 4; i <= ncol; i++) {
				printf "%-22s", col[i]
				for (v = 1; v <= nv; v++) {
					m = med(order[v], i)
					printf "  %-26s", sprintf("%g (%g..%g)", m, lo, hi)
				}
				print ""
			}
		}' "$f"
}

case "${1:-run}" in
run) cmd_run ;;
summarize) cmd_summarize "${2:-}" ;;
cleanup) cleanup ;;
*) die "usage: run.sh [run|summarize OUT_DIR|cleanup]" ;;
esac
