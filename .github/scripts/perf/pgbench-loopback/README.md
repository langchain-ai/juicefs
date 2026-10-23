# pgbench on a loop-mounted ext4 image

Measures JuiceFS under the I/O pattern of a VM disk image without running a VM:
an ext4 image stored on JuiceFS, attached with `losetup`, with PostgreSQL on it.
The kernel turns database writes into small random writes into one large file,
and every `fsync` in the database into an `fsync` of that file. Each run gets
its own `juicefs clone` of a shared base image, so the copy-on-write sharing,
fragmentation and compaction that per-VM disks see are reproduced too.

It compares variants (JuiceFS binaries, environment, mount options) in
interleaved repetitions and reports medians.

What it does not cover: memory-image restore (mmap page faults on a `.mem`
file) and the guest's own memory pressure beyond what `PG_MEMORY_MAX` models.

## Files

| File | Purpose |
| --- | --- |
| `run.sh` | The benchmark: builds the base image once, runs every rep, summarizes. |
| `backend.sh` | Local Redis and RustFS (or MinIO) behind `delayproxy`, for runs without a cloud account. |
| `delayproxy/` | Latency proxy: per-request log-normal delays and bandwidth limits for S3, a fixed delay for Redis. |

## Quick start

On a Linux box with Docker, Go, PostgreSQL server binaries, and root (`run.sh`
needs it for FUSE, `losetup`, and dropping caches):

```bash
.github/scripts/perf/pgbench-loopback/backend.sh up
```

```bash
sudo bash -c 'source /var/tmp/jfs-pgbench-backend/env && VARIANTS="before=/path/to/juicefs-main after=/path/to/juicefs-branch" .github/scripts/perf/pgbench-loopback/run.sh run'
```

```bash
.github/scripts/perf/pgbench-loopback/backend.sh down
```

Against a real bucket and Redis instead, skip `backend.sh` and set `META_URL`,
`BUCKET`, `ACCESS_KEY` and `SECRET_KEY` (or rely on the instance's credentials).
The volume is formatted on first use with `--trash-days 0`; an existing volume is
reused as is.

## Configuration

All settings are environment variables.

| Variable | Default | Meaning |
| --- | --- | --- |
| `META_URL` | (required) | Metadata engine URL. |
| `STORAGE`, `BUCKET`, `ACCESS_KEY`, `SECRET_KEY` | `s3`, unset | Used only to format a new volume. |
| `VARIANTS` | `default=$(command -v juicefs)` | Space-separated `name=/path/to/juicefs`. The first builds the base image. |
| `VARIANT_<name>_ENV` | | Extra environment for that variant's mount, e.g. `JFS_COMMIT_MODE=epoch`. |
| `VARIANT_<name>_MOUNT_OPTS` | | Extra mount options for that variant. |
| `MOUNT_OPTS` | sandbox-host's options, `--cache-size=20480`, `--no-bgjob` | Options for every mount. |
| `REPS` | `5` | Repetitions per variant. Order alternates each rep. |
| `IMAGE_SIZE`, `PG_SCALE` | `32G`, `1000` | Image size and pgbench scale (~15 MiB per unit). |
| `PG_MODE` | `tpcb` | `tpcb` (read-write, fsync per commit) or `select` (`-S`, read-only). |
| `PG_CLIENTS`, `PG_JOBS`, `PG_DURATION` | `16`, `4`, `600` | pgbench `-c`, `-j`, `-T`. |
| `PG_MEMORY_MAX` | | cgroup v2 memory limit for PostgreSQL, standing in for guest RAM. |
| `PG_SHARED_BUFFERS`, `PG_EXTRA_CONF` | `128MB`, | Server settings written into the base image. |
| `PG_USER`, `PG_BIN` | `postgres`, auto-detected | User PostgreSQL runs as, and its binaries' directory. |
| `PG_SAMPLING_RATE` | `1` | Fraction of transactions logged for percentiles; lower it for `select`. |
| `COLD` | `1` | Drop the kernel page cache after cloning. Every run uses a fresh JuiceFS cache dir regardless. |
| `IMAGE_XATTRS` | | Space-separated `name=value` xattrs set on each run's image, e.g. `user.juicefs.profile=vm-disk`. Needs `--enable-xattr` in the mount options. |
| `KEEP_RUNS` | `0` | Keep each run's image on the volume instead of deleting it. |
| `WORK_DIR`, `OUT_DIR` | `/var/tmp/jfs-pgbench`, `$WORK_DIR/results/<time>` | Mounts, caches, and results. `OUT_DIR` must be writable by `PG_USER`. |

The base image is keyed by scale and size, so changing either builds a new one;
changing `PG_SHARED_BUFFERS` or `PG_EXTRA_CONF` does not, so delete the
`pgbench/base-*` files on the volume after changing those.

## One run

1. Mount the variant with a fresh cache dir.
2. `juicefs clone` the base image (`clone_s`), then drop the page cache.
3. Attach, mount, and start PostgreSQL (`pg_ready_s`).
4. Run pgbench, scraping the client's metrics before and after.
5. Stop PostgreSQL and detach. Clone the now fragmented image (`reclone_s`),
   which stands in for the clone sandbox-host takes while a VM is paused.
6. Remount read-only with an empty cache, so reading can't start a compaction
   that changes the layout being measured. Count the image's chunk-list entries
   and their Redis memory, read its slices with `juicefs info --raw`, then drop the
   page cache and read the whole image sequentially.

## Output

`OUT_DIR/results.tsv` has one row per run; `run.sh summarize OUT_DIR` prints the
median and range per variant. Each run's directory keeps the pgbench output,
per-transaction logs (gzipped), PostgreSQL and JuiceFS logs, and both metric
scrapes.

| Column | Meaning |
| --- | --- |
| `tps`, `lat_avg_ms` | From pgbench's summary. |
| `lat_p50_ms`, `lat_p99_ms`, `lat_p999_ms` | From the per-transaction log. |
| `segs_per_chunk`, `segs_per_chunk_max` | Data segments per 64 MiB chunk of the image after the run: how fragmented it is. |
| `shared_with_base_pct`, `unshared_mib` | Share of the image's data still referencing the base's slices, and how much no longer does. |
| `put_mib`, `get_mib`, `put_reqs`, `get_reqs` | Object traffic during pgbench. |
| `cache_hit_pct` | Block-cache hits during pgbench. |
| `compacted_mib` | Data rewritten by compaction during pgbench. |
| `meta_entries`, `meta_kib` | Entries in the image's chunk lists and the Redis memory they take (Redis only). |
| `seq_read_s`, `seq_read_mibps` | Reading the whole image with `dd` after the run, through a read-only mount with an empty cache. |
| `seq_get_reqs`, `seq_get_mib` | Object reads during that sequential read. |

## Making the object store realistic

Against local RustFS or MinIO an upload takes about a millisecond, and in
JuiceFS an `fsync` waits for its blocks to upload, so a local backend hides the
cost that dominates write-heavy workloads. `delayproxy` adds it back:

- **S3** (`-mode http`): every request waits for a delay drawn from a log-normal
  distribution with the configured p50 and p99 (separately for GET, PUT, and
  everything else), and each body is limited to a per-stream bandwidth, plus an
  optional total. Per-request delays are used rather than `tc netem`, which
  delays packets and so charges a multi-round-trip upload several times.
- **Redis** (`-mode tcp`): a fixed one-way delay on each direction, applied
  without serializing pipelined commands.

The `s3-standard` and `s3-express` presets are starting points, not
measurements. Calibrate them against the bucket you are modelling by running the
same `juicefs objbench` against both and adjusting until they match:

```bash
juicefs objbench --storage s3 --big-object-size 64 --small-objects 100 -p 4 --skip-functional-tests https://<real-bucket-endpoint>/objbench
```

```bash
juicefs objbench --storage s3 --access-key jfsbench --secret-key jfsbench-secret --big-object-size 64 --small-objects 100 -p 4 --skip-functional-tests http://127.0.0.1:39100/objbench
```

Then set `S3_PROXY_ARGS` (for example
`S3_PROXY_ARGS="-put-p50 35ms -put-p99 150ms -stream-mbps 80"`) and
`REDIS_DELAY` to half the measured Redis round trip, and run `backend.sh up` again.

## Caveats

- **Fit in RAM, measure RAM.** If the database fits in the page cache, reads
  never reach JuiceFS. `run.sh` warns when it does; raise `PG_SCALE` or set
  `PG_MEMORY_MAX`.
- **Loop over FUSE under memory pressure.** Writeback through a loop device into
  a FUSE file can stall when the host is short of memory, because the FUSE
  daemon needs memory to make progress. Leave the host headroom.
- **Use a real host for numbers.** Docker Desktop or a laptop VM works for
  checking the harness, but its timers and disks are too noisy to compare
  variants.
- **Use enough reps.** Medians over five reps catch large effects. For small
  ones, add reps until the variants' ranges stop overlapping, or bootstrap
  confidence intervals from `results.tsv`.
