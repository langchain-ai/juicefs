---
title: Compact usage exports
sidebar_position: 8
---

`juicefs dump-usage META-URL [FILE]` exports the active volume tree for physical
storage attribution. It includes paths and raw slice references, including
overwritten references that still occupy storage. It excludes Trash, detached
and sustained-only files, extended attributes, symlink targets, credentials, and
other recovery metadata. It cannot be loaded with `juicefs load` and does not
replace a recovery backup.

```shell
juicefs dump-usage redis://localhost usage.ndjson.gz
juicefs dump-usage sqlite3://meta.db usage.ndjson --threads 4
```

Omitting `FILE` writes uncompressed NDJSON to standard output. A `.gz` suffix
enables gzip. Files are written through a private temporary file and renamed
only after successful completion, so a failed export leaves an existing output
file intact. `--threads` controls concurrent metadata reads, defaults to two,
and accepts values from 1 to 64.

## Automatic exports

Mount and gateway clients can enable `--usage-export 1h`. The default is `0`
(disabled), and the minimum enabled interval is five minutes. Read-only clients
and clients with `--no-bgjob` do not run automatic exports.

The schedule is independent of `--backup-meta`, its last-run timestamp, and its
inode-count cutoff. The interval is measured from the last successful completed
publication. A renewable, two-minute metadata lease coordinates participating
clients. Renewal failures or expiry cancel the traversal and upload; failed
runs become eligible for retry after their lease expires. Clients should have
synchronized clocks. Coordination is cooperative: consumers must still tolerate
multiple immutable exports and late arrivals during failures or clock skew.

Objects are published to
`usage/usage-YYYY-MM-DD-hhmmss.nnnnnnnnn-EXPORT-UUID.ndjson.gz` inside the volume's
object-storage prefix. The timestamp is the UTC traversal start. Local temporary
files are removed on success and failure. A failed upload is removed on a
best-effort basis; consumers must validate completeness before using any object.

After a successful export, cleanup removes at most 1,000 generated usage objects
per pass when both their start timestamp and object modification time are older
than 48 hours. Unrecognized names and objects outside `usage/` are untouched.
Configure object-storage lifecycle rules if a different retention policy is
needed.

Recovery exports and their Trash data remain unchanged. Usage exports perform a
separate active-tree scan, including when a recovery backup is due at the same
time. This reduces bytes consumed by usage readers and allows independent
completion; it does not reduce total metadata reads when both exports run.
Directory entries are retained for sorting, and raw chunk reads are processed in
bounded batches rather than collecting all slices of a large file.

## NDJSON version 1

Each record occupies one UTF-8 JSON line ending in LF. A header, zero or more
entry/slice records, and one final completion record form an export. Every export
includes the root directory entry. For example:

```json
{"type":"header","format":"juicefs-usage","version":1,"source_id":"6fded32e-4e2b-4eec-9dbe-8fb4f14d218d","export_id":"019c5930-c000-7000-8000-000000000001","started_at":"2026-01-01T00:00:00Z"}
{"type":"entry","path":"/","inode":1,"kind":"directory"}
{"type":"entry","path":"/data","inode":2,"kind":"regular"}
{"type":"slice","id":9007199254740993,"size":4096}
{"type":"end","entries":2,"slices":1,"sha256":"<hex digest of preceding lines>","completed_at":"2026-01-01T00:00:01Z"}
```

The header identifies the format, version, volume UUID (`source_id`), unique
export UUID (`export_id`), and traversal start (`started_at`, RFC 3339 with optional
fractional seconds). No metadata-server URL or storage credentials are included.

Entries use depth-first order with directory children sorted by their original
name bytes. `kind` is one of `regular`, `directory`, `symlink`, `fifo`, `blockdev`,
`chardev`, or `socket`. Every active path is emitted, even if it has no slices;
hardlinks appear once for each path. Paths start with `/` and use the same
percent escaping as JSON recovery dumps: control bytes, spaces, invalid UTF-8
bytes, percent signs, quotes, and backslashes are encoded as `%XX`. Separators
remain `/`. Decode escapes exactly once if raw filename bytes are needed; avoid
URL or filesystem path normalization.

Each slice record belongs to the immediately preceding entry. Only `regular`
entries have slices. Chunk indices are visited in ascending order and each
chunk's original slice order is retained. Files with zero logical length have no
slice records, matching JSON recovery dumps. Slice ID zero represents a hole and is
omitted; nonzero IDs with size zero are retained. Slice IDs and inode numbers
are exact unsigned 64-bit integers, and sizes are unsigned 32-bit byte counts.
Readers must avoid floating-point conversion. Multiple paths and multiple
records can refer to the same slice; the export performs no attribution policy
or global deduplication.

The final `end` record contains entry and slice record counts, `completed_at`,
and lowercase hexadecimal SHA-256 over the exact uncompressed bytes of all
preceding records, including each LF. The `end` record is excluded from the
checksum. Consumers must reject unsupported versions, missing or duplicate
headers/completion records, unknown record types, invalid ordering, count or
checksum mismatches, trailing data, and truncated or corrupt gzip streams. Read
through EOF and validate gzip integrity before publishing derived results. A
valid checksum establishes complete transmission, not authenticity; protect
access to exported filesystem paths using the volume's normal storage controls.

An encoded record including LF is limited to 1 MiB, percent-escaped paths to 64 KiB,
and directory depth to 1,024. Exceeding a limit fails the export. The traversal
is live, as with JSON recovery dumps; it is not a point-in-time filesystem
snapshot. Concurrent creates, deletes, writes, and renames can affect which
references are observed between the start and completion timestamps.
