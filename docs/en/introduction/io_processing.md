---
title: Data Processing Workflow
sidebar_position: 3
slug: /internals/io_processing
description: This article introduces read and write implementation of JuiceFS, including how it splits files into chunks.
---

## Data writing process {#workflow-of-write}

JuiceFS splits large files at multiple levels to improve I/O performance. See [how JuiceFS stores files](./architecture.md#how-juicefs-store-files). Files are initially divided into logical chunks (64 MiB each), which are isolated from each other and further broken down into slices. Slices are the data units for persistence. During a write request, data is stored in the client buffer as chunks/slices. A new slice is created if it does not overlap or adjoin any existing slices; otherwise, the affected existing slices are updated. On a flush operation, a slice is divided into blocks (4 MiB by default) and uploaded to the object storage. Metadata is updated upon successful upload.

Sequential writes are optimized, requiring only one continuously growing slice and one final flush. This maximizes object storage write performance. A simple [JuiceFS benchmark](../benchmark/performance_evaluation_guide.md) below shows sequentially writing a 1 GiB file with a 1 MiB I/O size at its first stage. The following figure shows the data flow in each component of the system.

![internals-write](../images/internals-write.png)

Use [`juicefs stats`](../reference/command_reference.mdx#stats) to obtain real-time performance monitoring metrics.

![internals-stats](../images/internals-stats.png)

The first highlighted section in the above figure shows:

- The average I/O size for writing to the object storage is `object.put / object.put_c = 4 MiB`. It is the same as the default block size.
- The ratio of metadata transactions to object storage transactions is `meta.txn : object.put_c -= 1 : 16`. It means that a single slice flush requires 1 metadata update and 16 uploads to the object storage. Each flush operation transmits 64 MiB of data (4 MiB * 16), equivalent to the default chunk size.
- The average request size in the FUSE layer approximately equals to `fuse.write / fuse.ops ~= 128 KiB`, matching the default request size limitation.

Generally, when JuiceFS writes a small file, the file is uploaded to the object storage upon file closure, and the I/O size is equal to the file size. In the third stage of the figure above, where 128 KiB small files are created, we can see that:

- The size of data written to the object storage during PUT operations is 128 KiB, calculated by `object.put / object.put_c`.
- The number of metadata transactions is approximately twice the number of PUT operations, since each file requires one create and one write.

When JuiceFS uploads objects smaller than the block size, it simultaneously writes them into the [local cache](../guide/cache.md) to improve future performance. As shown in the third stage of the figure above, the write bandwidth of the `blockcache` is the same as that of the object storage. Since small files are cached, reading these files is extremely fast, as demonstrated in the fourth stage.

Write operations are immediately committed to the client buffer, resulting in very low write latency (typically just a few microseconds). The actual upload to the object storage is automatically triggered internally when certain conditions are met, such as when the size or number of slices exceeds their limit, or data stays in the buffer for too long. Explicit calls, such as closing a file or invoking `fsync`, can also trigger uploading.

The client buffer is only released after the data stored inside is uploaded. In scenarios with high write concurrency, if the buffer size (configured using [`--buffer-size`](../reference/command_reference.mdx#mount-data-cache-options)) is not big enough, or the object storage's performance insufficient, write blocking may occur, because the buffer cannot be released timely. The real-time buffer usage is shown in the `usage.buf` field in the metrics figure. To slow things down, The JuiceFS client introduces a 10 ms delay to every write when the buffer usage exceeds the threshold. If the buffer usage is over twice the threshold, new writes are completely suspended until the buffer is released. Therefore, if the write latency keeps increasing or the buffer usage has exceeded the threshold for a long while, you should increase `--buffer-size`. Also consider increasing the maximum number of upload concurrency ([`--max-uploads`](../reference/command_reference.mdx#mount-data-storage-options), defaults to 20), which improves the upload bandwidth, thus boosting buffer release.

### Random writes {#random-write}

JuiceFS supports random writes, including mmap-based random writes.

Note that a block is an immutable object, because most object storage services don't support edit in blocks; they can only be re-uploaded and overwritten. Thus, when overwrites or random writes occur, JuiceFS avoids downloading the block for editing and re-uploading, which could cause serious I/O amplifications. Instead, writes are performed on new or existing slices. Relevant new blocks are uploaded to the object storage, and the new slice is appended to the slice list under the chunk. When a file is read, what the client sees is actually a consolidated view of all the slices.

Compared to sequential writes, random writes in large files are more complicated. There could be a number of intermittent slices in a chunk, possibly all smaller than 4 MiB. Frequent random writes require frequent metadata updates, which in turn further impact performance. To improve read performance, JuiceFS schedules compaction tasks when the number of slices under a chunk exceeds the limit. You can also manually trigger compaction by running [`juicefs gc`](../administration/status_check_and_maintenance.md#gc).

The write-triggered compaction interval defaults to 200 slices. Set `JFS_WRITE_COMPACTION_INTERVAL` in the JuiceFS client process environment to an integer from 100 through 350 to change it; `100` restores the previous cadence. The client reads this setting once when its metadata client is created, so changing a running mount requires restarting it with the new environment. Unset or empty values use 200; invalid or out-of-range values produce a warning and fall back to 200. The interval is based on each chunk's current slice count: compaction is considered when the count modulo the interval equals the interval minus one. The independent trigger above 350 slices, synchronous compaction at 2500 slices, and read-triggered and manual compaction are unaffected.

### Commit mode {#commit-mode}

By default each chunk commits its own slices to the metadata engine, one transaction per slice, in the order the chunk created them. The chunks of a file commit independently, so if the client crashes before a flush, the metadata can hold a later write of a file and miss an earlier one, of another chunk or of the same chunk.

Set `JFS_COMMIT_MODE=epoch` in the client process environment to commit in epochs instead. The client groups the slices of each file into epochs and commits each epoch in one metadata transaction (all of its slices or none of them), oldest first, once all of its slices are uploaded. After a client crash, each file's durable state is then its content at an epoch boundary: every write made before the boundary is there and no write made after it is, as if the writes had stopped at that point. Unset, empty, or `chunk` keeps the per-chunk commits described above. An epoch closes when it is `JFS_EPOCH_MAX_AGE_MS` milliseconds old (default 5000, from 1 through 3600000), counted from its first slice; when the file has had no write for a second; when it holds `JFS_EPOCH_MAX_SLICES` slices (default 512, from 2 through 1048576) or reaches the metadata engine's limits for one transaction; on `fsync`, `flush`, `close`, truncate, and the other operations that flush the file; when the write buffer is full; and when a read must see its writes (see [the reading process](#workflow-of-read)). The client checks the age and idle rules every 100 ms, so an epoch can stay open up to about 100 ms past them, and an age under 100 ms acts like one of about 100 ms.

The client reads these settings once when it starts. Unlike `JFS_WRITE_COMPACTION_INTERVAL` and `JFS_READ_FLUSH_RANGE`, an invalid value is not replaced by a default: the client refuses to start with an error naming the variable, so a typo cannot run a different configuration unnoticed. `juicefs mount` checks before it goes to the background, so the error reaches the terminal, and the gateway, WebDAV, and the Java SDK fail to create the file system. Values must be written exactly: `JFS_COMMIT_MODE` is `chunk` or `epoch` in lower case, and the others are decimal integers in their ranges, without spaces. The epoch settings are checked in chunk mode too, which ignores them.

- Durability is unchanged for `fsync`: it returns once every earlier write of the file is committed. Without one, a write can be lost for about one epoch (at most `JFS_EPOCH_MAX_AGE_MS`, plus up to 100 ms), plus the slowest upload of its epoch, the commits of older epochs, and one transaction.
- The guarantee is per file; there is no order between files. With the [client write cache](#client-write-cache) it covers the metadata only, as the blocks an epoch refers to may be staged locally and not yet uploaded.
- A commit that fails with an error that can follow a commit that landed (a lost connection, a timeout) is sent again, up to 10 times over about 26 seconds. Each resend first looks for the epoch in the metadata, in the same transaction as its write: it writes nothing if the epoch is there, and gives up if none of it is there but the file's inode changed since the first send (the epoch may have landed and been compacted since).
- If an upload or a commit of an epoch fails for good, no later epoch of the file commits: the objects they uploaded are removed, and writes, reads (on any handle, read-only ones included), `fsync`, and truncate on the file return an error until the last handle opened for writing closes and the client is done with the file's pending epochs. When the outcome of a failed commit stays unknown to the client, its objects are kept.
- Redis, the SQL engines, and the TKV engines commit an epoch with one `WriteMulti` transaction. An engine without it commits the slices one by one, in order, which does not keep an epoch whole; the client logs a warning once.
- Epoch mode adds `juicefs_writer_epoch*`, `juicefs_writer_slices_*`, `juicefs_writer_read_epoch_wait_seconds`, and `juicefs_writer_slice_cap_wait_seconds` metrics; chunk mode does not register them.

Epoch mode uploads the same blocks and commits the same slices as chunk mode, in fewer metadata transactions. What it costs is latency: without `fsync`, a write can stay uncommitted until its epoch closes, and a read that must see a pending write waits for every upload of that write's epoch and of the older ones, not only for its own.

### Client write cache {#client-write-cache}

Client write cache is also referred to as "Writeback mode" throughout the docs.

For scenarios that does not deem consistency and data security as top priorities, enabling client write cache is also an option to further improve performance. When client write cache is enabled, flush operations return immediately after writing data to the local cache directory. Then, local data is uploaded asynchronously to the object storage. In other words, the local cache directory is a cache layer for the object storage.

Learn more in [Client Write Cache](../guide/cache.md#client-write-cache).

## Data reading process {#workflow-of-read}

VFS reads flush the file's pending writes before reading by default. Set `JFS_READ_FLUSH_RANGE=true` in the client process environment to opt into flushing only pending writes that overlap the read, together with the earlier slices they depend on. Unrelated pending writes can remain buffered, reducing interference between reads and writes to different ranges of a large file. `fsync`, `flush`, and `release` retain their full-flush behavior; this setting does not change persistent formats or the separate `pkg/fs` read path.

The setting is read once when the VFS client is created; changing a running mount requires restarting it with the new environment. Unset, empty, or false values preserve full-file flushing. Boolean values follow Go's `strconv.ParseBool`: `1`, `t`, `T`, `true`, `TRUE`, and `True` enable it; `0`, `f`, `F`, `false`, `FALSE`, and `False` disable it. Invalid values produce a warning and leave the optimization disabled.

With `JFS_COMMIT_MODE=epoch` (see [commit mode](#commit-mode)), a read that must see pending writes waits for the epoch holding them, which commits whole, so writes made before it to other ranges of the file commit with it. With `JFS_READ_FLUSH_RANGE` off, a read flushes the whole file as before: it closes the open epoch and waits for every epoch of the file, and holds back the file's writes meanwhile. With it on, a read closes the open epoch only if it overlaps one of its writes, then waits for the newest epoch holding a write it overlaps, and so for the older ones; a read that overlaps no pending write waits for nothing. In epoch mode, a read whose flush fails, or that follows a failed epoch of the file, returns an error rather than data that lacks writes made before it; in chunk mode the flush error is ignored, as before.

JuiceFS supports sequential reads and random reads (including mmap-based random reads). During read requests, the object corresponding to the block is completely read through the `GetObject` API of the object storage, or only a certain range of data in the object may be read (e.g., the read range is limited by the `Range` parameter of [S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)). Meanwhile, prefetching is performed (controlled by the [`--prefetch`](../reference/command_reference.mdx#mount) option) to download the complete data block into the local cache directory, as shown in the `blockcache` write speed in the second stage of the above metrics figure. This is very good for sequential reads as all cached data is utilized, maximizing the object storage access efficiency. The dataflow is illustrated in the figure below:

![internals-read](../images/internals-read.png)

Although prefetching works well for sequential reads, it might not be so effective for random reads on large files. It can cause read amplification and frequent cache eviction. Consider disabling prefetching using `--prefetch=0`. It is always hard to design cache strategy for random read scenarios. Two possible solutions are increasing the cache size to store all data locally or completely disabling the cache (`--cache-size=0`) and relying on a high-performance object storage service.

Reading small files (smaller than the block size) is much easier because the entire file can be read in a single request. Since small files are cached locally during the write process, future reads are fast.
