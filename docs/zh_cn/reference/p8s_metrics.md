---
title: JuiceFS 监控指标
sidebar_position: 4
---

如果你尚未搭建监控系统、收集 JuiceFS 客户端指标，阅读[「监控」](../administration/monitoring.md)文档了解如何收集这些指标以及可视化。

## 全局标签 {#global-labels}

| 名称       | 描述        |
| ----       | ----------- |
| `vol_name` | Volume 名称 |
| `instance` | 客户端主机名，格式为 `<host>:<port>`。详见[官方文档](https://prometheus.io/docs/concepts/jobs_instances) |
| `mp`       | 挂载点路径，如果是通过 [Prometheus Pushgateway](https://github.com/prometheus/pushgateway) 上报，例如 [JuiceFS Hadoop Java SDK](../administration/monitoring.md#hadoop)，那么 `mp` 标签的值为 `sdk-<PID>` |

## 文件系统 {#file-system}

### 指标

| 名称                            | 描述            | 单位 |
|-------------------------------|---------------|----|
| `juicefs_used_space`          | 总使用空间         | 字节 |
| `juicefs_used_inodes`         | 总 inodes 数量   |    |

## 操作系统 {#operating-system}

### 指标

| 名称                | 描述        | 单位 |
| ----                | ----------- | ---- |
| `juicefs_uptime`    | 总运行时间  | 秒   |
| `juicefs_cpu_usage` | CPU 使用量  | 秒   |
| `juicefs_memory`    | 内存使用量  | 字节 |

## 元数据引擎 {#metadata-engine}

### 指标

| 名称                                              | 描述           | 单位 |
| ----                                              | -----------    | ---- |
| `juicefs_transaction_durations_histogram_seconds` | 事务的延时分布 | 秒   |
| `juicefs_transaction_restart`                     | 事务重启的次数 |      |

## FUSE {#fuse}

### 指标

| 名称                                           | 描述                 | 单位 |
| ----                                           | -----------          | ---- |
| `juicefs_fuse_read_size_bytes`                 | 读请求的大小分布     | 字节 |
| `juicefs_fuse_written_size_bytes`              | 写请求的大小分布     | 字节 |
| `juicefs_fuse_ops_durations_histogram_seconds` | 所有请求的延时分布   | 秒   |
| `juicefs_fuse_open_handlers`                   | 打开的文件和目录数量 |      |

## SDK {#sdk}

### 指标

| 名称                                          | 描述               | 单位 |
| ----                                          | -----------        | ---- |
| `juicefs_sdk_read_size_bytes`                 | 读请求的大小分布   | 字节 |
| `juicefs_sdk_written_size_bytes`              | 写请求的大小分布   | 字节 |
| `juicefs_sdk_ops_durations_histogram_seconds` | 所有请求的延时分布 | 秒   |

## 缓存 {#cache}

### 指标

| 名称                                      | 描述          | 单位 |
|-----------------------------------------|-------------|----|
| `juicefs_blockcache_blocks`             | 缓存块的总个数     |    |
| `juicefs_blockcache_bytes`              | 缓存块的总大小     | 字节 |
| `juicefs_blockcache_hits`               | 命中缓存块的总次数   |    |
| `juicefs_blockcache_miss`               | 没有命中缓存块的总次数 |    |
| `juicefs_blockcache_writes`             | 写入缓存块的总次数   |    |
| `juicefs_blockcache_drops`              | 丢弃缓存块的总次数   |    |
| `juicefs_blockcache_evicts`             | 淘汰缓存块的总次数   |    |
| `juicefs_blockcache_hit_bytes`          | 命中缓存块的总大小   | 字节 |
| `juicefs_blockcache_miss_bytes`         | 没有命中缓存块的总大小 | 字节 |
| `juicefs_blockcache_write_bytes`        | 写入缓存块的总大小   | 字节 |
| `juicefs_blockcache_read_hist_seconds`  | 读缓存块的延时分布   | 秒  |
| `juicefs_blockcache_write_hist_seconds` | 写缓存块的延时分布   | 秒  |
| `juicefs_staging_blocks`                | 暂存路径中的块数    |    |
| `juicefs_staging_block_bytes`           | 暂存路径中块的总字节数 | 秒  |
| `juicefs_staging_block_delay_seconds`   | 暂存块延迟的总秒数 | 秒  |

## 对象存储 {#object-storage}

### 标签

| 名称     | 描述                                              |
| ----     | -----------                                       |
| `method` | 请求对象存储的方法（例如 GET、PUT、HEAD、DELETE） |

### 指标

| 名称                                                 | 描述                     | 单位 |
| ----                                                 | -----------              | ---- |
| `juicefs_object_request_durations_histogram_seconds` | 请求对象存储的延时分布   | 秒   |
| `juicefs_object_request_errors`                      | 请求失败的总次数         |      |
| `juicefs_object_request_data_bytes`                  | 请求对象存储的总数据大小 | 字节 |

## 内部特性 {#internal}

### 指标

| 名称                                     | 描述               | 单位 |
|----------------------------------------| -----------        | ---- |
| `juicefs_compact_size_histogram_bytes` | 合并数据的大小分布 | 字节 |
| `juicefs_compactions_total` | 按触发原因统计的 chunk 合并次数（标签：`trigger`） | |
| `juicefs_compacted_bytes_total` | 按触发原因统计的合并数据总大小（标签：`trigger`） | 字节 |
| `juicefs_used_read_buffer_size_bytes`  | 当前用于读取的缓冲区的大小 |    |
| `juicefs_bgjob_duration_seconds` | 后台清理任务耗时（标签：`job`、`status`） | 秒 |
| `juicefs_bgjob_deletions_total` | 后台任务执行的删除总数（标签：`job`） | |

`juicefs_compactions_total` 和 `juicefs_compacted_bytes_total` 的 `trigger` 标签表示触发合并的原因：

- `write_interval`：写入使 chunk 的 slice 数量达到写入触发的合并间隔（`JFS_WRITE_COMPACTION_INTERVAL`，详见[随机写](../introduction/io_processing.md#random-write)）
- `write_backlog`：写入后 chunk 中的 slice 超过 350 个
- `write_sync`：写入使 chunk 的 slice 数量达到 2500 个或更多，该写入需等待合并完成
- `read`：读取时发现 chunk 中有 5 个或更多 slice
- `compact`：[`juicefs compact`](command_reference.mdx#compact)，由路径所在的挂载点执行
- `gc`：[`juicefs gc --compact`](command_reference.mdx#gc)，在独立的进程中运行，因此其合并不会出现在挂载点的指标中

这两个指标统计的合并与 `juicefs_compact_size_histogram_bytes` 观测到的合并相同（无论合并成功与否），因此除即将开始的合并外，它们按 `trigger` 求和后分别等于该直方图的 `_count` 和 `_sum`。slice 很多的 chunk 可能需要多次合并，每次都计入同一个触发原因。如需了解 `JFS_WRITE_COMPACTION_INTERVAL` 的效果，可以在修改前后按 `trigger` 比较这两个指标的增长速率：间隔越大，`write_interval` 合并越少；如果 `write_backlog` 或 `write_sync` 的计数增长，说明有 chunk 在合并前就达到了更高的阈值。

## 挂载配置 {#mount-configuration}

### 指标

| 名称 | 描述 | 单位 |
|-|-|-|
| `subdir_info` | 当前挂载的子目录配置信息（标签：`subdir`，值恒为 `1`） | |

`subdir` 标签包含挂载的子目录路径，空字符串表示根挂载。

## 数据同步 {#sync}

### 指标

| 名称 | 描述 | 单位 |
|-|-|-|
| `juicefs_sync_scanned` | 从源端扫描的所有对象数量 | |
| `juicefs_sync_handled` | 已经处理过的来自源端的对象数量 | |
| `juicefs_sync_pending` | 等待同步的对象数量 | |
| `juicefs_sync_copied` | 已经同步过的对象数量 | |
| `juicefs_sync_copied_bytes` | 已经同步过的数据总大小 | 字节 |
| `juicefs_sync_skipped` | 同步时被跳过的对象数量 | |
| `juicefs_sync_failed` | 同步时失败的对象数量 | |
| `juicefs_sync_deleted` | 同步时被删除的对象数量 | |
| `juicefs_sync_checked` | 同步时校验过 checksum 的对象数量 | |
| `juicefs_sync_checked_bytes` | 同步时校验过 checksum 的数据总大小 | 字节 |
