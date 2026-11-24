# 可验证的简历素材

按本人实际参与、验证和理解程度选用。性能数字对应本仓库的固定样本和本机 WSL 实验。

## A · Go 后端开发实习

**项目名称**　TaskFlow 异步 PDF 任务处理平台
**技术栈**　Go、Redis Streams、Lua、Poppler、Docker Compose

| 可使用的经历 | 实现与证据 | 可能追问 |
| --- | --- | --- |
| 实现 Go HTTP 文件上传、任务查询及结果下载，使用流式 multipart 校验与限长复制落盘。384 页固定 PDF 上传对照中，12 并发下 API 采样峰值 RSS 中位数由 81.63 降至 12.52 MiB。 | `internal/api/server.go`、`TestUploadLimitAndSafeName`、[上传对照](benchmark.md) | 为什么用 RSS 而非 Go heap？测试是否包含转换？ |
| 使用 Redis Streams 消费者组解耦上传与转换，支持多 Worker、Pending 恢复和有界重试。真实 Redis 上验证双 Worker 重叠执行及重复消息处理。 | `internal/worker/worker.go`、`internal/tasks/repository.go`、`TestTwoWorkersDuplicateAndPDF` | 重复消息在 running 阶段怎么处理？ |
| 用 Context 限制 Poppler 子进程执行时间，提供任务结构化日志、积压与终态延迟指标。 | `internal/convert/pdf.go`、`internal/tasks/metrics.go`、`TestConverterTimeout` | API、Worker 和 Poppler 如何分离定位瓶颈？ |

## B · 基础架构 / 分布式系统实习

**项目名称**　TaskFlow 高可靠异步任务调度平台
**技术栈**　Go、Redis Streams、Lua、Redis AOF、Poppler

| 可使用的经历 | 实现与证据 | 可能追问 |
| --- | --- | --- |
| 设计 Redis Lua 状态机，用 Redis 时钟、尝试预算和执行令牌约束任务领取及终态写入，处理队列的至少一次投递。 | `internal/tasks/transitions.go`、`TestEarlyRecoveryFencingAndCrashBudget` | Lua 遇到运行错误会回滚吗？预算何时扣减？ |
| 实现 Pending 接管与不可变尝试结果提交协议。双 Worker 故障脚本验证对原 Worker 强制终止后任务在第 2 次尝试成功。集成测试验证过期提交不能替换下载内容。 | `internal/worker/worker.go`、`internal/convert/pdf.go`、`scripts/failover.py`、`TestStaleCannotReplaceDownloadedResult` | 重命名后 Redis 回执丢失如何处理？ |
| 构建 1、2、4、8 Worker 可复现压测，区分 API 受理、完整链路和 Poppler 吞吐。在 160 份固定 32 页 PDF、WSL 单机环境下，8 Worker 完成 322.91–373.47 任务/秒，两轮错误率均为 0%。 | `scripts/benchmark.py`、[压测原始记录](benchmark-results.json) | P95 对应的起止点是什么？如何控制文件系统和 CPU 的变量？ |
