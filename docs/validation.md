# 工程验收

## 环境

接收的原目录位于 Windows 桌面，包含 Go 1.23 模块，Redis RESP2 客户端仅使用标准库。该目录没有 Git 元数据。升级前在 Windows 运行 `go test ./...` 返回成功，Poppler 当时未进入测试 PATH。升级后的集成验证使用 WSL Ubuntu 24.04、Linux Go 1.24.13、Redis 7.0.15 和 Poppler 24.02.0。对本机回环拒连的初次尝试单独排查后，正式验证在隔离的 Linux 回环网络中运行。

## 执行结果

| 检查 | 结果 |
| --- | --- |
| `go build ./...` | 通过，编译 API、Worker、离线核对命令 |
| `go test ./...` | 通过 |
| `TASKFLOW_INTEGRATION=1 go test -count=1 ./...` | 通过，集成测试启动真实 Redis 和 Poppler |
| `TASKFLOW_INTEGRATION=1 go test -race -count=1 ./...` | 通过，包含真实依赖集成测试 |
| `go vet ./...` | 通过 |
| `test -z "$(gofmt -l cmd internal)"` | 通过 |
| `python3 scripts/failover.py` | 通过，提前转移 Pending 后 SIGKILL 原 Worker，第 2 次尝试成功，下载 228 字节且匹配示例 |
| Compose 2.40.3 的 `config --quiet` | 通过 |
| PowerShell 解析，`bash -n scripts/compose-smoke.sh`，`python3 -m py_compile scripts/*.py` | 通过 |
| 1、2、4、8 Worker 压测及上传前后对照 | 完成，记录见[性能测试](benchmark.md) |
| Docker 容器构建和运行、远程 GitHub Actions | 待验证。本机无可用 Docker Engine，已提供 Docker smoke 脚本和 CI 配置 |

## 故障与边界证据

- `TestTwoWorkersDuplicateAndPDF` 用同步屏障观察两个任务同时进入转换。注入相同任务消息后，总转换次数仍为两次，最终 PEL 归零，两条下载路由返回预期的文本。
- `TestEarlyRecoveryFencingAndCrashBudget` 注入有效租约内的消息回收，模拟租约过期和多次失联，验证预算耗尽后进入最终失败。
- `TestStaleCannotReplaceDownloadedResult` 在 Redis 上提交第二次执行的结果，再尝试旧令牌的成功和失败提交。下载正文保留赢得提交权的候选。
- `TestRedisOutageDuringExecution` 在执行中关闭 Redis，观察就绪检查失败，重启同目录的 AOF 实例后完成重投递。
- `TestInvalidPDFRetriesToLimit` 与 `TestConverterTimeout` 分别验证损坏 PDF 的终态错误与进程超时后的临时文件清理。
- `TestReconcileRetainsReferencedFiles` 在独立 Redis 和临时目录中验证默认仅列出孤儿，显式应用仅移除未引用候选，保留输入和成功结果。

## 交付判断

核心链路达到可重复演示和可审查的单机工程版本。README 提供架构图和 API 示例，仓库包含故障脚本、可复现压测、28 道面试问答和两套简历描述。用于更高可靠性目标时，优先补齐 Docker Desktop 实机部署证据、长时间负载测试、任务历史归档、对象存储与 Redis 故障域设计。详见[可靠性说明](reliability.md)。
