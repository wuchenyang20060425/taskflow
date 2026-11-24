# 系统架构

## 进程与数据

| 模块 | 责任 | 实现 |
| --- | --- | --- |
| API | 限制上传、读写元数据、下载已引用文件 | `internal/api` |
| Redis 仓库 | 任务 Hash、Streams 消费组、租约、指标 | `internal/tasks` |
| Worker | 恢复、读取、领取、运行执行器、提交 | `internal/worker` |
| Poppler 适配器 | 有时限的参数化进程调用、识别空白输出、写入候选文件 | `internal/convert` |
| 数据目录 | `uploads/<id>.pdf` 与 `results/<id>-<token>.txt` | 由 API 和所有 Worker 共享 |

Redis 键名沿用 `taskflow:task:<id>`、`taskflow:stream` 和 `taskflow:workers`。`taskflow:metrics` 是合并多个 Worker 的计数和直方图。队列积压通过 `XINFO GROUPS` 取得，其中 `lag` 代表未分发消息数，`pending` 代表已分发但未确认的消息数，二者都以消息为单位。

## 并发模型

每个 HTTP 请求由 Go HTTP Server 交给 goroutine 处理。上传使用 `MultipartReader` 和限长复制流式写入，避免按文件大小分配完整的中间表单缓冲。客户端并发越大，打开文件、连接与磁盘带宽需求越高。

Worker 每进程有一个调度循环。每次优先按游标扫描一条可恢复消息，然后检查新消息。在没有恢复工作时，阻塞读取新消息最多约一秒。每个进程同一时刻运行一个转换子进程。消费者名附加随机实例后缀，避免同名进程共享消费者身份。退出信号取消阻塞 Redis 请求和正在运行的转换，未完成的消息由其他 Worker 接管。API 收到退出信号后等待正在进行的 HTTP 请求，最多八秒。

## 配置

| 环境变量 | 默认值 | 用途 |
| --- | --- | --- |
| `REDIS_ADDR` | `127.0.0.1:6379` | Redis 地址 |
| `REDIS_PASSWORD` | 空 | RESP 客户端可用的单密码认证 |
| `DATA_DIR` | `./data` | 输入和输出根目录 |
| `HTTP_ADDR` | `:8080` | HTTP 监听地址 |
| `PDFTOTEXT_BIN` | `pdftotext` | 受部署者控制的 Poppler 可执行文件 |
| `WORKER_TIMEOUT` | `25s` | 领取后读取和转换的时间上限 |
| `WORKER_LEASE` | `40s` | 租约长度，至少比 Worker 超时多一秒 |
| `WORKER_MIN_IDLE` | `45s` | Pending 消息可被回收的最小空闲时间 |
| `WORKER_MAX_ATTEMPTS` | `3` | 任务最大领取次数 |
| `LOG_LEVEL` | `info` | 支持 `info`、`debug`、`warn` |
| `PPROF_ADDR` | 空 | 启用独立 pprof 监听，地址必须是回环 IP |

进程输出 JSON 日志。API 在成功受理和出错时记录 `request_id` 与 `task_id`，Worker 使用 `task_id`、`stream_id`、消费者和尝试次数，完成日志提供执行耗时。`/metrics` 聚合终态任务计数和从创建到提交的延迟直方图。指标标签使用固定状态和区间。

## 从原始版本升级

停止旧 API 和 Worker 后，再运行新版本。旧 Worker 的提交 Lua 缺少严格租约到期检查，滚动混跑会绕过新版本的提交约束。Redis 键和已有状态保留，旧记录的毫秒时间字段可能为空或为零，旧 `/download` 路径继续可用。

新版容器使用 UID/GID `10001:10001`。如果已有旧版 Docker 共享数据卷，备份后停服，确认卷内容，再用具备权限的运维账号给卷内 `uploads` 和 `results` 目录及其文件迁移所有权。旧版本以 root 写出的 `0600` PDF 需要这个步骤。新建卷从镜像继承目录权限。
