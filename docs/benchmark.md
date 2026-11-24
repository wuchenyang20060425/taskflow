# 性能测试

## 复现

在 Linux 或 WSL 使用 Go、Python 3、Redis 和 Poppler。每个场景启动独立的回环 Redis 实例和 API，创建独立的数据目录，结束后清理。每个任务用同一份由确定性脚本生成的 PDF。

```bash
python3 scripts/benchmark.py --tasks 160 --pages 32 --clients 12 --repeats 2
python3 scripts/benchmark.py --tasks 160 --pages 32 --clients 12 --profile-dir benchmark-output/profiles
go tool pprof -top -nodecount=12 benchmark-output/profiles/worker.pprof
```

若 WSL 的 Windows 回环转发影响临时端口，可用 `unshare --user --map-root-user --net bash` 启动隔离网络，再执行 `ip link set lo up` 后运行上述命令。本次在默认 WSL 网络中观察到与任务无关的随机回环拒连，以下表格只统计隔离网络中的复测。

## 统计口径

- API-only 启动 Redis 和 API，记录 HTTP 上传及 Redis 入队的速率。无 Worker，上传后的任务会留在该实验的临时实例中。
- 端到端在上传时运行 Worker。提交吞吐从批量发起到收到最后一个上传响应，完成吞吐从批量发起到负载客户端看到全部终态。
- 排队时间为 Redis 记录的 `started_ms - created_ms`，仅计算首次领取。端到端分位数为 `finished_ms - created_ms`，用 nearest-rank 算法，包含排队、转换、重试和提交。文件上传时间在这个分位数之外。错误率包含上传失败、转换失败和到时仍未观测到的终态。
- Poppler-only 并发调用 `pdftotext`，统计独立转换吞吐、子进程 CPU 和单次峰值 RSS，以及输入、输出文件字节。操作系统缓存参与实验，字节数表示逻辑文件体积。
- 端到端用 `/proc` 读取进程 CPU，并每 25 ms 采样 RSS。100% CPU 表示一个逻辑核心。子进程 CPU 来自 Worker 回收的子进程累计值。多进程的 RSS 相加包含共享页，短尖峰可能漏采。实验未测量物理磁盘 IOPS、Redis 峰值内存或长期运行表现。

## 当前实测

WSL Ubuntu 24.04、Intel Core i7-14650HX，WSL 可见 24 个逻辑 CPU，Redis 7.0.15，Go 1.24.13，Poppler 24.02.0。PDF 大小 90,426 字节，32 页。每个场景提交 160 个任务，12 条并发上传，独立重复两轮。所有端到端任务均成功。仅测 API 时受理 2296.80–2700.91 个任务/秒。

| Worker | 提交/秒 | 完成/秒 | 平均排队 ms | P50 ms | P95 ms | P99 ms | 错误率 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2513.00–2577.88 | 60.70–69.59 | 1066.76–1256.78 | 1067–1276 | 2059–2412 | 2181–2519 | 0% |
| 2 | 2387.86–2439.48 | 140.50–140.85 | 519.04–521.35 | 528–531 | 997–1000 | 1047–1051 | 0% |
| 4 | 1654.22–2040.59 | 205.76–241.12 | 262.89–328.17 | 277–353 | 513–630 | 544–653 | 0% |
| 8 | 1401.40–1470.41 | 322.91–373.47 | 141.47–154.29 | 163–177 | 286–324 | 298–332 | 0% |

| Worker | API CPU | Worker CPU | Worker 子进程 CPU | API 峰值 RSS MiB | Worker 聚合峰值 RSS MiB | 单测 Poppler 转换/秒 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 34.79–40.22% | 9.13–11.00% | 80.43–82.20% | 13.36–14.21 | 12.33–12.38 | 73.68–75.69 |
| 2 | 40.40–40.49% | 17.56–17.61% | 161.98–162.46% | 13.17–13.78 | 22.88–23.62 | 138.49–165.08 |
| 4 | 54.25–57.87% | 37.68–42.44% | 290.64–304.41% | 12.32–13.12 | 39.38–40.50 | 255.61–274.79 |
| 8 | 70.64–81.70% | 80.73–88.70% | 536.84–606.89% | 11.88–12.69 | 66.19–68.25 | 452.67–494.59 |

Poppler-only 中单进程峰值 RSS 的各轮最大值为 23.25 MiB。每轮读取 14,468,160 字节输入，生成 11,617,280 字节文本。子进程 CPU 与独立对照表明，这组 PDF 的计算成本主要在 Poppler。增加 Worker 后完成速率上升，同时 API 受理速率下降，混合负载下存在资源竞争。未测得 8 个 Worker 以上的饱和点。

独立的 5 秒 CPU 采样获得 API 610 ms、单 Worker 120 ms 的 Go 样本。两者的 syscall 样本居首，分别占 45.90% 和 41.67%。按调用栈累计，Redis 客户端约占 API 样本的 26.23% 和 Worker 样本的 41.67%，其中出现 TCP 连接调用。结果提示后续可用连接池单独复测，短采样不足以确认长期收益。详见 [pprof 摘要](benchmark-pprof.txt)。

## 流式上传前后对照

原实现的 `FormFile` 会完整解析 multipart 表单，中等尺寸并发上传会产生明显的临时内存占用。新版读取 PDF 签名后使用限长复制写入文件，继续检查 multipart 的剩余部分。用 384 页固定 PDF（1,083,390 字节）、160 次上传、12 个客户端和零 Worker，对原版与升级版各执行三轮，表格为三轮中位数。

| 版本 | 受理任务/秒 | API 峰值 RSS MiB | API CPU | 受理错误率 |
| --- | ---: | ---: | ---: | ---: |
| 原版 | 939.20 | 81.63 | 488.97% | 0% |
| 流式上传版 | 1945.31 | 12.52 | 327.08% | 0% |

对照比较两个版本的完整上传路径。除了表单读取，日志和状态初始化也有变化，无法仅凭这个实验拆分各项贡献。复现原版对照需要其源文件副本。

```bash
python3 scripts/benchmark.py --api-only --project /path/to/original-taskflow --tasks 160 --pages 384 --clients 12 --repeats 3 --output benchmark-output/before.json
python3 scripts/benchmark.py --api-only --tasks 160 --pages 384 --clients 12 --repeats 3 --output benchmark-output/after.json
```

原始数据见 [伸缩测试](benchmark-results.json)、[上传基线](benchmark-upload-before.json) 和 [升级版上传](benchmark-upload-after.json)。
