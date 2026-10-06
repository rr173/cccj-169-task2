# 跨设备工艺事件回放与风险处置服务

接收打印机产生的温度 / 压力 / 耗材计量事件，按设备时钟漂移校正后重建可复现的生产时间线，
从时间线识别连续超限、缺失区间与计量跳变并生成待复核项，支持接受 / 驳回 / 退回重算的
复核工作流，网页展示原始事件、修正版本与处理历史，并导出带签名的审计报告。

纯 Python 标准库实现（`http.server` + `sqlite3` + `hmac`），无第三方依赖。

## 运行

```bash
python3 run.py --db replay.db --port 8000     # 打开 http://127.0.0.1:8000
python3 scripts/seed_demo.py replay.db        # 可选：灌入演示数据
python3 -m unittest discover -s tests -v      # 17 个端到端测试
```

## 架构

```
replay_service/
  db.py       SQLite（WAL、外键、BEGIN IMMEDIATE 事务，异常即回滚）
  drift.py    时钟漂移估计：对 (device_ts, server_ts) 做精确有理数最小二乘拟合
              corrected_ts = round(device_ts · a + b)，a/b 以 Fraction 存储，结果确定
  detect.py   检测器：连续超限（次数/时长双阈值）、缺失区间、计量跳变（负增量/超阈值）
  core.py     业务层：分块上传、导入、校正版本、时间线、复核、重算、签名报告
  server.py   HTTP 路由 + 静态页
  static/     单页网页（设备/导入、时间线、复核队列、历史、报告）
```

## 关键设计

**断点续传与幂等导入**
- 上传会话以 `(device_id, batch_id, package_sha256)` 唯一：中断后用相同参数重建会话即返回
  已收到的分块清单，客户端跳过已传块继续上传。
- 分块按 `(session, index)` 幂等：同内容重传返回 `idempotent`，不同内容 409。
- 封存校验整体 SHA-256；同 `batch_id` 不同内容 409；重复导入同一包为幂等空操作
  （事件表 `UNIQUE(device_id,batch_id,kind,seq)` + `INSERT OR IGNORE`）。

**可复现时间线**
- 校正系数为精确有理数，相同输入永远得到相同 `timeline_hash`（SHA-256 指纹）。
- 相同 `(version_id, input_hash, algo_version, generation)` 的重建是幂等的；
  退回重算会递增 `generation` 强制产生新世代时间线。

**校正版本不可静默覆盖**
- 版本只追加不修改；被时间线引用后（`referenced=1`）更新/删除返回 409。
- 仅允许显式的 `supersede` 状态迁移（非破坏性、可审计），相同系数自动去重。

**复核工作流**
- 状态机 `open → accepted/rejected/returned`，携带 `version` 的条件 UPDATE 实现乐观锁：
  并发复核同一项只有一个成功，其余 409；另有部分唯一索引兜底保证终止决定唯一。
- 退回 → 重算：新校正版本 + 新世代时间线 + 新待复核项在同一事务内完成；
  任何失败整体回滚，复核项重新置为 `open` 并记录 `recompute_failed`。

**签名审计报告**
- 报告载荷规范化 JSON（排序键、紧凑分隔符）后用设备私钥做 HMAC-SHA256。
- 加载时重算签名并常量时间比较：篡改载荷、伪造 device_id、用他设备密钥重签、
  未知设备，一律 403 拒绝加载。

## 主要 API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/devices` | 注册设备（返回一次性签名密钥） |
| POST | `/api/uploads` | 创建/恢复上传会话（断点续传） |
| PUT | `/api/uploads/{sid}/chunks/{i}` | 上传分块（`X-Chunk-SHA256` 头，幂等） |
| POST | `/api/uploads/{sid}/seal` | 校验并封存批次（幂等） |
| POST | `/api/batches/import` | 导入批次事件（重复包幂等） |
| POST | `/api/devices/{id}/corrections` | 估计漂移，生成校正版本 |
| PUT/DELETE | `/api/corrections/{vid}` | 未引用才可改/删，否则 409 |
| POST | `/api/timelines/build` | 重建时间线并生成待复核项 |
| GET | `/api/timelines/{id}/points` | 原始事件 + 校正时间 |
| POST | `/api/findings/{id}/reviews` | 复核（accept/reject/return + version） |
| POST | `/api/findings/{id}/recompute` | 退回后重算（失败回滚） |
| GET | `/api/findings/{id}/history` | 处理历史 |
| POST | `/api/reports/export` / `/api/reports/load` | 导出签名报告 / 校验加载 |

## 批次包格式

```json
{
  "device_id": "printer-1",
  "batch_id": "batch-001",
  "events": [
    {"seq": 1, "kind": "temp", "device_ts": 5000, "server_ts": 100, "value": 50.2}
  ]
}
```

`kind ∈ {temp, pressure, material}`；`server_ts` 为网关接收时刻（缺省取导入时刻），
是漂移校正的参考时钟。检测阈值（温度/压力上限、超限持续时间、缺失阈值、计量跳变阈值）
可在注册设备时通过 `params` 覆盖。
