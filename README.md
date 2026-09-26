# 野生动物疫病监测与离线同步

这是一个只使用Python标准库和SQLite的模块化项目，默认端口为`8305`。所有业务规则集中在`src/rules.py`，`app.py`只负责组装依赖和启动服务。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、数据结构、领域异常和基础校验。
- `src/rules.py`：状态机、权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务和乐观锁。
- `src/service.py`：用例编排、幂等处理、版本控制和审计写入。
- `src/http_api.py`：HTTP路由、请求解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线。
- `static/index.html`：演示页面，展示批次接收记录与待处理冲突。
- `static/styles.css`：页面外部样式表（`/static/styles.css`）。
- `tests/`：完整流程、规则和失败场景测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8305
```

服务启动时会自动建表。`--host`可修改监听地址，`--db`可指定其他SQLite文件。

## 核心对象

- `observation`：现场观察；`sample`：样本与实验室结果；`cluster`：异常聚集事件。

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `GET /api/audit`：读取审计记录。
- `POST /api/batches`：现场端回网后整批提交离线编辑（也可用 `Batch-Id` 头传批次号）。
- `GET /api/batches`：批次接收记录（首页展示）。
- `GET /api/conflicts?status=pending`：待处理冲突（首页展示）。
- `GET /api/changes?cursor=N`：按游标拉取后续变更，返回 `next_cursor`。

请求身份通过`X-User-Id`和`X-Role`请求头传入。创建和动作的可执行角色由规则引擎控制。

## 离线批次接收链路

`POST /api/batches` 请求体：

```json
{
  "batch_id": "terminal-7-20260926-001",
  "items": [
    {"op": "create", "kind": "observation", "data": {"...": "..."}},
    {"op": "patch", "kind": "observation", "id": "<记录ID>", "base_version": 3, "data": {"location": "North Ridge"}},
    {"op": "action", "kind": "observation", "id": "<记录ID>", "action": "submit", "base_version": 4, "data": {"location": "N", "observed_at": "2026-04-01"}}
  ]
}
```

规则：

- **幂等重发**：相同 `batch_id` 且内容一致，直接返回第一次接收时保存的结果（含相同游标），不会重复建记录。
- **批次冲突**：相同 `batch_id` 但条目内容不同（SHA-256 比对），返回 `409 ConflictError`，且不落库该次篡改尝试。
- **过期基础版本**：`patch`/`action` 携带的 `base_version` 已落后时，实体保持原样、版本不增，只在 `conflicts` 表写一条 `pending` 冲突及失败原因；批次状态为 `conflict`，仍返回当前游标。
- **游标**：成功处理的批次返回递增游标（取自审计流自增 ID）；现场端用 `GET /api/changes?cursor=N` 增量同步服务端后续变更。
- **持久化**：批次结果、冲突、游标全部存入 SQLite，服务重启后仍可查询和幂等重放。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

离线同步使用批次和幂等键演示，不包含真实野外通信协议、地图底图或完整空间索引。
